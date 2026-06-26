from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio
from aiohttp.test_utils import make_mocked_request
from aiohttp.web_response import Response
from temporalio.client import WorkflowUpdateFailedError
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode

from oms.api import make_app
from oms.constants import OMS_QUEUE
from oms.models import ERR_DUPLICATE_PAYMENT, ERR_INVALID_ORDER, OrderInput, OrderItem, PaymentInput
from oms.workflows import OrderProcessingWorkflow


@dataclass
class StartCall:
    workflow: object
    order: OrderInput
    id: str
    task_queue: str
    id_conflict_policy: object | None = None
    id_reuse_policy: object | None = None


@dataclass
class SignalCall:
    order_id: str
    signal: object
    arg: object | None


@dataclass
class UpdateCall:
    order_id: str
    update: object
    arg: object | None


class FakeWorkflowHandle:
    def __init__(self, client: FakeTemporalClient, order_id: str) -> None:
        self.client = client
        self.order_id = order_id

    async def signal(self, signal: object, arg: object | None = None) -> None:
        self.client.signals.append(SignalCall(self.order_id, signal, arg))

    async def execute_update(
        self,
        update: object,
        arg: object | None = None,
        **_kwargs: object,
    ) -> None:
        # Honour any pre-configured rejection for this update+arg combination.
        # Wrap in WorkflowUpdateFailedError to match the real SDK behaviour: the
        # SDK never raises ApplicationError directly from execute_update — it
        # always wraps it so the caller can access it via e.cause.
        rejection = self.client.update_rejections.get((self.order_id, update))
        if rejection is not None:
            raise WorkflowUpdateFailedError(rejection)
        self.client.updates.append(UpdateCall(self.order_id, update, arg))

    async def query(self, query: object) -> str:
        self.client.queries.append((self.order_id, query))
        return self.client.statuses.get(self.order_id, "AWAITING_PAYMENT")


class FakeTemporalClient:
    def __init__(self) -> None:
        self.starts: list[StartCall] = []
        self.signals: list[SignalCall] = []
        self.updates: list[UpdateCall] = []
        self.queries: list[tuple[str, object]] = []
        self.statuses: dict[str, str] = {}
        # Map of (order_id, update_handler) -> ApplicationError to simulate rejections
        self.update_rejections: dict[tuple[str, object], ApplicationError] = {}
        # If set, start_workflow raises this exception (used to simulate REJECT_DUPLICATE)
        self.start_error: Exception | None = None

    async def start_workflow(
        self,
        workflow: object,
        order: OrderInput,
        *,
        id: str,
        task_queue: str,
        id_conflict_policy: object | None = None,
        id_reuse_policy: object | None = None,
        **_kwargs: object,
    ) -> None:
        if self.start_error is not None:
            raise self.start_error
        self.starts.append(
            StartCall(workflow, order, id, task_queue, id_conflict_policy, id_reuse_policy)
        )

    def get_workflow_handle_for(
        self, workflow: object, order_id: str
    ) -> FakeWorkflowHandle:
        return FakeWorkflowHandle(self, order_id)


class JsonBody:
    def __init__(self, body: dict[str, Any] | None = None) -> None:
        self._body = json.dumps(body or {}).encode()
        self._read = False

    def set_read_chunk_size(self, _: int) -> None:
        pass

    async def readany(self) -> bytes:
        if self._read:
            return b""
        self._read = True
        return self._body


@pytest_asyncio.fixture
async def api_client():
    temporal = FakeTemporalClient()
    yield make_app(temporal), temporal


async def request_json(
    app, method: str, path: str, body: dict[str, Any] | None = None
) -> tuple[int, dict[str, Any]]:
    request = make_mocked_request(
        method,
        path,
        headers={"Content-Type": "application/json"},
        payload=JsonBody(body),
        app=app,
    )
    match_info = await app.router.resolve(request)
    response: Response = await match_info.handler(request)
    return response.status, json.loads(response.text)


# ── Order start ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_start_order_uses_order_id_from_body(api_client):
    """The Commerce App webhook includes order.order_id; the API must use it as the
    workflow ID so that duplicate deliveries are idempotent (USE_EXISTING policy)."""
    app, temporal = api_client

    status, body = await request_json(
        app,
        "POST",
        "/api/orders",
        {
            "customer_id": "CUST-1",
            "order": {
                "order_id": "ORD-COMMERCE-1",
                "items": [{"item_id": "ITEM-1", "quantity": 2}],
            },
        },
    )

    assert status == 200
    assert body["status"] == "accepted"
    assert body["itemCount"] == 1
    assert body["valid"] is True
    # order_id from the webhook body must be used as-is
    assert body["orderId"] == "ORD-COMMERCE-1"

    assert len(temporal.starts) == 1
    start = temporal.starts[0]
    assert start.workflow == OrderProcessingWorkflow.process_order
    assert start.id == "ORD-COMMERCE-1"
    assert start.task_queue == OMS_QUEUE
    assert start.id_conflict_policy == WorkflowIDConflictPolicy.USE_EXISTING
    assert start.id_reuse_policy == WorkflowIDReusePolicy.REJECT_DUPLICATE
    assert start.order == OrderInput(
        "CUST-1", "ORD-COMMERCE-1", [OrderItem("ITEM-1", 2)]
    )


@pytest.mark.asyncio
async def test_start_order_requires_order_id(api_client):
    """order.order_id is mandatory (OMS-F003).

    A server-generated UUID fallback was removed because each delivery without
    an order_id gets a fresh UUID, defeating USE_EXISTING and creating duplicate
    workflows for the same logical order.  Callers must supply a stable,
    business-meaningful ID so that duplicate webhook deliveries are idempotent.
    """
    app, temporal = api_client

    status, body = await request_json(
        app,
        "POST",
        "/api/orders",
        {
            "customer_id": "CUST-1",
            "order": {
                "items": [{"item_id": "ITEM-1", "quantity": 2}],
                # order_id intentionally omitted
            },
        },
    )

    assert status == 400
    assert "order_id" in body["error"].lower()
    # No workflow should have been started
    assert len(temporal.starts) == 0


# ── Payment capture (Update) ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_payment_webhook_sends_update(api_client):
    app, temporal = api_client

    status, body = await request_json(
        app,
        "POST",
        "/api/payments",
        {
            "customer_id": "CUST-1",
            "rrn": "RRN-1",
            "amount_cents": 5000,
            "metadata": {"order_id": "ORD-1"},
        },
    )

    assert status == 200
    assert body == {
        "status": "payment_captured",
        "rrn": "RRN-1",
        "orderId": "ORD-1",
        "amountCents": 5000,
    }
    assert len(temporal.updates) == 1
    upd = temporal.updates[0]
    assert upd.order_id == "ORD-1"
    assert upd.update == OrderProcessingWorkflow.capture_payment
    assert upd.arg == PaymentInput("RRN-1", 5000)


@pytest.mark.asyncio
async def test_duplicate_payment_returns_409(api_client):
    app, temporal = api_client
    # Simulate the workflow validator rejecting a duplicate capture
    temporal.update_rejections[("ORD-1", OrderProcessingWorkflow.capture_payment)] = (
        ApplicationError("Payment already captured for this order", type=ERR_DUPLICATE_PAYMENT)
    )

    status, body = await request_json(
        app,
        "POST",
        "/api/payments",
        {
            "rrn": "RRN-2",
            "amount_cents": 5000,
            "metadata": {"order_id": "ORD-1"},
        },
    )

    assert status == 409
    assert "already captured" in body["error"]


# ── Order correction (Update) ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_correction_webhook_sends_update(api_client):
    app, temporal = api_client

    status, body = await request_json(
        app,
        "POST",
        "/api/orders/correct?orderId=ORD-1",
        {
            "customer_id": "CUST-1",
            "order": {
                "items": [{"item_id": "ITEM-2", "quantity": 3}],
            },
        },
    )

    assert status == 200
    assert body == {"status": "corrected", "orderId": "ORD-1", "itemCount": 1}
    assert len(temporal.updates) == 1
    upd = temporal.updates[0]
    assert upd.order_id == "ORD-1"
    assert upd.update == OrderProcessingWorkflow.handle_correction
    assert upd.arg == OrderInput("CUST-1", "ORD-1", [OrderItem("ITEM-2", 3)])


@pytest.mark.asyncio
async def test_correction_with_empty_items_returns_400(api_client):
    app, temporal = api_client
    # Simulate the workflow validator rejecting empty items
    temporal.update_rejections[("ORD-1", OrderProcessingWorkflow.handle_correction)] = (
        ApplicationError("Corrected order must contain at least one item", type=ERR_INVALID_ORDER)
    )

    status, body = await request_json(
        app,
        "POST",
        "/api/orders/correct?orderId=ORD-1",
        {
            "customer_id": "CUST-1",
            "order": {"items": []},
        },
    )

    assert status == 400
    assert "at least one item" in body["error"]


# ── Status query ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_status_query_returns_workflow_status(api_client):
    app, temporal = api_client
    temporal.statuses["ORD-1"] = "FULFILLED"

    status, body = await request_json(app, "GET", "/api/orders/status?orderId=ORD-1")

    assert status == 200
    assert body == {"orderId": "ORD-1", "status": "FULFILLED"}
    assert temporal.queries == [("ORD-1", OrderProcessingWorkflow.get_order_status)]


# ── OMS-017: REJECT_DUPLICATE collision → 409, not 400 ──────────────────────

@pytest.mark.asyncio
async def test_start_order_returns_409_when_completed_order_id_is_reused(api_client):
    """When WorkflowIDReusePolicy.REJECT_DUPLICATE fires (Commerce App resubmits
    the order_id of a closed execution), the API must return 409 Conflict so
    callers can distinguish 'already processed' from a malformed request (400)."""
    app, temporal = api_client
    temporal.start_error = RPCError(
        "workflow already started", RPCStatusCode.ALREADY_EXISTS, b""
    )

    status, body = await request_json(
        app,
        "POST",
        "/api/orders",
        {
            "customer_id": "CUST-1",
            "order": {
                "order_id": "ORD-COMPLETED",
                "items": [{"item_id": "ITEM-1", "quantity": 2}],
            },
        },
    )

    assert status == 409
    assert "already processed" in body["error"]
    # start_workflow was called but raised before any StartCall was appended
    assert len(temporal.starts) == 0
