"""Generate replay fixture histories by running workflows against a local
WorkflowEnvironment and exporting their event histories.

Run this script after any non-additive workflow code change to regenerate
fixtures, then commit the updated JSON files.

Usage:
    cd python-backend
    python tests/fixtures/generate_replay_fixtures.py

This script starts an ephemeral Temporal server, runs each path to completion,
fetches the event history via the low-level WorkflowService gRPC API, and
writes it as JSON.  The resulting files are committed to the repo and used
by tests/test_replay.py to verify replay safety.

Scenarios generated
-------------------
  happy_path          — validation → risk → payment → PIM → Kafka → FULFILLED
  risk_rejected       — risk gate rejects the order → RISK_REJECTED
  correction_timeout  — validation fails, no correction in 30 days → CORRECTION_TIMEOUT
  payment_expired     — invalid RRN, no resubmission in 7 days → PAYMENT_EXPIRED
  fulfillment_failed  — PIM activity raises non-retryable error → FULFILLMENT_FAILED
"""

from __future__ import annotations

import asyncio
import contextlib
import pathlib
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.workflowservice.v1 import GetWorkflowExecutionHistoryRequest
from temporalio.client import WorkflowFailureError, WorkflowHistory
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import sys
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))

from oms.constants import COMMERCE_QUEUE, OMS_QUEUE
from oms.models import (
    DashboardUpdate,
    EnrichedOrder,
    EnrichmentInput,
    ERR_INVALID_RRN,
    OrderInput,
    OrderItem,
    PaymentInput,
    PaymentValidationInput,
)
from oms.workflows import OrderProcessingWorkflow


_FIXTURES_DIR = pathlib.Path(__file__).parent


# ── Order / payment helpers ───────────────────────────────────────────────────


def _valid_order(order_id: str) -> OrderInput:
    return OrderInput("CUST-1", order_id, [OrderItem("ITEM-1", 2)])


def _payment(rrn: str) -> PaymentInput:
    return PaymentInput(rrn, 5000)


# ── Configurable activity double ──────────────────────────────────────────────


@dataclass
class _Config:
    """Behaviour knobs for each fixture scenario."""

    validate_order_ok: bool = True
    risk_ok: bool = True
    rrn_ok: bool = True
    raise_fulfillment: bool = False


class _FixtureActivities:
    """Configurable activity double for fixture generation.

    Intentionally a *standalone* class (not a subclass of OmsActivities) so
    that every method has an explicit @activity.defn.  Subclass overrides do
    NOT inherit the @activity.defn attribute from the parent's function object,
    which causes a registration error when the Worker tries to resolve the
    activity name.  The standalone pattern mirrors RecordingActivities in the
    functional test suite.
    """

    def __init__(self, cfg: _Config) -> None:
        self._cfg = cfg

    @activity.defn
    async def validate_order_api(self, order: OrderInput) -> bool:
        return self._cfg.validate_order_ok

    @activity.defn
    async def assess_order_risk(self, order: OrderInput) -> bool:
        return self._cfg.risk_ok

    @activity.defn
    async def validate_payment_rrn(self, input: PaymentValidationInput) -> bool:
        # Mirror the non-retryable blank-RRN guard from the real activity so
        # the fixture history contains the same error types a production history
        # would, making replay tests more realistic.
        if input.rrn is None or not input.rrn.strip():
            raise ApplicationError(
                "RRN must not be blank", type=ERR_INVALID_RRN, non_retryable=True
            )
        return self._cfg.rrn_ok

    @activity.defn
    async def enrich_with_pim(self, input: EnrichmentInput) -> EnrichedOrder:
        if self._cfg.raise_fulfillment:
            raise ApplicationError("PIM unavailable", non_retryable=True)
        return EnrichedOrder(
            customer_id=input.order.customer_id,
            order_id=input.order.order_id,
            rrn=input.rrn,
            items=[
                OrderItem(item.item_id, item.quantity, f"SKU-{item.item_id}", "BRAND-X")
                for item in input.order.items
            ],
        )

    @activity.defn
    async def update_customer_dashboard(self, update: DashboardUpdate) -> None:
        pass

    @activity.defn
    async def publish_to_fulfillment_kafka(self, order: EnrichedOrder) -> None:
        pass


# ── Worker context manager ────────────────────────────────────────────────────


@contextlib.asynccontextmanager
async def _workers(client, acts: _FixtureActivities):
    """Spin up OMS_QUEUE + COMMERCE_QUEUE workers and tear them down on exit."""
    activity_methods = [
        acts.validate_order_api,
        acts.assess_order_risk,
        acts.validate_payment_rrn,
        acts.enrich_with_pim,
        acts.update_customer_dashboard,
        acts.publish_to_fulfillment_kafka,
    ]
    oms_worker = Worker(
        client,
        task_queue=OMS_QUEUE,
        workflows=[OrderProcessingWorkflow],
        activities=activity_methods,
    )
    commerce_worker = Worker(
        client, task_queue=COMMERCE_QUEUE, activities=activity_methods
    )
    async with oms_worker, commerce_worker:
        yield


# ── History fetch / write helpers ─────────────────────────────────────────────


async def _fetch_history_json(client, workflow_id: str) -> str:
    """Fetch a completed workflow's full event history as a JSON string."""
    request = GetWorkflowExecutionHistoryRequest(
        namespace="default",
        execution=WorkflowExecution(workflow_id=workflow_id),
    )
    response = await client.workflow_service.get_workflow_execution_history(request)
    history = WorkflowHistory(workflow_id, response.history.events)
    return history.to_json()


async def _write_fixture(client, workflow_id: str, name: str) -> None:
    history_json = await _fetch_history_json(client, workflow_id)
    path = _FIXTURES_DIR / f"{name}.json"
    path.write_text(history_json)
    print(f"  Written → {path}")


# ── Fixture generators ────────────────────────────────────────────────────────


async def _generate_happy_path() -> None:
    """Full happy path: validation → risk → payment → PIM → Kafka → FULFILLED."""
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        async with _workers(env.client, _FixtureActivities(_Config())):
            wf = await env.client.start_workflow(
                OrderProcessingWorkflow.process_order,
                _valid_order("REPLAY-HAPPY"),
                id="REPLAY-HAPPY",
                task_queue=OMS_QUEUE,
            )
            await env.sleep(timedelta(seconds=1))  # reaches AWAITING_PAYMENT
            await wf.execute_update(
                OrderProcessingWorkflow.capture_payment, _payment("RRN-REPLAY")
            )
            await wf.result()
        await _write_fixture(env.client, "REPLAY-HAPPY", "happy_path")
    finally:
        await env.shutdown()


async def _generate_risk_rejected() -> None:
    """Risk gate rejects the order → RISK_REJECTED terminal status."""
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        async with _workers(env.client, _FixtureActivities(_Config(risk_ok=False))):
            wf = await env.client.start_workflow(
                OrderProcessingWorkflow.process_order,
                _valid_order("REPLAY-RISK"),
                id="REPLAY-RISK",
                task_queue=OMS_QUEUE,
            )
            await wf.result()
        await _write_fixture(env.client, "REPLAY-RISK", "risk_rejected")
    finally:
        await env.shutdown()


async def _generate_correction_timeout() -> None:
    """Validation always fails; support team never corrects → CORRECTION_TIMEOUT."""
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        async with _workers(
            env.client, _FixtureActivities(_Config(validate_order_ok=False))
        ):
            wf = await env.client.start_workflow(
                OrderProcessingWorkflow.process_order,
                _valid_order("REPLAY-CORR-TIMEOUT"),
                id="REPLAY-CORR-TIMEOUT",
                task_queue=OMS_QUEUE,
            )
            await env.sleep(timedelta(seconds=1))  # reaches PENDING_CORRECTION
            await env.sleep(timedelta(days=31))     # skip past 30-day correction window
            await wf.result()
        await _write_fixture(env.client, "REPLAY-CORR-TIMEOUT", "correction_timeout")
    finally:
        await env.shutdown()


async def _generate_payment_expired() -> None:
    """Invalid RRN submitted; no resubmission within 7 days → PAYMENT_EXPIRED."""
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        async with _workers(env.client, _FixtureActivities(_Config(rrn_ok=False))):
            wf = await env.client.start_workflow(
                OrderProcessingWorkflow.process_order,
                _valid_order("REPLAY-PAY-EXP"),
                id="REPLAY-PAY-EXP",
                task_queue=OMS_QUEUE,
            )
            await env.sleep(timedelta(seconds=1))   # reaches AWAITING_PAYMENT
            await wf.execute_update(
                OrderProcessingWorkflow.capture_payment, _payment("RRN-INVALID-REPLAY")
            )
            await env.sleep(timedelta(seconds=1))   # reaches PAYMENT_INVALID
            await env.sleep(timedelta(days=8))       # skip past 7-day retry window
            await wf.result()
        await _write_fixture(env.client, "REPLAY-PAY-EXP", "payment_expired")
    finally:
        await env.shutdown()


async def _generate_fulfillment_failed() -> None:
    """PIM activity raises a non-retryable error → FULFILLMENT_FAILED, workflow FAILED."""
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        async with _workers(
            env.client, _FixtureActivities(_Config(raise_fulfillment=True))
        ):
            wf = await env.client.start_workflow(
                OrderProcessingWorkflow.process_order,
                _valid_order("REPLAY-FF"),
                id="REPLAY-FF",
                task_queue=OMS_QUEUE,
            )
            await env.sleep(timedelta(seconds=1))  # reaches AWAITING_PAYMENT
            await wf.execute_update(
                OrderProcessingWorkflow.capture_payment, _payment("RRN-FF-REPLAY")
            )
            try:
                await wf.result()
            except WorkflowFailureError:
                pass  # expected — workflow exits via re-raise after FULFILLMENT_FAILED
        await _write_fixture(env.client, "REPLAY-FF", "fulfillment_failed")
    finally:
        await env.shutdown()


# ── Entry point ───────────────────────────────────────────────────────────────


async def main() -> None:
    _FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    scenarios: list[tuple[str, object]] = [
        ("happy_path",         _generate_happy_path),
        ("risk_rejected",      _generate_risk_rejected),
        ("correction_timeout", _generate_correction_timeout),
        ("payment_expired",    _generate_payment_expired),
        ("fulfillment_failed", _generate_fulfillment_failed),
    ]
    for name, fn in scenarios:
        print(f"[{name}]")
        await fn()
    print("\nAll fixtures generated.")


if __name__ == "__main__":
    asyncio.run(main())
