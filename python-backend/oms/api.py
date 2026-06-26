"""Thin HTTP layer acting as the webhook receiver for external systems.

Forwards events into running Temporal workflows via start / update / signal / query.

Endpoints:
  POST /api/orders          — Commerce App webhook    -> starts OrderProcessingWorkflow
  POST /api/orders/correct  — Support correction       -> handle_correction update
  POST /api/orders/cancel   — Cancellation request     -> cancel_order signal
  POST /api/payments        — Payment Processor hook   -> capture_payment update
  GET  /api/orders/status   — Live status query        -> get_order_status @query

Note on Updates vs Signals:
  /api/orders/correct and /api/payments use execute_update so that callers get
  synchronous accept/reject feedback (e.g. "payment already captured") instead of
  a silent fire-and-forget. HTTP 409 is returned when the workflow rejects the
  update via its validator.
"""

import os
from datetime import timedelta

from aiohttp import web
from temporalio.client import Client, WorkflowUpdateFailedError
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode

from oms.constants import OMS_QUEUE
from oms.models import OrderInput, OrderItem, PaymentInput
from oms.workflows import OrderProcessingWorkflow

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _json(status: int, body: dict) -> web.Response:
    return web.json_response(body, status=status, headers=CORS_HEADERS)


def _require_param(request: web.Request, name: str) -> str:
    value = request.query.get(name)
    if value is None:
        raise ValueError(f"Missing required query parameter: {name}")
    return value


def _items_from_payload(order: dict) -> list[OrderItem]:
    return [
        OrderItem(item_id=i["item_id"], quantity=i["quantity"])
        for i in order.get("items", [])
    ]


async def options_handler(_: web.Request) -> web.Response:
    return web.Response(status=204, headers=CORS_HEADERS)


def make_app(client: Client) -> web.Application:
    def stub(order_id: str):
        return client.get_workflow_handle_for(
            OrderProcessingWorkflow.process_order, order_id
        )

    # Commerce App webhook: uses order.order_id from the payload as the workflow ID
    # (per PRD spec: { "order": { "order_id": <string>, ... } }).
    # order_id is REQUIRED — callers must supply a stable, business-meaningful ID
    # so that duplicate webhook deliveries are idempotent (USE_EXISTING policy).
    # A server-generated UUID fallback was removed (OMS-F003): each delivery
    # without an order_id would get a fresh UUID, defeating USE_EXISTING and
    # creating duplicate workflows for the same logical order.
    async def start_order(request: web.Request) -> web.Response:
        order_id = None
        try:
            req = await request.json()
            order = req.get("order", {})
            order_id = order.get("order_id")
            if not order_id:
                return _json(400, {"error": "Missing required field: order.order_id"})
            items = _items_from_payload(order)
            order_input = OrderInput(
                customer_id=req["customer_id"], order_id=order_id, items=items
            )
            await client.start_workflow(
                OrderProcessingWorkflow.process_order,
                order_input,
                id=order_id,
                task_queue=OMS_QUEUE,
                # USE_EXISTING: duplicate in-flight webhook deliveries are idempotent —
                # the second call joins the already-running execution.
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                # OMS-011: REJECT_DUPLICATE prevents re-processing a closed order if the
                # Commerce App ever resubmits a completed order_id.  Order IDs are
                # assigned exactly once, so re-starting a finished workflow is always wrong.
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                # Hard safety-net ceiling: correction loop (30 days) + payment window (30 days)
                # + payment retry (7 days) = 67 days max legitimate runtime.  90 days gives
                # headroom while preventing a runaway correction-loop from running forever.
                execution_timeout=timedelta(days=90),
            )
            return _json(
                200,
                {
                    "status": "accepted",
                    "orderId": order_id,
                    "itemCount": len(items),
                    "valid": bool(items),
                },
            )
        except RPCError as e:
            # OMS-017: REJECT_DUPLICATE fires when the Commerce App resubmits a
            # completed order_id.  Map to 409 Conflict so callers can distinguish
            # "order already processed" from a malformed request (400).
            if e.status == RPCStatusCode.ALREADY_EXISTS:
                return _json(409, {"error": f"Order {order_id!r} was already processed — reuse rejected"})
            return _json(400, {"error": str(e) or "Bad request"})
        except Exception as e:  # noqa: BLE001 — surface any parse/start error as 400
            return _json(400, {"error": str(e) or "Bad request"})

    # Support team correction: execute_update so the caller gets synchronous
    # accept/reject feedback (e.g. empty items → 400 before writing to history).
    async def correct_order(request: web.Request) -> web.Response:
        order_id = None
        try:
            order_id = _require_param(request, "orderId")
            req = await request.json()
            items = _items_from_payload(req["order"])
            corrected = OrderInput(
                customer_id=req["customer_id"], order_id=order_id, items=items
            )
            await stub(order_id).execute_update(
                OrderProcessingWorkflow.handle_correction,
                corrected,
            )
            return _json(
                200,
                {"status": "corrected", "orderId": order_id, "itemCount": len(items)},
            )
        except WorkflowUpdateFailedError as e:
            # Validator rejected the correction; e.cause carries the ApplicationError
            # message (e.g. "wrong phase" or "empty items").  str(e) alone would return
            # the uninformative "Workflow update failed" — extract e.cause instead.
            return _json(400, {"error": str(e.cause) or "Update rejected"})
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND:
                return _json(
                    404,
                    {"error": f"Order {order_id} not found or already complete"},
                )
            return _json(400, {"error": str(e) or "Bad request"})
        except Exception as e:  # noqa: BLE001
            return _json(400, {"error": str(e) or "Bad request"})

    # Cancel order: remains a Signal — fire-and-forget, no confirmation needed
    async def cancel_order(request: web.Request) -> web.Response:
        order_id = None
        try:
            order_id = _require_param(request, "orderId")
            await stub(order_id).signal(OrderProcessingWorkflow.cancel_order)
            return _json(
                200, {"status": "cancellation_requested", "orderId": order_id}
            )
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND:
                return _json(
                    404,
                    {"error": f"Order {order_id} not found or already complete"},
                )
            return _json(400, {"error": str(e) or "Bad request"})
        except Exception as e:  # noqa: BLE001
            return _json(400, {"error": str(e) or "Bad request"})

    # Payment Processor webhook: execute_update so duplicate or invalid captures
    # return an immediate HTTP 409 instead of silently being swallowed.
    async def capture_payment(request: web.Request) -> web.Response:
        try:
            req = await request.json()
            order_id = req["metadata"]["order_id"]
            payment = PaymentInput(rrn=req["rrn"], amount_cents=req["amount_cents"])
            await stub(order_id).execute_update(
                OrderProcessingWorkflow.capture_payment,
                payment,
            )
            return _json(
                200,
                {
                    "status": "payment_captured",
                    "rrn": req["rrn"],
                    "orderId": order_id,
                    "amountCents": req["amount_cents"],
                },
            )
        except WorkflowUpdateFailedError as e:
            # Validator rejected the payment (duplicate, cancelled, wrong phase, etc.).
            # str(e) returns the uninformative "Workflow update failed" — use e.cause.
            return _json(409, {"error": str(e.cause) or "Payment rejected"})
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND:
                return _json(
                    404, {"error": "Order not found or workflow already complete"}
                )
            return _json(400, {"error": str(e) or "Bad request"})
        except Exception as e:  # noqa: BLE001
            return _json(400, {"error": str(e) or "Bad request"})

    # Live status query: calls the @query method on the running workflow
    async def order_status(request: web.Request) -> web.Response:
        try:
            order_id = _require_param(request, "orderId")
            status = await stub(order_id).query(
                OrderProcessingWorkflow.get_order_status
            )
            return _json(200, {"orderId": order_id, "status": status})
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND:
                return _json(
                    404,
                    {
                        "error": "Order not found or workflow already complete",
                        "hint": "Check Temporal UI at http://localhost:8080 for final status",
                    },
                )
            return _json(400, {"error": str(e) or "Bad request"})
        except Exception as e:  # noqa: BLE001
            return _json(400, {"error": str(e) or "Bad request"})

    app = web.Application()
    app.add_routes(
        [
            web.post("/api/orders/correct", correct_order),
            web.post("/api/orders/cancel", cancel_order),
            web.get("/api/orders/status", order_status),
            web.post("/api/orders", start_order),
            web.post("/api/payments", capture_payment),
            web.options("/api/{tail:.*}", options_handler),
        ]
    )
    return app


async def _build_app() -> web.Application:
    target = os.environ.get("TEMPORAL_HOST", "127.0.0.1:7233")
    client = await Client.connect(target)
    return make_app(client)


def main() -> None:
    print("OMS API listening on :3000")
    web.run_app(_build_app(), host="0.0.0.0", port=3000)


if __name__ == "__main__":
    main()
