"""Comprehensive test suite for OrderProcessingWorkflow.

Techniques mirrored from the original Java suite:
  - WorkflowEnvironment time-skipping for deterministic, fast execution
  - env.sleep() for instant time travel (no real waiting)
  - Updates / Signals sent from the test coroutine to the running workflow
  - A recording activities double for isolation and call verification
  - Ordered ("InOrder") verification of workflow phase transitions
  - Dual-queue setup (OMS_QUEUE + COMMERCE_QUEUE) mirroring production
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from datetime import timedelta

import pytest
import pytest_asyncio
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle, WorkflowUpdateFailedError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from oms.activities import fulfillment_message
from oms.models import (
    DashboardUpdate,
    EnrichedOrder,
    EnrichmentInput,
    ERR_INVALID_ORDER,
    ERR_ORDER_CANCELLED,
    ERR_PAYMENT_NOT_ACCEPTED,
    OrderInput,
    OrderItem,
    PaymentInput,
    PaymentValidationInput,
)
from oms.constants import COMMERCE_QUEUE, OMS_QUEUE
from oms.workflows import OrderProcessingWorkflow


# ── Recording activities double ──────────────────────────────────────────────


@dataclass
class Recorder:
    dashboard: list[tuple[str, str]] = field(default_factory=list)
    enrich_calls: list[tuple[OrderInput, str]] = field(default_factory=list)
    publish_calls: list[EnrichedOrder] = field(default_factory=list)
    validate_rrn_calls: list[str] = field(default_factory=list)
    # Config knobs
    validate_order_results: list[bool] = field(default_factory=list)
    validate_rrn_overrides: dict[str, bool] = field(default_factory=dict)
    risk_ok: bool = True
    raise_fulfillment_error: bool = False  # OMS-003: trigger FULFILLMENT_FAILED path
    _order_idx: int = 0

    def statuses_for(self, order_id: str) -> list[str]:
        return [s for (oid, s) in self.dashboard if oid == order_id]

    def count(self, order_id: str, status: str) -> int:
        return self.statuses_for(order_id).count(status)


class RecordingActivities:
    """Configurable double exposing the same activity names as OmsActivities."""

    def __init__(self, rec: Recorder) -> None:
        self.rec = rec

    @activity.defn
    async def validate_order_api(self, order: OrderInput) -> bool:
        results = self.rec.validate_order_results
        if not results:
            return True
        idx = min(self.rec._order_idx, len(results) - 1)
        self.rec._order_idx += 1
        return results[idx]

    @activity.defn
    async def assess_order_risk(self, order: OrderInput) -> bool:
        return self.rec.risk_ok

    @activity.defn
    async def validate_payment_rrn(self, input: PaymentValidationInput) -> bool:
        self.rec.validate_rrn_calls.append(input.rrn)
        return self.rec.validate_rrn_overrides.get(input.rrn, True)

    @activity.defn
    async def enrich_with_pim(self, input: EnrichmentInput) -> EnrichedOrder:
        if self.rec.raise_fulfillment_error:
            from temporalio.exceptions import ApplicationError

            raise ApplicationError("PIM unavailable", non_retryable=True)
        self.rec.enrich_calls.append((input.order, input.rrn))
        items = [
            OrderItem(item.item_id, item.quantity, f"SKU-{item.item_id}", "BRAND-X")
            for item in input.order.items
        ]
        return EnrichedOrder(input.order.customer_id, input.order.order_id, input.rrn, items)

    @activity.defn
    async def update_customer_dashboard(self, update: DashboardUpdate) -> None:
        self.rec.dashboard.append((update.order_id, update.status))

    @activity.defn
    async def publish_to_fulfillment_kafka(self, order: EnrichedOrder) -> None:
        self.rec.publish_calls.append(order)


# ── Fixtures & helpers ────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def env():
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        yield env
    finally:
        await env.shutdown()


@contextlib.asynccontextmanager
async def workers(client: Client, rec: Recorder):
    acts = RecordingActivities(rec)
    # Mirror production topology (OMS-006):
    #   OMS_QUEUE — all activities EXCEPT validate_order_api
    #   COMMERCE_QUEUE — validate_order_api only, rate-capped at 150 RPS
    # This ensures tests would fail if the workflow accidentally routes
    # validate_order_api to OMS_QUEUE (which has no worker for it in prod).
    oms_activity_methods = [
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
        activities=oms_activity_methods,
    )
    commerce_worker = Worker(
        client, task_queue=COMMERCE_QUEUE, activities=[acts.validate_order_api]
    )
    async with oms_worker, commerce_worker:
        yield


async def start(client: Client, order_id: str, order: OrderInput) -> WorkflowHandle:
    # Mirror the execution_timeout from api.py so that time-skipping tests that
    # jump 30+ days are validated against the same ceiling as production.
    return await client.start_workflow(
        OrderProcessingWorkflow.process_order,
        order,
        id=order_id,
        task_queue=OMS_QUEUE,
        execution_timeout=timedelta(days=90),
    )


def valid_order(order_id: str) -> OrderInput:
    return OrderInput("CUST-1", order_id, [OrderItem("ITEM-1", 2)])


def invalid_order(order_id: str) -> OrderInput:
    return OrderInput("CUST-1", order_id, [])  # empty items → validate returns False


def payment(rrn: str) -> PaymentInput:
    return PaymentInput(rrn, 5000)


def assert_subsequence(actual: list[str], expected: list[str]) -> None:
    """Assert `expected` appears as an ordered subsequence of `actual`."""
    it = iter(actual)
    for want in expected:
        assert want in it, f"{want!r} not found in order within {actual!r}"


SETTLE = timedelta(seconds=1)


# ── Test 1: Happy Path ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_happy_path(env):
    rec = Recorder()
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-HAPPY", valid_order("ORD-HAPPY"))
        await env.sleep(SETTLE)  # reaches AWAITING_PAYMENT
        await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-001"))
        await wf.result()

    assert_subsequence(
        rec.statuses_for("ORD-HAPPY"),
        ["AWAITING_PAYMENT", "VALIDATING_PAYMENT", "ENRICHING", "FULFILLED"],
    )
    assert rec.validate_rrn_calls == ["RRN-001"]
    assert [rrn for (_, rrn) in rec.enrich_calls] == ["RRN-001"]
    assert len(rec.publish_calls) == 1


# ── Test 2: 30-Day TTL Expiration Cancels Order ────────────────────────────────


@pytest.mark.asyncio
async def test_ttl_expiration(env):
    rec = Recorder()
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-EXPIRE", valid_order("ORD-EXPIRE"))
        await env.sleep(timedelta(days=31))  # skip past the 30-day window
        await wf.result()

    assert_subsequence(rec.statuses_for("ORD-EXPIRE"), ["EXPIRED", "CANCELLED"])
    assert rec.enrich_calls == []
    assert rec.publish_calls == []


# ── Test 3: Cancellation Before Payment ────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancellation_before_payment(env):
    rec = Recorder()
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-CANCEL", valid_order("ORD-CANCEL"))
        await env.sleep(SETTLE)
        await wf.signal(OrderProcessingWorkflow.cancel_order)
        await wf.result()

    assert "CANCELLED" in rec.statuses_for("ORD-CANCEL")
    assert rec.enrich_calls == []
    assert rec.publish_calls == []


# ── Test 4: Cancellation After Payment Is a No-Op ──────────────────────────────


@pytest.mark.asyncio
async def test_cancellation_after_payment_is_noop(env):
    rec = Recorder()
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-CANCEL-LATE", valid_order("ORD-CANCEL-LATE"))
        await env.sleep(SETTLE)
        await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-002"))
        await wf.signal(OrderProcessingWorkflow.cancel_order)  # ignored
        await wf.result()

    statuses = rec.statuses_for("ORD-CANCEL-LATE")
    assert "FULFILLED" in statuses
    assert "CANCELLED" not in statuses


# ── Test 5: Single Correction → Payment → Fulfilled ────────────────────────────


@pytest.mark.asyncio
async def test_single_correction_then_fulfillment(env):
    rec = Recorder(validate_order_results=[False, True])
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-CORR", invalid_order("ORD-CORR"))
        await env.sleep(SETTLE)  # reaches PENDING_CORRECTION
        assert "PENDING_CORRECTION" in rec.statuses_for("ORD-CORR")

        await wf.execute_update(
            OrderProcessingWorkflow.handle_correction, valid_order("ORD-CORR")
        )
        await env.sleep(SETTLE)  # re-validates → AWAITING_PAYMENT
        await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-003"))
        await wf.result()

    assert_subsequence(
        rec.statuses_for("ORD-CORR"),
        ["PENDING_CORRECTION", "AWAITING_PAYMENT", "FULFILLED"],
    )


# ── Test 6: Multiple Corrections Required ──────────────────────────────────────
# The update validator enforces that corrections must have at least one item.
# Both corrections here pass the validator; the Commerce API mock rejects the
# first (results[1]=False) and accepts the second (results[2]=True).


@pytest.mark.asyncio
async def test_multiple_correction_attempts(env):
    rec = Recorder(validate_order_results=[False, False, True])
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-MULTI-CORR", invalid_order("ORD-MULTI-CORR"))
        await env.sleep(SETTLE)
        # First correction: validator accepts it (non-empty items), Commerce API rejects it
        await wf.execute_update(
            OrderProcessingWorkflow.handle_correction, valid_order("ORD-MULTI-CORR")
        )
        await env.sleep(SETTLE)
        # Second correction: validator and Commerce API both accept it
        await wf.execute_update(
            OrderProcessingWorkflow.handle_correction, valid_order("ORD-MULTI-CORR")
        )
        await env.sleep(SETTLE)
        await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-004"))
        await wf.result()

    assert rec.count("ORD-MULTI-CORR", "PENDING_CORRECTION") == 2
    assert "AWAITING_PAYMENT" in rec.statuses_for("ORD-MULTI-CORR")
    assert "FULFILLED" in rec.statuses_for("ORD-MULTI-CORR")


# ── Test 7: Empty-Items Correction Is Rejected by Update Validator ──────────────


@pytest.mark.asyncio
async def test_empty_items_correction_is_rejected(env):
    rec = Recorder(validate_order_results=[False])
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-EMPTY-CORR", invalid_order("ORD-EMPTY-CORR"))
        await env.sleep(SETTLE)  # reaches PENDING_CORRECTION

        # Empty-items correction: rejected by validator, NOT written to history
        with pytest.raises(WorkflowUpdateFailedError) as exc_info:
            await wf.execute_update(
                OrderProcessingWorkflow.handle_correction, invalid_order("ORD-EMPTY-CORR")
            )
        assert isinstance(exc_info.value.cause, ApplicationError)
        assert "at least one item" in str(exc_info.value.cause)

        # Workflow is still in PENDING_CORRECTION after the rejected update
        status = await wf.query(OrderProcessingWorkflow.get_order_status)
    assert status == "PENDING_CORRECTION"


# ── Test 8: Invalid RRN Is Rejected by Payment Processor ───────────────────────


@pytest.mark.asyncio
async def test_invalid_rrn_payment(env):
    rec = Recorder(validate_rrn_overrides={"INVALID-RRN-BAD": False})
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-BAD-RRN", valid_order("ORD-BAD-RRN"))
        await env.sleep(SETTLE)
        # Validator accepts it (non-blank RRN, not cancelled, no duplicate)
        await wf.execute_update(
            OrderProcessingWorkflow.capture_payment, payment("INVALID-RRN-BAD")
        )
        await env.sleep(SETTLE)
        status = await wf.query(OrderProcessingWorkflow.get_order_status)

    assert status == "PAYMENT_INVALID"
    assert "PAYMENT_INVALID" in rec.statuses_for("ORD-BAD-RRN")
    assert rec.enrich_calls == []
    assert rec.publish_calls == []


# ── Test 9: Duplicate Payment Update Is Rejected by Validator ──────────────────
# With Signal, the second call was a silent no-op.
# With Update + validator, the second call raises WorkflowUpdateFailedError
# so the caller (e.g. payment webhook) gets an explicit DUPLICATE_PAYMENT error.


@pytest.mark.asyncio
async def test_duplicate_payment_update_is_rejected(env):
    rec = Recorder()
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-DUPE-PAY", valid_order("ORD-DUPE-PAY"))
        await env.sleep(SETTLE)
        # First capture: accepted
        await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-FIRST"))
        # Second capture: rejected — _captured_payment already set
        with pytest.raises(WorkflowUpdateFailedError) as exc_info:
            await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-SECOND"))
        assert isinstance(exc_info.value.cause, ApplicationError)
        assert exc_info.value.cause.type == "DUPLICATE_PAYMENT"
        # Workflow completes using only the first RRN
        await wf.result()

    used_rrns = [rrn for (_, rrn) in rec.enrich_calls]
    assert used_rrns == ["RRN-FIRST"]
    assert "RRN-SECOND" not in used_rrns
    assert "FULFILLED" in rec.statuses_for("ORD-DUPE-PAY")


# ── Test 10: Payment on a Cancelled Order Is Rejected by Validator ─────────────
# The cancel signal sets _is_cancelled=True. If the execute_update arrives
# before the workflow processes the wait_condition and exits, the validator
# fires with ORDER_CANCELLED. If the workflow has already exited, the server
# returns NOT_FOUND. Both outcomes correctly prevent the payment from landing.
# We send the signal and update back-to-back (no sleep) so they arrive in the
# same window — the SDK delivers them in order, giving the validator a chance
# to fire before the workflow processes the condition change.


@pytest.mark.asyncio
async def test_capture_payment_on_cancelled_order_is_rejected(env):
    from temporalio.service import RPCError

    rec = Recorder()
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-CANCEL-PAY", valid_order("ORD-CANCEL-PAY"))
        await env.sleep(SETTLE)  # reaches AWAITING_PAYMENT
        # Send cancel and payment capture without sleeping; both arrive before
        # the workflow has a chance to process the condition and return.
        await wf.signal(OrderProcessingWorkflow.cancel_order)
        try:
            await wf.execute_update(
                OrderProcessingWorkflow.capture_payment, payment("RRN-TOO-LATE")
            )
            pytest.fail("Expected payment to be rejected on a cancelled order")
        except WorkflowUpdateFailedError as exc:
            # Validator fired: order was in AWAITING_PAYMENT and _is_cancelled=True
            assert isinstance(exc.cause, ApplicationError)
            assert exc.cause.type == ERR_ORDER_CANCELLED
        except RPCError:
            # Workflow had already exited (CANCELLED + drain completed) before
            # the update arrived — equally correct: no payment was captured.
            pass
        await wf.result()

    assert "CANCELLED" in rec.statuses_for("ORD-CANCEL-PAY")
    assert rec.enrich_calls == []


# ── Test 11: Query Method Returns Live Status ───────────────────────────────────


@pytest.mark.asyncio
async def test_get_order_status_query(env):
    rec = Recorder()
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-QUERY", valid_order("ORD-QUERY"))
        await env.sleep(SETTLE)
        status = await wf.query(OrderProcessingWorkflow.get_order_status)
        assert status == "AWAITING_PAYMENT"

        await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-QUERY"))
        await wf.result()


# ── Test 12: Cancellation During 30-Day Window Does Not Expire ──────────────────


@pytest.mark.asyncio
async def test_cancellation_at_mid_window(env):
    rec = Recorder()
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-MID-CANCEL", valid_order("ORD-MID-CANCEL"))
        await env.sleep(timedelta(days=15))  # halfway through the window
        await wf.signal(OrderProcessingWorkflow.cancel_order)
        await wf.result()

    statuses = rec.statuses_for("ORD-MID-CANCEL")
    assert "CANCELLED" in statuses
    assert "EXPIRED" not in statuses


# ── Test 13: PAYMENT_INVALID Retry Loop — Corrected RRN Leads to Fulfillment ───


@pytest.mark.asyncio
async def test_invalid_rrn_then_valid_rrn_fulfills(env):
    rec = Recorder(validate_rrn_overrides={"RRN-BAD": False, "RRN-GOOD": True})
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-RRN-RETRY", valid_order("ORD-RRN-RETRY"))
        await env.sleep(SETTLE)  # AWAITING_PAYMENT
        # First payment: validator accepts (non-blank RRN), workflow rejects via activity
        await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-BAD"))
        await env.sleep(SETTLE)  # PAYMENT_INVALID, _captured_payment reset to None
        assert "PAYMENT_INVALID" in rec.statuses_for("ORD-RRN-RETRY")
        assert rec.enrich_calls == []

        # Second payment: validator accepts (captured_payment is None after reset)
        await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-GOOD"))
        await wf.result()

    assert_subsequence(
        rec.statuses_for("ORD-RRN-RETRY"),
        [
            "AWAITING_PAYMENT",
            "VALIDATING_PAYMENT",
            "PAYMENT_INVALID",
            "VALIDATING_PAYMENT",
            "ENRICHING",
            "FULFILLED",
        ],
    )
    assert [rrn for (_, rrn) in rec.enrich_calls] == ["RRN-GOOD"]
    assert "RRN-BAD" not in [rrn for (_, rrn) in rec.enrich_calls]


# ── Test 14: vNext risk gate (workflow.patched) ──────────────────────────


@pytest.mark.asyncio
async def test_happy_path_passes_risk_gate(env):
    # New workflows run the patched risk gate; a low-risk order proceeds normally.
    rec = Recorder()
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-RISK-OK", valid_order("ORD-RISK-OK"))
        await env.sleep(SETTLE)
        await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-R1"))
        await wf.result()

    assert_subsequence(
        rec.statuses_for("ORD-RISK-OK"),
        ["AWAITING_RISK_ASSESSMENT", "AWAITING_PAYMENT", "FULFILLED"],
    )


# ── Test 15: high-risk order is rejected before payment ────────────────────


@pytest.mark.asyncio
async def test_high_risk_order_rejected(env):
    rec = Recorder(risk_ok=False)
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-RISK-BAD", valid_order("ORD-RISK-BAD"))
        await wf.result()

    statuses = rec.statuses_for("ORD-RISK-BAD")
    assert "RISK_REJECTED" in statuses
    assert "AWAITING_PAYMENT" not in statuses
    assert rec.enrich_calls == []
    assert rec.publish_calls == []


# ── Test 16: fulfillment Kafka contract ───────────────────────────────────────


def test_fulfillment_message_contract():
    order = EnrichedOrder(
        customer_id="CUST-1",
        order_id="ORD-KAFKA",
        rrn="RRN-K1",
        items=[
            OrderItem("ITEM-A", 2, "SKU-A", "BRAND-A"),
            OrderItem("ITEM-B", 1, "SKU-B", "BRAND-B"),
        ],
    )

    assert fulfillment_message(order) == {
        "customer_id": "CUST-1",
        "order_id": "ORD-KAFKA",
        "payment_details": {"rrn": "RRN-K1"},
        "items": [
            {"item_id": "ITEM-A", "sku_id": "SKU-A", "brand_code": "BRAND-A"},
            {"item_id": "ITEM-B", "sku_id": "SKU-B", "brand_code": "BRAND-B"},
        ],
    }


# ── Test 17: PAYMENT_EXPIRED after 7-day retry window (OMS-001) ──────────────
# Verifies that an invalid RRN followed by no resubmission within 7 days
# causes the workflow to exit cleanly as PAYMENT_EXPIRED (not hang indefinitely).


@pytest.mark.asyncio
async def test_payment_expired_after_retry_window(env):
    rec = Recorder(validate_rrn_overrides={"RRN-EXPIRE": False})
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-PAY-EXPIRE", valid_order("ORD-PAY-EXPIRE"))
        await env.sleep(SETTLE)  # AWAITING_PAYMENT
        await wf.execute_update(
            OrderProcessingWorkflow.capture_payment, payment("RRN-EXPIRE")
        )
        await env.sleep(SETTLE)          # PAYMENT_INVALID; 7-day resubmit window starts
        await env.sleep(timedelta(days=8))  # skip past PAYMENT_RETRY_TIMEOUT
        await wf.result()

    statuses = rec.statuses_for("ORD-PAY-EXPIRE")
    assert "PAYMENT_INVALID" in statuses
    assert "PAYMENT_EXPIRED" in statuses
    assert rec.enrich_calls == []
    assert rec.publish_calls == []


# ── Test 18: FULFILLMENT_FAILED when Phase 5 activity exhausts retries (OMS-003) ─
# Verifies that activity failure in Phase 5 writes a terminal FULFILLMENT_FAILED
# status to the dashboard instead of leaving it stuck in "ENRICHING".


@pytest.mark.asyncio
async def test_fulfillment_failed_writes_terminal_status(env):
    rec = Recorder(raise_fulfillment_error=True)
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-FF", valid_order("ORD-FF"))
        await env.sleep(SETTLE)
        await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-FF"))
        with pytest.raises(WorkflowFailureError):
            await wf.result()

    statuses = rec.statuses_for("ORD-FF")
    assert "ENRICHING" in statuses
    assert "FULFILLMENT_FAILED" in statuses
    assert rec.publish_calls == []


# ── Test 19: CORRECTION_TIMEOUT after 30-day correction window (OMS-009) ──────
# Verifies that an order stuck in PENDING_CORRECTION with no correction submitted
# within 30 days exits cleanly as CORRECTION_TIMEOUT (not hang indefinitely).


@pytest.mark.asyncio
async def test_correction_timeout(env):
    rec = Recorder(validate_order_results=[False])
    async with workers(env.client, rec):
        wf = await start(
            env.client, "ORD-CORR-TIMEOUT", invalid_order("ORD-CORR-TIMEOUT")
        )
        await env.sleep(SETTLE)  # PENDING_CORRECTION
        assert "PENDING_CORRECTION" in rec.statuses_for("ORD-CORR-TIMEOUT")
        await env.sleep(timedelta(days=31))  # skip past CORRECTION_WAIT_TIMEOUT
        await wf.result()

    statuses = rec.statuses_for("ORD-CORR-TIMEOUT")
    assert "PENDING_CORRECTION" in statuses
    assert "CORRECTION_TIMEOUT" in statuses
    assert rec.enrich_calls == []
    assert rec.publish_calls == []


# ── Test 20: cancel_order during PENDING_CORRECTION exits promptly as CANCELLED ─
# OMS-016: Phase 1 wait_condition now includes is_cancelled so a business
# cancel_order signal immediately unblocks the wait instead of letting the
# workflow park for up to 30 days before CORRECTION_TIMEOUT fires.


@pytest.mark.asyncio
async def test_cancel_during_pending_correction(env):
    rec = Recorder(validate_order_results=[False])  # always invalid → PENDING_CORRECTION
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-CANCEL-CORR", invalid_order("ORD-CANCEL-CORR"))
        await env.sleep(SETTLE)  # PENDING_CORRECTION; correction window opens
        assert "PENDING_CORRECTION" in rec.statuses_for("ORD-CANCEL-CORR")

        await wf.signal(OrderProcessingWorkflow.cancel_order)
        await env.sleep(SETTLE)  # workflow unblocks immediately (not after 30 days)
        await wf.result()

    statuses = rec.statuses_for("ORD-CANCEL-CORR")
    assert "PENDING_CORRECTION" in statuses
    assert "CANCELLED" in statuses
    # Must not wait for the 30-day timeout
    assert "CORRECTION_TIMEOUT" not in statuses
    assert rec.enrich_calls == []
    assert rec.publish_calls == []


# ── Test 21: cancel_order during PAYMENT_INVALID exits promptly as CANCELLED ────
# OMS-016: Phase 4 wait_condition now includes is_cancelled so a business
# cancel_order signal immediately unblocks the payment-retry wait instead of
# letting the workflow park for up to 7 days before PAYMENT_EXPIRED fires.


@pytest.mark.asyncio
async def test_cancel_during_payment_invalid_wait(env):
    rec = Recorder(validate_rrn_overrides={"RRN-CANCEL-BAD": False})
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-CANCEL-INV", valid_order("ORD-CANCEL-INV"))
        await env.sleep(SETTLE)  # AWAITING_PAYMENT
        await wf.execute_update(
            OrderProcessingWorkflow.capture_payment, payment("RRN-CANCEL-BAD")
        )
        await env.sleep(SETTLE)  # PAYMENT_INVALID; 7-day resubmit window opens
        assert "PAYMENT_INVALID" in rec.statuses_for("ORD-CANCEL-INV")

        await wf.signal(OrderProcessingWorkflow.cancel_order)
        await env.sleep(SETTLE)  # workflow unblocks immediately (not after 7 days)
        await wf.result()

    statuses = rec.statuses_for("ORD-CANCEL-INV")
    assert "PAYMENT_INVALID" in statuses
    assert "CANCELLED" in statuses
    # Must not wait for the 7-day timeout
    assert "PAYMENT_EXPIRED" not in statuses
    assert rec.enrich_calls == []
    assert rec.publish_calls == []


# ── Test 22: handle_correction rejected when workflow is not in PENDING_CORRECTION
# OMS-019: phase guard in handle_correction validator prevents the Update from
# silently overwriting current_order while the order is in any other phase.


@pytest.mark.asyncio
async def test_correction_rejected_outside_pending_correction(env):
    rec = Recorder()
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-CORR-PHASE", valid_order("ORD-CORR-PHASE"))
        await env.sleep(SETTLE)  # AWAITING_PAYMENT — corrections not valid here

        with pytest.raises(WorkflowUpdateFailedError) as exc_info:
            await wf.execute_update(
                OrderProcessingWorkflow.handle_correction,
                valid_order("ORD-CORR-PHASE"),
            )
        exc = exc_info.value
        assert isinstance(exc.cause, ApplicationError)
        assert exc.cause.type == ERR_INVALID_ORDER

        # Workflow must be unaffected — complete the happy path normally
        await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-PHASE"))
        await wf.result()

    statuses = rec.statuses_for("ORD-CORR-PHASE")
    assert "FULFILLED" in statuses
    # Original order items were used (correction was rejected before being recorded)
    assert len(rec.enrich_calls) == 1


# ── Test 23: capture_payment rejected outside payment-waiting phases (OMS-020) ──
# Verifies that the phase guard in validate_capture_payment rejects a payment
# Update when the workflow is not in AWAITING_PAYMENT or PAYMENT_INVALID,
# preventing the payment from being silently orphaned with no fulfillment.
# We use PENDING_CORRECTION as the non-payable phase because it is a stable,
# observable state (the workflow parks there), unlike terminal states which
# complete before the test can reliably submit the Update.


@pytest.mark.asyncio
async def test_capture_payment_rejected_when_not_payable(env):
    """capture_payment must be rejected when the workflow is in a non-payable
    phase (OMS-020: phase guard prevents orphaned payments)."""
    rec = Recorder(validate_order_results=[False, True])  # invalid → then valid
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-PAY-PHASE", invalid_order("ORD-PAY-PHASE"))
        await env.sleep(SETTLE)  # parked in PENDING_CORRECTION (stable, non-payable)

        assert "PENDING_CORRECTION" in rec.statuses_for("ORD-PAY-PHASE")

        with pytest.raises(WorkflowUpdateFailedError) as exc_info:
            await wf.execute_update(
                OrderProcessingWorkflow.capture_payment, payment("RRN-ORPHAN")
            )
        exc = exc_info.value
        assert isinstance(exc.cause, ApplicationError)
        assert exc.cause.type == ERR_PAYMENT_NOT_ACCEPTED

        # Unblock the workflow so it can complete cleanly
        await wf.execute_update(
            OrderProcessingWorkflow.handle_correction, valid_order("ORD-PAY-PHASE")
        )
        await env.sleep(SETTLE)
        await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-VALID"))
        await wf.result()

    statuses = rec.statuses_for("ORD-PAY-PHASE")
    assert "FULFILLED" in statuses
    # Only one (valid) payment capture resulted in fulfillment
    assert len(rec.enrich_calls) == 1


# ── Test 24: correction applied mid-loop without duplicate submission (OMS-021) ─
# Verifies that a handle_correction Update arriving concurrently during the
# validate_order_api activity does not require the support team to re-submit the
# same correction: the corrected current_order is validated on the very next
# iteration without parking again.


@pytest.mark.asyncio
async def test_correction_applied_without_duplicate_submission(env):
    """A handle_correction Update accepted between validate_order_api calls must
    allow the loop to re-validate immediately on the next iteration rather than
    waiting indefinitely for a second correction (OMS-021 guard)."""
    # First call returns False (invalid), second returns True (corrected order valid)
    rec = Recorder(validate_order_results=[False, True])
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-CORR-LOOP", invalid_order("ORD-CORR-LOOP"))
        await env.sleep(SETTLE)  # PENDING_CORRECTION
        assert "PENDING_CORRECTION" in rec.statuses_for("ORD-CORR-LOOP")

        # Submit the correction exactly once — the loop should re-validate and proceed
        await wf.execute_update(
            OrderProcessingWorkflow.handle_correction, valid_order("ORD-CORR-LOOP")
        )
        await env.sleep(SETTLE)  # re-validation passes; should reach AWAITING_PAYMENT

        # Complete the happy path
        await wf.execute_update(OrderProcessingWorkflow.capture_payment, payment("RRN-LOOP"))
        await wf.result()

    statuses = rec.statuses_for("ORD-CORR-LOOP")
    assert_subsequence(
        statuses,
        ["PENDING_CORRECTION", "AWAITING_PAYMENT", "FULFILLED"],
    )
    # Correction was applied exactly once — no re-submission needed
    assert rec.enrich_calls != []
    assert len(rec.publish_calls) == 1


# ── Test 25: External Temporal cancellation writes CANCELLED status ─────────────
# Verifies that handle.cancel() (operator/UI termination) triggers the
# asyncio.CancelledError → asyncio.shield → _cleanup_on_cancel path,
# writing a terminal CANCELLED status and draining handlers.


@pytest.mark.asyncio
async def test_external_temporal_cancellation_writes_cancelled_status(env):
    rec = Recorder()
    async with workers(env.client, rec):
        wf = await start(env.client, "ORD-EXT-CANCEL", valid_order("ORD-EXT-CANCEL"))
        await env.sleep(SETTLE)  # reaches AWAITING_PAYMENT
        await wf.cancel()
        with pytest.raises(WorkflowFailureError):
            await wf.result()

    assert "CANCELLED" in rec.statuses_for("ORD-EXT-CANCEL")
    assert rec.enrich_calls == []
    assert rec.publish_calls == []
