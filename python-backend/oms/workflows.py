"""Order processing workflow — durable orchestration of the full order lifecycle.

Temporal primitives used:
  - @workflow.run     -> process_order entry point (durable execution)
  - @workflow.update  -> capture_payment  (validated sync request; replaces Signal so
                         the Payment Processor webhook gets immediate accept/reject feedback)
  - @workflow.update  -> handle_correction (validated sync request; rejects bad payloads
                         before they are written to Event History)
  - @workflow.signal  -> cancel_order (fire-and-forget; no confirmation needed)
  - @workflow.query   -> get_order_status (returns status string; backward-compatible)
  - @workflow.query   -> get_order_state  (returns full WorkflowState for richer observability)
  - workflow.wait_condition(timeout=timedelta(days=30))  -> payment window TTL
  - workflow.wait_condition(timeout=PAYMENT_RETRY_TIMEOUT) -> bounded PAYMENT_INVALID wait
  - workflow.wait_condition(all_handlers_finished)        -> TMPRL1102 handler-drain guard
  - workflow.patched(...) -> safe in-place versioning for the vNext risk gate
  - workflow_id = order_id -> exactly-once workflow per order (idempotency)
  - dedicated COMMERCE_QUEUE -> Commerce API rate limit isolated to 150 RPS

Why Updates instead of Signals for payment and correction?
  - Signals are fire-and-forget: the caller gets no validation feedback.
  - Updates allow a validator to reject invalid input *before* it is written to
    Event History; the caller receives an immediate error rather than a silent no-op.
  - This lets the Payment Processor webhook know instantly if a payment was already
    captured, and the support UI know instantly if a correction was rejected.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from oms.activities import OmsActivities
    from oms.models import (
        DashboardUpdate,
        EnrichedOrder,
        EnrichmentInput,
        ERR_DUPLICATE_PAYMENT,
        ERR_INVALID_ORDER,
        ERR_INVALID_RRN,
        ERR_ORDER_CANCELLED,
        ERR_PAYMENT_NOT_ACCEPTED,
        OrderInput,
        OrderStatus,
        PaymentInput,
        PaymentValidationInput,
        WorkflowState,
    )
    from oms.constants import COMMERCE_QUEUE, ORDER_STATUS_ATTR

# ── Retry policy shared across all activity calls ────────────────────────────
# - No maximum_attempts: per Temporal guidance, use schedule_to_close_timeout
#   to bound total retry time rather than a fixed attempt count. This matches
#   elapsed-time SLOs better than counting retries.
# - non_retryable_error_types: defence-in-depth on top of the
#   ApplicationError(non_retryable=True) raised inside validate_payment_rrn.
STANDARD_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
    # OMS-013: reference constants from models.py — no magic string literals here.
    non_retryable_error_types=[ERR_INVALID_RRN, ERR_INVALID_ORDER, ERR_ORDER_CANCELLED],
)

# NOTE: Patching lifecycle — this is step 1 (patched).
# Step 2: Once all pre-patch workflows have completed, replace
#   `if workflow.patched(RISK_PATCH):` with `workflow.deprecate_patch(RISK_PATCH)`.
# Step 3: Once no executions reference the patch, remove the patch infrastructure
#   entirely and always run the risk gate.
RISK_PATCH = "risk-assessment-v1"

# Retry policy for the terminal Kafka fulfillment publish.
# Uses a longer schedule_to_close_timeout than STANDARD_RETRY to survive Kafka
# cluster failovers (typically 30–120 s) without permanently failing the order.
# An outage exceeding 30 minutes is treated as a terminal infrastructure failure
# and surfaces as FULFILLMENT_FAILED.  The backoff starts at 5 s (not 1 s) to
# avoid hammering a cluster that is actively recovering.
FULFILLMENT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    maximum_interval=timedelta(minutes=5),
    backoff_coefficient=2.0,
    non_retryable_error_types=[ERR_INVALID_RRN, ERR_INVALID_ORDER, ERR_ORDER_CANCELLED],
)
FULFILLMENT_SCHEDULE_TO_CLOSE = timedelta(minutes=30)

# Per-attempt execution budget for a single Activity Task Execution.
# Must equal or exceed the p99 downstream latency of the slowest I/O call
# (Commerce API, Payment Processor, PIM, Kafka).
ACTIVITY_TIMEOUT = timedelta(seconds=30)

# Bounds total Activity Execution time across *all* attempts and wait intervals.
# With 1 s initial interval + 2× backoff capped at 30 s, this comfortably covers
# several retries (~1 + 2 + 4 + 8 + 30 s between attempts + up to 30 s per attempt).
SCHEDULE_TO_CLOSE = timedelta(minutes=5)

# Maximum silence before Temporal declares a worker dead and retries the Activity.
# Must be SHORTER than start_to_close_timeout so a crashed worker is detected
# *before* the activity is killed by its own timeout.  10 s means a genuinely
# crashed worker is discovered quickly, while the 30 s start_to_close budget gives
# the real downstream I/O enough time to complete on a healthy worker.
HEARTBEAT_TIMEOUT = timedelta(seconds=10)

# Maximum number of validate → fail → correct cycles before the workflow gives
# up and writes CORRECTION_TIMEOUT.  Each cycle contributes ~10-15 events to
# the workflow history (activity schedule/start/complete + timer + handler
# marker events).  100 cycles ≈ 1,000-1,500 events — well within the 50,000
# hard limit — while protecting against a runaway correction integration.
MAX_CORRECTION_ATTEMPTS = 100

# How long the workflow parks waiting for a support-team correction.
CORRECTION_WAIT_TIMEOUT = timedelta(days=30)

# How long the workflow parks waiting for payment after an invalid RRN.
PAYMENT_RETRY_TIMEOUT = timedelta(days=7)

# How long the workflow parks in the payment window before auto-expiring.
PAYMENT_WAIT_TIMEOUT = timedelta(days=30)

# Terminal business statuses — used by the Temporal-cancellation handler (OMS-004)
# to avoid writing CANCELLED on top of an already-resolved order.
# Using OrderStatus members instead of raw strings so a renamed/missing status
# is an AttributeError at import time, not a silent guard bypass at runtime.
#
# EXPIRED is included even though it is immediately followed by CANCELLED in the
# same TTL-expiry branch: both _update_status calls yield to the event loop (they
# await an activity), so an external handle.cancel() could arrive between them.
# Without EXPIRED here, _cleanup_on_cancel would see status=EXPIRED (not terminal)
# and write a redundant CANCELLED, producing a confusing double-write in the
# dashboard history.
_TERMINAL_STATUSES = frozenset(
    {
        OrderStatus.CANCELLED,
        OrderStatus.CORRECTION_TIMEOUT,
        OrderStatus.EXPIRED,
        OrderStatus.FULFILLED,
        OrderStatus.FULFILLMENT_FAILED,
        OrderStatus.PAYMENT_EXPIRED,
        OrderStatus.PAYMENT_VALIDATION_FAILED,
        OrderStatus.RISK_REJECTED,
        OrderStatus.RISK_ASSESSMENT_FAILED,
        OrderStatus.VALIDATION_FAILED,
    }
)


@workflow.defn
class OrderProcessingWorkflow:
    def __init__(self) -> None:
  
        self._state = WorkflowState()

    # ── Queries ────────────────────────────────────────────────────────────────

    @workflow.query
    def get_order_status(self) -> str:
        """Return the current status string (backward-compatible with existing callers)."""
        return self._state.current_status

    @workflow.query
    def get_order_state(self) -> WorkflowState:
        """Return the full cohesive WorkflowState for richer observability."""
        return self._state

    # ── Internal helpers ────────────────────────────────────────────────────────

    async def _update_status(self, status: OrderStatus) -> None:
        self._state.current_status = status
        # Upsert a Search Attribute so operators can filter open workflows by
        # business status in the Temporal UI and via ListWorkflowExecutions.
        # upsert_search_attributes is synchronous — no await, no history yield.
        workflow.upsert_search_attributes({ORDER_STATUS_ATTR: [str(status)]})
        await workflow.execute_activity_method(
            OmsActivities.update_customer_dashboard,
            DashboardUpdate(
                order_id=self._state.current_order.order_id, status=status
            ),
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            schedule_to_close_timeout=SCHEDULE_TO_CLOSE,
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            retry_policy=STANDARD_RETRY,
        )

    async def _drain_handlers(self) -> None:
        """Block until all Update and Signal handlers have completed.

        Prevents TMPRL1102 ("workflow finished while handlers still running").
        Must be called before every `return` in the main workflow function.
        """
        await workflow.wait_condition(workflow.all_handlers_finished)

    async def _cleanup_on_cancel(self) -> None:
        """Write terminal CANCELLED status and drain handlers after an external
        Temporal cancellation (e.g. operator pressing Terminate in the UI or a
        client calling handle.cancel()).

        Called inside asyncio.shield() so both operations complete even though
        the outer workflow task has already been cancelled.
        """
        if self._state.current_status not in _TERMINAL_STATUSES:
            try:
                await self._update_status(OrderStatus.CANCELLED)
            except ActivityError as exc:
                # Dashboard write failed during cancellation cleanup (e.g. DB unreachable).
                # The workflow must still exit cleanly — we cannot retry indefinitely inside
                # asyncio.shield — so we log the failure and continue.  Without this log the
                # dashboard would silently show a stale status (e.g. AWAITING_PAYMENT)
                # permanently.  Operators should monitor for this log pattern and correct the
                # dashboard record out-of-band if necessary.
                workflow.logger.error(
                    "Order %s: dashboard CANCELLED write failed during cancellation "
                    "cleanup — dashboard may show stale status: %s",
                    self._state.current_order.order_id if self._state.current_order else "unknown",
                    exc,
                )
        await self._drain_handlers()

    # ── Workflow entry point ─────────────────────────────────────────────────────

    @workflow.run
    async def process_order(self, order: OrderInput) -> None:
        self._state.current_order = order
        workflow.logger.info("Order %s: workflow started", order.order_id)
        try:
            # ── Phase 1: Validate with Commerce API — Support Team Correction Loop ──
            workflow.logger.info(
                "Order %s: entering validation loop",
                self._state.current_order.order_id,
            )
            validated = False
            correction_attempt = 0
            while not validated:
                try:
                    validated = await workflow.execute_activity_method(
                        OmsActivities.validate_order_api,
                        self._state.current_order,
                        task_queue=COMMERCE_QUEUE,
                        start_to_close_timeout=ACTIVITY_TIMEOUT,
                        schedule_to_close_timeout=SCHEDULE_TO_CLOSE,
                        heartbeat_timeout=HEARTBEAT_TIMEOUT,
                        retry_policy=STANDARD_RETRY,
                    )
                except ActivityError:
                    await self._update_status(OrderStatus.VALIDATION_FAILED)
                    await self._drain_handlers()
                    raise
                if not validated:
                    correction_attempt += 1
                    if correction_attempt > MAX_CORRECTION_ATTEMPTS:
                        workflow.logger.error(
                            "Order %s: exceeded maximum correction attempts (%d); "
                            "writing CORRECTION_TIMEOUT to prevent event-history bloat",
                            self._state.current_order.order_id,
                            MAX_CORRECTION_ATTEMPTS,
                        )
                        await self._update_status(OrderStatus.CORRECTION_TIMEOUT)
                        await self._drain_handlers()
                        return
                    await self._update_status(OrderStatus.PENDING_CORRECTION)

                    if not self._state.correction_received:
                        try:

                            await workflow.wait_condition(
                                lambda: self._state.correction_received
                                or self._state.is_cancelled,
                                timeout=CORRECTION_WAIT_TIMEOUT,
                            )
                        except asyncio.TimeoutError:
                            workflow.logger.info(
                                "Order %s: correction window expired",
                                self._state.current_order.order_id,
                            )
                            await self._update_status(OrderStatus.CORRECTION_TIMEOUT)
                            await self._drain_handlers()
                            return
                    # OMS-016: cancel_order arrived while waiting for a correction
                    if self._state.is_cancelled:
                        workflow.logger.info(
                            "Order %s: cancellation detected in correction loop",
                            self._state.current_order.order_id,
                        )
                        await self._update_status(OrderStatus.CANCELLED)
                        await self._drain_handlers()
                        return
                    # Reset the flag AFTER the wait so the next iteration starts clean.
                    self._state.correction_received = False

            # ── Phase 1.5 (vNext) — Risk assessment, versioned with workflow.patched ─
            # Workflows started BEFORE this change skip the block (they run on the
            # default version and continue straight to payment); workflows started
            # AFTER run the risk gate. This is Temporal's safe in-place versioning,
            # the Python equivalent of Java's Workflow.getVersion.
            if workflow.patched(RISK_PATCH):
                await self._update_status(OrderStatus.AWAITING_RISK_ASSESSMENT)
                workflow.logger.info(
                    "Order %s: entering risk assessment",
                    self._state.current_order.order_id,
                )
                try:
                    risk_ok = await workflow.execute_activity_method(
                        OmsActivities.assess_order_risk,
                        self._state.current_order,
                        start_to_close_timeout=ACTIVITY_TIMEOUT,
                        schedule_to_close_timeout=SCHEDULE_TO_CLOSE,
                        heartbeat_timeout=HEARTBEAT_TIMEOUT,
                        retry_policy=STANDARD_RETRY,
                    )
                except ActivityError:
                    await self._update_status(OrderStatus.RISK_ASSESSMENT_FAILED)
                    await self._drain_handlers()
                    raise
                if not risk_ok:
                    await self._update_status(OrderStatus.RISK_REJECTED)
                    await self._drain_handlers()
                    return

            # ── Phase 2: Race — Payment vs Cancellation vs TTL ──────────────────────
            await self._update_status(OrderStatus.AWAITING_PAYMENT)
            workflow.logger.info(
                "Order %s: entering payment wait window (TTL=%s)",
                self._state.current_order.order_id,
                PAYMENT_WAIT_TIMEOUT,
            )
            try:
                await workflow.wait_condition(
                    lambda: self._state.captured_payment is not None
                    or self._state.is_cancelled,
                    timeout=PAYMENT_WAIT_TIMEOUT,
                )
                resolved = True
            except asyncio.TimeoutError:
                workflow.logger.info(
                    "Order %s: payment window expired",
                    self._state.current_order.order_id,
                )
                resolved = False

            # ── Phase 3: Outcome Resolution ─────────────────────────────────────────
            if not resolved:
                await self._update_status(OrderStatus.EXPIRED)
                await self._update_status(OrderStatus.CANCELLED)
                await self._drain_handlers()
                return

            if self._state.is_cancelled:
                workflow.logger.info(
                    "Order %s: cancellation detected before payment",
                    self._state.current_order.order_id,
                )
                await self._update_status(OrderStatus.CANCELLED)
                await self._drain_handlers()
                return

            # ── Phase 4: Validate RRN against Payment Processor API ─────────────────
            payment_valid = False
            while not payment_valid:
                await self._update_status(OrderStatus.VALIDATING_PAYMENT)
                try:
                    rrn_valid = await workflow.execute_activity_method(
                        OmsActivities.validate_payment_rrn,
                        PaymentValidationInput(rrn=self._state.captured_payment.rrn),
                        start_to_close_timeout=ACTIVITY_TIMEOUT,
                        schedule_to_close_timeout=SCHEDULE_TO_CLOSE,
                        heartbeat_timeout=HEARTBEAT_TIMEOUT,
                        retry_policy=STANDARD_RETRY,
                    )
                except ActivityError:
                    await self._update_status(OrderStatus.PAYMENT_VALIDATION_FAILED)
                    await self._drain_handlers()
                    raise

                if not rrn_valid:
                    await self._update_status(OrderStatus.PAYMENT_INVALID)
                    self._state.captured_payment = None
                    try:
                        await workflow.wait_condition(
                            lambda: self._state.captured_payment is not None
                            or self._state.is_cancelled,
                            timeout=PAYMENT_RETRY_TIMEOUT,
                        )
                    except asyncio.TimeoutError:
                        await self._update_status(OrderStatus.PAYMENT_EXPIRED)
                        await self._drain_handlers()
                        return
                    if self._state.is_cancelled:
                        await self._update_status(OrderStatus.CANCELLED)
                        await self._drain_handlers()
                        return
                else:
                    payment_valid = True

            # ── Phase 5: Enrich with PIM and publish to Kafka ────────────────────────
            await self._update_status(OrderStatus.ENRICHING)
            workflow.logger.info(
                "Order %s: entering fulfillment (PIM + Kafka)",
                self._state.current_order.order_id,
            )
            try:
                final_order: EnrichedOrder = await workflow.execute_activity_method(
                    OmsActivities.enrich_with_pim,
                    EnrichmentInput(
                        order=self._state.current_order,
                        rrn=self._state.captured_payment.rrn,
                    ),
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    schedule_to_close_timeout=SCHEDULE_TO_CLOSE,
                    heartbeat_timeout=HEARTBEAT_TIMEOUT,
                    retry_policy=STANDARD_RETRY,
                )
                await workflow.execute_activity_method(
                    OmsActivities.publish_to_fulfillment_kafka,
                    final_order,
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    # FULFILLMENT_RETRY / FULFILLMENT_SCHEDULE_TO_CLOSE give the Kafka
                    # publish a 30-minute total budget (vs. the 5-minute STANDARD budget)
                    # so a brief cluster failover does not permanently fail the order.
                    schedule_to_close_timeout=FULFILLMENT_SCHEDULE_TO_CLOSE,
                    heartbeat_timeout=HEARTBEAT_TIMEOUT,
                    retry_policy=FULFILLMENT_RETRY,
                )
            except ActivityError:
                await self._update_status(OrderStatus.FULFILLMENT_FAILED)
                await self._drain_handlers()
                raise
            await self._update_status(OrderStatus.FULFILLED)
            await self._drain_handlers()

        except asyncio.CancelledError:
            await asyncio.shield(self._cleanup_on_cancel())
            raise

    # ── Updates ────────────────────────────────────────────────────────────────

    @workflow.update
    def handle_correction(self, corrected_input: OrderInput) -> None:
        """Accept a corrected order from the support team.

        Using Update (not Signal) so the caller gets synchronous accept/reject
        feedback. The validator runs *before* the event is written to history,
        so an invalid correction never appears in the audit trail.
        """
        self._state.current_order = corrected_input
        self._state.correction_received = True

    @handle_correction.validator
    def validate_handle_correction(self, corrected_input: OrderInput) -> None:
        if self._state.current_status != OrderStatus.PENDING_CORRECTION:
            raise ApplicationError(
                f"Corrections only accepted in PENDING_CORRECTION; "
                f"current status: {self._state.current_status}",
                type=ERR_INVALID_ORDER,
                non_retryable=True,
            )
        if not corrected_input.items:
            raise ApplicationError(
                "Corrected order must contain at least one item",
                type=ERR_INVALID_ORDER,
                non_retryable=True,
            )

    @workflow.update
    def capture_payment(self, payment: PaymentInput) -> None:
        """Record an incoming payment from the Payment Processor webhook.

        Using Update (not Signal) so the webhook caller learns immediately if
        the capture was rejected (already captured, order cancelled) rather than
        receiving a silent no-op. The validator guards the event history:
        rejected captures are never recorded.
        """
        self._state.captured_payment = payment

    @capture_payment.validator
    def validate_capture_payment(self, payment: PaymentInput) -> None:
        # Check the most specific business rejections first so the Payment
        # Processor gets actionable error types:
        #   0. Blank RRN               → ERR_INVALID_RRN  (OMS-F002: reject here,
        #      before history write, rather than letting validate_payment_rrn raise
        #      a non-retryable ActivityError that escapes Phase 4 uncaught and
        #      crashes the workflow without writing a terminal status.)
        #   1. Order cancelled         → ERR_ORDER_CANCELLED
        #   2. Payment already exists  → ERR_DUPLICATE_PAYMENT (catches re-sends
        #      during VALIDATING_PAYMENT where current_status would fail the
        #      phase guard below)
        #   3. Wrong workflow phase    → ERR_PAYMENT_NOT_ACCEPTED (OMS-020: guards
        #      against orphaned payments accepted during RISK_REJECTED,
        #      CORRECTION_TIMEOUT, PAYMENT_EXPIRED terminal drain windows)
        if not payment.rrn or not payment.rrn.strip():
            raise ApplicationError(
                "RRN must not be blank",
                type=ERR_INVALID_RRN,
                non_retryable=True,
            )
        if self._state.is_cancelled:
            raise ApplicationError(
                "Cannot capture payment: order has been cancelled",
                type=ERR_ORDER_CANCELLED,
                non_retryable=True,
            )
        if self._state.captured_payment is not None:
            raise ApplicationError(
                "Payment already captured for this order",
                type=ERR_DUPLICATE_PAYMENT,
                non_retryable=True,
            )
        if self._state.current_status not in (OrderStatus.AWAITING_PAYMENT, OrderStatus.PAYMENT_INVALID):
            raise ApplicationError(
                f"Payment capture only accepted in AWAITING_PAYMENT or PAYMENT_INVALID; "
                f"current status: {self._state.current_status}",
                type=ERR_PAYMENT_NOT_ACCEPTED,
                non_retryable=True,
            )

    # ── Signal ─────────────────────────────────────────────────────────────────

    @workflow.signal
    def cancel_order(self) -> None:
        """Fire-and-forget cancellation request.

        Remains a Signal (not Update) because the caller needs no confirmation:
        the status transitions to CANCELLED on the next workflow task. The
        idempotency guard ensures that once payment is captured the order is
        committed to fulfillment and further cancel signals are no-ops.
        """
        if self._state.captured_payment is None:
            self._state.is_cancelled = True
