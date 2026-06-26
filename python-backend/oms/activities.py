"""Activity implementations — the side-effecting work the workflow orchestrates.

Each method is a stub for what would be a real network call in production
(Commerce API, Payment Processor API, PIM, dashboard store, Kafka). Activities
are defined as instance methods so the workflow can reference them via
``workflow.execute_activity_method`` and tests can swap in a recording double
that exposes the same activity names.

PRODUCTION I/O RULES (OMS-F005)
--------------------------------
All activities are ``async def``.  This is correct **only** when every I/O call
inside them is async-safe (i.e. never blocks the event loop thread):

  - HTTP calls   → use ``aiohttp.ClientSession``, NOT ``requests``
  - Kafka        → use ``aiokafka.AIOKafkaProducer``, NOT ``confluent_kafka``
  - Database     → use ``asyncpg`` / ``aiomysql``, NOT ``psycopg2`` / ``pymysql``
  - AWS          → use ``aiobotocore``, NOT ``boto3``

A blocking call inside an ``async def`` activity freezes the entire worker
event loop, silently serialising all concurrency and potentially deadlocking
heartbeat threads.  If you need a blocking library, change the activity to a
plain ``def`` instead of ``async def`` — Temporal runs synchronous activities
in a thread-pool executor automatically.

CONNECTION POOLING (OMS-F008)
------------------------------
Clients are injected once at worker startup via ``OmsActivities.__init__`` and
reused across every activity invocation.  **Never** open a new session or
connection inside an activity body — doing so incurs TCP + TLS handshake
overhead on every call and risks file-descriptor exhaustion under load.

  Example (worker.py)::

      async with aiohttp.ClientSession() as session:
          activities = OmsActivities(http_session=session)
          worker = Worker(client, activities=[activities.validate_order_api, ...])
"""

import hashlib
import json

from temporalio import activity
from temporalio.exceptions import ApplicationError

from oms.models import (
    DashboardUpdate,
    EnrichedOrder,
    EnrichmentInput,
    ERR_INVALID_RRN,
    OrderInput,
    OrderItem,
    PaymentValidationInput,
)


def fulfillment_message(order: EnrichedOrder) -> dict:
    return {
        "customer_id": order.customer_id,
        "order_id": order.order_id,
        "payment_details": {"rrn": order.rrn},
        "items": [
            {"item_id": i.item_id, "sku_id": i.sku_id, "brand_code": i.brand_code}
            for i in order.items
        ],
    }


class OmsActivities:
    def __init__(
        self,
        http_session=None,    # aiohttp.ClientSession | None  (Commerce API, Payment API, PIM)
        kafka_producer=None,  # aiokafka.AIOKafkaProducer | None
        db_pool=None,         # asyncpg.Pool | None  (dashboard store)
    ) -> None:
        """Inject pre-constructed async clients at worker startup.

        All three default to ``None`` in the POC (stubs handle everything in
        memory).  In production, create long-lived client objects once in
        ``worker.main()`` and pass them here so they are reused across every
        activity invocation rather than re-opened per call.

        Args:
            http_session:   Shared ``aiohttp.ClientSession`` for Commerce API,
                            Payment Processor, and PIM calls.
            kafka_producer: Shared ``AIOKafkaProducer`` for order-fulfillment
                            publishes.
            db_pool:        Connection pool for the customer dashboard store
                            (e.g. ``asyncpg.Pool`` for PostgreSQL).
        """
        self._http = http_session
        self._kafka = kafka_producer
        self._db = db_pool

    @activity.defn
    async def validate_order_api(self, order: OrderInput) -> bool:
        # In production: HTTP call to Commerce API (worker capped at 150 RPS via COMMERCE_QUEUE)
        activity.logger.info("[Commerce API] Validating order: %s", order.order_id)
        activity.heartbeat("validating", order.order_id)
        return bool(order.items)

    @activity.defn
    async def assess_order_risk(self, order: OrderInput) -> bool:
        # vNext (gated by workflow.patched in the workflow): collect + validate
        # risk for an order. Returns True when the order is low-risk and may proceed.
        # In production: call an ML fraud/risk scoring service with order + customer data.
        activity.logger.info("[Risk] Assessing order: %s", order.order_id)
        activity.heartbeat("assessing", order.order_id)
        total_qty = sum(i.quantity for i in order.items)
        # Mock heuristic: abnormally large bulk orders are treated as high-risk.
        return total_qty < 1000

    @activity.defn
    async def validate_payment_rrn(self, input: PaymentValidationInput) -> bool:
        # OMS-002: idempotency key is stable across Temporal retries (same
        # workflow_run_id + activity_id) but unique per workflow execution.
        # In production: pass as the Idempotency-Key header to the Payment
        # Processor so a network-timeout retry cannot double-process a charge.
        info = activity.info()
        idempotency_key = f"{info.workflow_run_id}-{info.activity_id}"
        activity.logger.info(
            "[Payment API] Validating RRN: %s (idempotency_key=%s)",
            input.rrn,
            idempotency_key,
        )
        activity.heartbeat("validating_rrn", input.rrn)
        if input.rrn is None or not input.rrn.strip():
            # Non-retryable: blank RRN is a programming error, not a transient failure
            raise ApplicationError(
                "RRN must not be blank", type=ERR_INVALID_RRN, non_retryable=True
            )
        # In production: PaymentProcessorAPI.validate(
        #     input.rrn, idempotency_key=idempotency_key
        # )
        # Simulate: any RRN prefixed with "INVALID-" is not found in the payment system
        return not input.rrn.startswith("INVALID-")

    @activity.defn
    async def enrich_with_pim(self, input: EnrichmentInput) -> EnrichedOrder:
        # OMS-012: typed EnrichmentInput instead of raw (order: OrderInput, rrn: str).
        # In production: call PIM API with each item's item_id to resolve SKU and Brand Code.
        activity.logger.info("[PIM] Enriching order: %s", input.order.order_id)
        enriched_items = []
        for item in input.order.items:
            # OMS-015: heartbeat is sent AFTER the per-item work so that the
            # heartbeat timeout is not violated by the I/O call itself.
            # In production: result = PimAPI.fetch(item.item_id)
            enriched_items.append(
                OrderItem(
                    item_id=item.item_id,
                    quantity=item.quantity,
                    sku_id=f"SKU-{hashlib.md5(item.item_id.encode()).hexdigest()[:8]}",
                    brand_code=f"BRAND-{item.item_id[:3].upper()}",
                )
            )
            activity.heartbeat("enriching", item.item_id)
        return EnrichedOrder(
            customer_id=input.order.customer_id,
            order_id=input.order.order_id,
            rrn=input.rrn,
            items=enriched_items,
        )

    @activity.defn
    async def update_customer_dashboard(self, update: DashboardUpdate) -> None:
        # OMS-007: typed DashboardUpdate input instead of raw (order_id, status) primitives.
        # In production: upsert to denormalized DB (e.g., DynamoDB or Redis) keyed by
        # order_id. Upsert semantics make this activity naturally idempotent on retry.
        activity.logger.info("[Dashboard] Order %s -> %s", update.order_id, update.status)
        activity.heartbeat("updating_dashboard", update.order_id, update.status)

    @activity.defn
    async def publish_to_fulfillment_kafka(self, order: EnrichedOrder) -> None:
        # Build the exact order-fulfillment message contract the downstream
        # allocation step consumes (per the assessment spec): customer_id,
        # order_id, payment_details.rrn, and items carrying only the enriched
        # identifiers (item_id, sku_id, brand_code) — no quantity.
        #
        # OMS-002: idempotency key is stable across Temporal retries (same
        # workflow_run_id + activity_id) but unique per workflow execution.
        # In production: pass as a Kafka message header so the consumer can
        # deduplicate retried publishes and avoid double-fulfillment.
        info = activity.info()
        idempotency_key = f"{info.workflow_run_id}-{info.activity_id}"
        message = fulfillment_message(order)
        activity.heartbeat("publishing", order.order_id)
        # In production: KafkaProducer.send(
        #     "order-fulfillment",
        #     key=order.order_id,
        #     value=message,
        #     headers={"Idempotency-Key": idempotency_key},
        # )
        activity.logger.info(
            "[Kafka] Published to 'order-fulfillment' (idempotency_key=%s): %s",
            idempotency_key,
            json.dumps(message),
        )
