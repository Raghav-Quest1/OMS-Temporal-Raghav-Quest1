# Order Management System (OMS) — Temporal POC (Python SDK)

Durable order lifecycle orchestration: Commerce API validation, support-correction loop, 30-day payment window, RRN verification, PIM enrichment, and Kafka fulfillment hand-off — all in a single Temporal workflow.

---

## Quick start

```bash
cd OrderProcessingApplication-POC
docker compose up --build
```

| Service | URL |
|---|---|
| Operator portal | http://localhost:80 |
| Temporal UI | http://localhost:8080 |
| API | http://localhost:3000 |

Temporal takes ~20–30s to boot on first run; the API and worker self-heal via `restart: on-failure`.

---

## Architecture

```
 Operator Portal (Nginx :80)
       │ HTTP (:3000)
       ▼
 API server (aiohttp)  ──Temporal client──►  Temporal Server (:7233)  ◄──►  PostgreSQL
                                                      │
                              ┌───────────────────────┴──────────────────────┐
                        OMS_QUEUE worker                           COMMERCE_QUEUE worker
                        workflow + all activities                  validate_order_api only
                                                                   (150 RPS cap)
```

---

## Order lifecycle

```
INITIALIZING
  │ validate_order_api (COMMERCE_QUEUE, 150 RPS)
  ├─ invalid ──► PENDING_CORRECTION ──(handle_correction / 30-day timeout)──► CORRECTION_TIMEOUT
  └─ valid
       │ workflow.patched("risk-assessment-v1")
       ├─ AWAITING_RISK_ASSESSMENT ──► RISK_REJECTED (terminal)
       └─ AWAITING_PAYMENT ── 30-day timer ──► EXPIRED ──► CANCELLED (terminal)
            │ cancel_order (before capture) ──► CANCELLED (terminal)
            └─ capture_payment
                 │ VALIDATING_PAYMENT
                 ├─ invalid RRN ──► PAYMENT_INVALID ──(new capture / 7-day timeout)──► PAYMENT_EXPIRED
                 └─ valid RRN
                      │ ENRICHING
                      ├─ activity failure ──► FULFILLMENT_FAILED (terminal)
                      └─ FULFILLED (terminal)
```

Every transition writes to `update_customer_dashboard`. External Temporal cancellation writes `CANCELLED` via `asyncio.shield`.

---

## Temporal primitives

| Primitive | Purpose |
|---|---|
| `@workflow.update` + validator | `capture_payment`, `handle_correction` — synchronous accept/reject before history write |
| `@workflow.signal` | `cancel_order` — fire-and-forget |
| `@workflow.query` | `get_order_status` / `get_order_state` — live state reads |
| `wait_condition(pred, timeout=...)` | 30-day payment window; 7-day payment-retry window; 30-day correction window |
| `workflow.all_handlers_finished()` | Handler drain before every exit (prevents TMPRL1102) |
| `workflow.patched("risk-assessment-v1")` | Safe in-place versioning for the vNext risk gate |
| `workflow_id = order.order_id` + `USE_EXISTING` | Exactly-once workflow per order; duplicate webhooks are idempotent |
| Dual task queues | Commerce API rate-limited to 150 RPS without throttling PIM/Kafka/dashboard |
| `RetryPolicy` + `schedule_to_close_timeout` | Exponential backoff bounded by a 2-minute total budget per activity |
| `asyncio.CancelledError` + `asyncio.shield` | Temporal cancellation handler writes terminal status and drains handlers |
| `WorkerDeploymentConfig(PINNED)` | Each execution stays on the Worker version it started; safe rolling deploys |

---

## Requirements coverage

| Requirement | Implementation |
|---|---|
| Out-of-order / missing inputs | Updates + Signal + `wait_condition` gates |
| 30-day TTL | `wait_condition(timeout=timedelta(days=30))` → `EXPIRED` → `CANCELLED` |
| Order validation + correction loop | `validate_order_api` on COMMERCE_QUEUE + `handle_correction` Update |
| PIM enrichment (SKU + Brand Code) | `enrich_with_pim` → `sku_id` / `brand_code` in Kafka message |
| Payment capture 1–30 days; failure → cancel | Payment vs cancel vs timer race in Phase 2 |
| Cancellation before capture only | `cancel_order` Signal guarded by `captured_payment` |
| RRN validation + retry | `validate_payment_rrn` + `PAYMENT_INVALID` retry loop (7-day window) |
| Idempotent duplicate webhooks | `order.order_id` as workflow ID + `USE_EXISTING` |
| Commerce API 150 RPS cap | `COMMERCE_QUEUE` worker with `max_activities_per_second=150` |
| Customer dashboard | `update_customer_dashboard` on every status transition |
| Kafka fulfillment contract | `publish_to_fulfillment_kafka` emits `{customer_id, order_id, payment_details.rrn, items[{item_id, sku_id, brand_code}]}` |
| vNext risk gate (implemented) | `workflow.patched("risk-assessment-v1")` before payment window |
| PII protection (design) | Custom `DataConverter` (encrypt before leaving worker) + Codec Server (decrypt for UI) |

---

## API endpoints

| Method | Path | Action |
|---|---|---|
| `POST` | `/api/orders` | Start workflow; `order.order_id` becomes the workflow ID (UUID fallback) |
| `POST` | `/api/orders/correct?orderId=X` | Send `handle_correction` Update |
| `POST` | `/api/payments` | Send `capture_payment` Update; `metadata.order_id` routes to the right workflow |
| `POST` | `/api/orders/cancel?orderId=X` | Send `cancel_order` Signal |
| `GET` | `/api/orders/status?orderId=X` | Query live status |

---

## Tests

```bash
cd python-backend
pip install -r requirements-dev.txt
pytest          # → 26 passed
```

26 tests across two files: 19 workflow/activity scenarios (time-skipping `WorkflowEnvironment`, recording activities double) and 7 API webhook tests (in-process fake Temporal client).

| File | Count | Covers |
|---|---|---|
| `test_order_processing.py` | 19 | Happy path, TTL, cancellation, corrections, RRN retry, risk gate, Kafka contract, PAYMENT_EXPIRED, FULFILLMENT_FAILED, CORRECTION_TIMEOUT |
| `test_api.py` | 7 | Webhook parsing, idempotent start, Update routing, validator rejections, status query |

---

## Local development

```bash
# Infra only
docker compose up -d postgres temporal temporal-ui

# Worker (terminal 1)
cd python-backend && python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt && python -m oms.worker

# API (terminal 2)
cd python-backend && source .venv/bin/activate && python -m oms.api
```

Portal defaults to `http://localhost:3000/api`; change under **Settings → Connection**.
