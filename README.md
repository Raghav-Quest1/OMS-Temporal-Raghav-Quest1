# Order Management System (OMS) — Temporal POC (Python SDK)

Durable order lifecycle orchestration: Commerce API validation, support-correction loop, 30-day payment window, RRN verification, PIM enrichment, and Kafka fulfillment hand-off — all in a single Temporal workflow.

---

## Quick start

```bash
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
                        except validate_order_api                  (150 RPS cap)
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
| `RetryPolicy` + `schedule_to_close_timeout` | Exponential backoff bounded by a 5-minute total budget per activity |
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
| vNext risk gate (implemented) | `workflow.patched("risk-assessment-v1")` before payment window — see [Risk Gate Deployment Runbook](#risk-gate-deployment-runbook) |
| PII protection (design) | Custom `DataConverter` (AES-256-GCM, KMS envelope) + Codec Server — see [PII Protection Design](#pii-protection-design) |

---

## PII Protection Design

> **Status: design only — not yet implemented.** The implementation milestone is tracked separately. This section documents the agreed design so that the encryption algorithm, key management strategy, field scope, Codec Server topology, and schema evolution rules are all in one place before any code is written.

### Encryption algorithm

All PII-bearing payloads are encrypted with **AES-256-GCM** before they leave the worker process:

- 256-bit symmetric key derived via envelope encryption (see Key management below)
- 96-bit IV generated randomly per encryption call using `os.urandom(12)`
- 128-bit authentication tag appended to the ciphertext — provides both confidentiality and tamper detection
- Encoded payload format stored in Temporal history: `base64(iv || ciphertext || tag)` with a `"encoding": "binary/encrypted"` metadata header so the Codec Server can identify encrypted payloads

### Key management

Envelope encryption via **AWS KMS** (or HashiCorp Vault Transit as an alternative):

1. At worker startup, request a **data key** from KMS (`GenerateDataKey`). KMS returns a plaintext copy (used to encrypt) and a ciphertext copy (stored alongside the payload).
2. The plaintext data key is held in memory only for the duration of the encryption call and then zeroed.
3. The encrypted data key is stored in the payload metadata so the Codec Server can call `KMS.Decrypt` to recover it at decode time.
4. **Key rotation**: KMS key aliases are versioned. New executions automatically use the current alias. Old ciphertext in immutable history events continues to decrypt via the old key version — no re-encryption of historical events is required or attempted.
5. **Access control**:
   - Workers hold an IAM role with `kms:GenerateDataKey` + `kms:Decrypt` on the OMS key ARN.
   - The Codec Server holds an IAM role with `kms:Decrypt` only.
   - No other service has KMS access.

### Field-level scope

Temporal's `DataConverter` operates at the **payload level** (one payload = one activity input, one activity output, one workflow input, one signal/update argument, or one query result). The following payloads contain PII and will be encrypted:

| Payload | PII fields |
|---|---|
| Workflow input (`OrderInput`) | `customer_id` (and future `customer_email`) |
| `assess_order_risk` input (`OrderInput`) | `customer_id` |
| `update_customer_dashboard` input (`DashboardUpdate`) | `order_id` (indirect identifier) |
| `get_order_state` query result (`WorkflowState`) | `current_order.customer_id` |
| `publish_to_fulfillment_kafka` input (`EnrichedOrder`) | `customer_id` |

Payloads that contain only non-PII fields (e.g. `PaymentValidationInput` carrying only `rrn`, `EnrichmentInput` carrying only order metadata) are **not** encrypted to avoid unnecessary KMS calls. The `DataConverter` will check a per-type opt-in annotation to decide whether to encrypt.

### Codec Server deployment topology

```
  ┌─────────────────────────────────────────────────────────────────┐
  │  Private network                                                │
  │                                                                 │
  │  Worker (OMS_QUEUE / COMMERCE_QUEUE)                            │
  │    │  encrypt(payload) via custom DataConverter                 │
  │    ▼                                                            │
  │  Temporal Server (:7233) ◄──► PostgreSQL  (stores ciphertext)  │
  │                  │                                              │
  │                  │  Temporal UI / tctl                          │
  │                  │    ──decode request──►  Codec Server (:8888) │
  │                  │    ◄─decoded payload──  (SSO-gated)          │
  └─────────────────────────────────────────────────────────────────┘
```

Key topology constraints:

- The Codec Server is a separate service, **not** co-located with the worker.
- It is not reachable from the public internet; it sits inside the private VPC.
- Authentication: every decode request must carry a valid SSO/OAuth bearer token. The Codec Server validates the token against the IdP before calling KMS. This ensures only authorised humans (developers, support) can trigger decryption — automated systems cannot.
- The Codec Server URL is registered in the Temporal UI namespace settings (`Settings → Codec Server`) so the UI can make browser-side decode calls for workflow history inspection.
- The Codec Server is stateless and can be horizontally scaled behind an internal load balancer.

### WorkflowState schema evolution for encrypted fields

`WorkflowState` is serialised into workflow history on every query response and must evolve safely:

1. **Adding a new PII field** (e.g. `customer_email: str`): always provide a default value (`field(default="")`) so that in-flight executions replaying history written before the field existed do not raise a deserialization error.
2. **Plaintext → encrypted migration**: payloads written before the `DataConverter` was deployed are stored as plaintext. The Codec Server must detect unencrypted payloads (by checking for the `"encoding": "binary/encrypted"` metadata header) and pass them through unchanged rather than attempting to decrypt.
3. **Encrypted data key rotation**: old history events are immutable — their ciphertext is never re-encrypted. The Codec Server resolves the correct KMS key version from the encrypted data key stored in the payload metadata, so decryption always succeeds regardless of when the event was written.
4. **Removing a PII field**: mark it as `field(default=None)` and type it `Optional[T]` for at least one full release cycle before deleting it, so executions with the old field in their history can still replay.

---

## Risk Gate Deployment Runbook

The `workflow.patched("risk-assessment-v1")` call at `workflows.py:293` is currently in **Step 1** of a three-step lifecycle. This runbook documents the full sequencing, how to confirm safety before advancing each step, and how to coordinate with a rolling deploy.

### Background: why three steps?

`workflow.patched` is Temporal's safe in-place versioning primitive (equivalent to `Workflow.getVersion` in the Java SDK). It lets a single worker binary serve both old and new executions simultaneously:

- Executions that **started before** the Step 1 deploy replay the `else` branch (no risk gate).
- Executions that **started after** the Step 1 deploy replay the `if` branch (risk gate runs).

Skipping directly to Step 3 (unconditional risk gate) while any pre-Step-1 execution is still open would cause a **non-determinism error** on replay and permanently break those executions.

### Step 1 — Deploy the patch (current state)

**Code**: `if workflow.patched(RISK_PATCH): ... # else: skip risk gate`

**Deploy procedure**:
1. Build and push the new worker image.
2. Perform a rolling restart of all worker pods (OMS_QUEUE). Because `WorkerDeploymentConfig(PINNED)` is in use, each in-flight execution stays pinned to the worker version it started on — the rolling restart does not affect open executions.
3. Verify all pods are running the new image before declaring Step 1 complete.

**When Step 1 is "done"**: all worker replicas are running the patched code. New orders now go through the risk gate; existing open orders continue without it.

### Step 2 — Deprecate the patch

**Code**: replace `workflow.patched(RISK_PATCH)` with `workflow.deprecate_patch(RISK_PATCH)`

**Pre-condition — confirming all pre-patch executions have completed**:

Before making this change you must verify that **zero open workflow executions started before the Step 1 deploy timestamp** remain in the namespace. Use the Temporal CLI:

```bash
# Replace <STEP1_DEPLOY_RFC3339> with the ISO-8601 timestamp of the Step 1 deploy,
# e.g. 2025-08-01T12:00:00Z
temporal workflow list \
  --query 'StartTime < "<STEP1_DEPLOY_RFC3339>" AND ExecutionStatus = "Running"' \
  --namespace default
```

When this query returns zero results, it is safe to proceed. If any executions are returned, wait for them to reach a terminal state (or handle them operationally) before continuing.

**Deploy procedure**: same rolling restart as Step 1. `workflow.deprecate_patch` records a marker in new history events to signal that the old path is gone; it is a no-op for any execution that never hit the patch.

### Step 3 — Remove the patch infrastructure

**Code**: delete `RISK_PATCH`, the `if workflow.patched(...)` block, and the `else` branch — making `assess_order_risk` unconditional.

**Pre-condition**: repeat the CLI query from Step 2 but filter for executions started before the **Step 2** deploy timestamp. When the count is zero, all live executions were started under Step 2 code and have never seen the `if/else` branch — removing it is safe.

**Before deploying, run the replay tests**:

```bash
cd python-backend
pytest tests/test_replay.py -v
```

The fixture-based replay tests load pre-recorded histories (including `risk_rejected`) and replay them against the current code. They must all pass before the Step 3 image is built. If any fixture was recorded under Step 1/2 code, regenerate it after the code change and commit the updated JSON alongside the PR.

### Rolling deploy coordination summary

| Step | Pre-condition | Action | Verify |
|---|---|---|---|
| 1 | All workers on old code | Rolling restart to patched image | All pods healthy; new orders enter risk gate |
| 2 | Zero open executions with `StartTime < step-1-deploy` | Rolling restart to deprecate-patch image | CLI query returns 0; no non-determinism alerts |
| 3 | Zero open executions with `StartTime < step-2-deploy` | Rolling restart to patch-free image; replay tests green | All replay tests pass; no non-determinism alerts |

---

## API endpoints

| Method | Path | Action |
|---|---|---|
| `POST` | `/api/orders` | Start workflow; `order.order_id` becomes the workflow ID (required — missing ID returns 400) |
| `POST` | `/api/orders/correct?orderId=X` | Send `handle_correction` Update |
| `POST` | `/api/payments` | Send `capture_payment` Update; `metadata.order_id` routes to the right workflow |
| `POST` | `/api/orders/cancel?orderId=X` | Send `cancel_order` Signal |
| `GET` | `/api/orders/status?orderId=X` | Query live status |

---

## Tests

```bash
cd python-backend
pip install -r requirements-dev.txt
pytest          # → 38 passed
```

38 tests across three files: 25 workflow/activity scenarios (time-skipping `WorkflowEnvironment`, recording activities double), 8 API webhook tests (in-process fake Temporal client), and 5 replay tests (saved event histories replayed against current workflow code).

| File | Count | Covers |
|---|---|---|
| `test_order_processing.py` | 25 | Happy path, TTL, cancellation, corrections, RRN retry, risk gate, Kafka contract, PAYMENT_EXPIRED, FULFILLMENT_FAILED, CORRECTION_TIMEOUT, phase guards, duplicate payment, status query, external Temporal cancellation |
| `test_api.py` | 8 | Webhook parsing, idempotent start, Update routing, validator rejections, 409 on duplicate order ID, status query |
| `test_replay.py` | 5 | Replay safety for happy path, risk rejected, correction timeout, payment expired, fulfillment failed |

---

## Replay fixtures

`test_replay.py` loads pre-recorded event histories from `tests/fixtures/*.json` and replays them against the current workflow code to catch non-determinism errors before they reach production.

The five fixture files (`happy_path`, `risk_rejected`, `correction_timeout`, `payment_expired`, `fulfillment_failed`) are committed to the repo. Regenerate them after any non-additive workflow code change:

```bash
cd python-backend
python tests/fixtures/generate_replay_fixtures.py
```

Then commit the updated JSON files alongside the code change.

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
