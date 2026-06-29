# REQ-12 — PII Protection Plan: Customer Email Addresses

This document describes the design for protecting customer email addresses (and other PII fields introduced in future releases) in the OMS Temporal workflow. It is a forward-looking plan — the current POC does not yet collect email addresses, but the design must be established before any such field is added to `OrderInput` or `WorkflowState`.

---

## 1. Scope — which fields are PII

| Field | Model | PII class | Treatment |
|---|---|---|---|
| `customer_email` (future) | `OrderInput`, `WorkflowState.current_order` | Direct identifier | Encrypt at rest in history |
| `customer_id` | `OrderInput`, `EnrichedOrder` | Pseudonymous | No encryption required (opaque ID, not linkable without external table) |
| `rrn` | `PaymentInput`, `EnrichedOrder` | Financial reference | No encryption required (already masked in logs; not a personal identifier) |
| Item identifiers (`item_id`, `sku_id`, `brand_code`) | `OrderItem` | Non-personal | Plaintext |

When `customer_email` is added to `OrderInput`, it must be declared as a PII field in this table and the Codec Server configured before the field goes to production.

---

## 2. Encryption approach — Custom DataConverter + Codec Server

Temporal's data model serialises every activity input/output, signal payload, update payload, and workflow state snapshot into the workflow event history stored in the Temporal Server's PostgreSQL database. Because the OMS Temporal Server is self-hosted (see `docker-compose.yml`), the history is under our control, but the Temporal UI and CLI can read it in plaintext without any additional protection.

The defence-in-depth strategy has two layers:

```
Worker / API server                 Temporal Server (Postgres)
┌──────────────────┐                ┌──────────────────────────────┐
│  Custom          │  encrypted     │  Event history stored as      │
│  DataConverter   │─────────────►  │  opaque base64 ciphertext.    │
│  (encrypt on     │  payloads      │  Temporal UI shows ████████.  │
│   serialize,     │                └──────────────────────────────┘
│   decrypt on     │
│   deserialize)   │◄────────────── Codec Server (HTTP sidecar)
└──────────────────┘  decrypt req   ┌──────────────────────────────┐
                                    │  Only authorised callers       │
                                    │  (Temporal UI, tctl) can       │
                                    │  request decryption.           │
                                    └──────────────────────────────┘
```

### 2.1 Encryption algorithm

**AES-256-GCM** with a random 96-bit nonce per encrypted value.

- AES-256-GCM is authenticated encryption — it provides both confidentiality and integrity (AEAD). A tampered ciphertext will fail to decrypt rather than silently producing garbage.
- The 96-bit nonce is prepended to the ciphertext blob so it travels with the data.
- The resulting binary blob is base64-encoded and stored as a Temporal `Payload` with encoding metadata `"binary/encrypted"`.

Wire format per encrypted `Payload`:

```
[ 12 bytes nonce ][ N bytes AES-256-GCM ciphertext+tag ]
```

Stored as:

```json
{
  "metadata": { "encoding": "YmluYXJ5L2VuY3J5cHRlZA==" },
  "data": "<base64(nonce || ciphertext)>"
}
```

### 2.2 What gets encrypted — field-level vs payload-level

Temporal's `DataConverter` operates at the **payload** level (one payload per activity argument, signal argument, etc.), not at the JSON field level. Two options:

| Option | Granularity | Trade-off |
|---|---|---|
| **Encrypt the whole payload** | All fields in `OrderInput` are opaque | Simpler implementation; `customer_id` and `order_id` are also encrypted (no operational harm since they're accessed via workflow ID, not by reading history) |
| **Field-level encryption** | Only `customer_email` bytes are encrypted; other fields remain plaintext | More complex (requires a custom JSON transform step); allows tctl/UI to read non-PII fields without Codec Server |

**Decision**: encrypt the whole payload for `OrderInput` and `WorkflowState`. The operational cost is minimal — the Temporal UI's Codec Server integration restores readability for authorised operators, and there is no legitimate need to read raw history JSON for non-PII fields. Field-level encryption adds complexity without meaningful benefit at this system's scale.

Payloads that do **not** contain PII (`OrderItem`, `EnrichedOrder` post-PIM, `DashboardUpdate`, `PaymentValidationInput`) are left as plaintext JSON so activity outputs remain observable without Codec Server access.

---

## 3. Key management

### 3.1 Key hierarchy

```
Root key (KMS-managed, never leaves KMS)
    └── Data Encryption Key (DEK, AES-256, rotated monthly)
            └── Per-payload encrypted value
```

- The **Root key** lives in AWS KMS (or equivalent). It is used only to encrypt/decrypt the DEK.
- The **DEK** is a 256-bit AES key. Workers fetch the current DEK from a secrets store (e.g. AWS Secrets Manager / HashiCorp Vault) at startup and cache it in memory. The DEK is never written to disk or committed to source control.
- When a DEK is rotated, the old DEK is retained in the secrets store under a versioned key name (e.g. `oms/dek/v3`) so that existing encrypted history entries can still be decrypted during replay. Workers load all active DEK versions at startup.

### 3.2 Key identifier in the payload

Each encrypted `Payload` carries a `key_id` metadata field so the decrypting worker or Codec Server knows which DEK version to use:

```json
{
  "metadata": {
    "encoding": "YmluYXJ5L2VuY3J5cHRlZA==",
    "key_id": "b3JkZXItZGVrLXYz"
  },
  "data": "<base64(nonce || ciphertext)>"
}
```

### 3.3 Key rotation procedure

1. Generate a new DEK in the secrets store (`oms/dek/v<N+1>`).
2. Roll out new worker image with the updated DEK version list. Workers will encrypt new payloads with `v<N+1>` and can decrypt payloads encrypted with any prior version.
3. Old DEK versions are retained for the maximum workflow execution lifetime (90 days per `execution_timeout` in `api.py`). After 90 days from the last workflow that used `v<N>`, `v<N>` may be archived.
4. Re-encryption of historical payloads is not required — old executions are immutable once closed.

### 3.4 Local development

In the local Docker Compose environment, the DEK is injected via an environment variable (`OMS_DEK_B64`) rather than fetched from KMS. This variable is set in `.env` (git-ignored) and never committed to the repo. The `docker-compose.yml` references it as:

```yaml
environment:
  OMS_DEK_B64: ${OMS_DEK_B64}
```

---

## 4. Codec Server deployment topology

The Codec Server is an HTTP service that the Temporal UI and `tctl` use to decode encrypted payloads for authorised operators. It must **never** be publicly reachable.

```
                        ┌─────────────────────────────────────────┐
                        │  Internal network (VPC / Docker network) │
                        │                                          │
  Operator browser ─────┼──► Temporal UI (:8080)                  │
                        │         │  POST /decode (Codec Server URL)│
                        │         ▼                                │
                        │    Codec Server (:8888)                  │
                        │         │  fetch DEK from Secrets Manager│
                        │         │  verify mTLS / OIDC token       │
                        │         └── decrypt payload, return JSON  │
                        │                                          │
                        └─────────────────────────────────────────┘
```

### 4.1 Codec Server responsibilities

- Accept `POST /decode` requests from the Temporal UI (standard Temporal Codec Server API).
- Verify the caller identity via one of:
  - **mTLS**: client certificate issued to the Temporal UI pod.
  - **OIDC bearer token**: short-lived token issued by an internal identity provider, checked against an allowlist of operator email domains.
- Fetch the DEK version named in `key_id` from the secrets store (cached in memory with a 5-minute TTL).
- Decrypt the payload and return the plaintext JSON.
- Emit an audit log entry for every decode request: `{timestamp, operator_identity, workflow_id, payload_type}`.

### 4.2 Docker Compose (local)

A local Codec Server container is added alongside the existing services:

```yaml
codec-server:
  build: ./codec-server
  environment:
    OMS_DEK_B64: ${OMS_DEK_B64}
  ports: []                          # not exposed on the host
  networks:
    - temporal-network
```

The Temporal UI is configured to point at `http://codec-server:8888` (internal DNS only). No host port binding means it is unreachable from outside the Docker network.

### 4.3 Production (Kubernetes)

| Component | Deployment |
|---|---|
| Codec Server | Single `Deployment` (2 replicas), internal `ClusterIP` service only — no `Ingress` or `LoadBalancer` |
| Temporal UI | Configured with `TEMPORAL_CODEC_ENDPOINT=http://codec-server.temporal.svc.cluster.local:8888` |
| Secrets | DEK versions injected via Kubernetes `Secret` (sourced from AWS Secrets Manager via External Secrets Operator) |
| Network policy | `NetworkPolicy` restricts Codec Server ingress to pods labelled `app=temporal-ui` only |

---

## 5. WorkflowState schema evolution for encrypted fields

Adding `customer_email` to `OrderInput` (and by extension `WorkflowState.current_order`) is a **history-breaking change** if done naively — open executions replaying against the new code will fail to deserialise the old payload that lacks `customer_email`.

### 5.1 Safe addition procedure

Follow the same principle as `workflow.patched`: deploy in stages so no running execution ever encounters a schema it wasn't started under.

**Step A — Add the field as Optional with a default**

```python
@dataclass
class OrderInput:
    customer_id: str
    order_id: str
    items: list[OrderItem] = field(default_factory=list)
    customer_email: str | None = None          # NEW — optional, defaults to None
```

Using `None` as the default means the existing JSON payloads in history (which lack the key entirely) deserialise successfully — `dataclasses` will fill the missing key with `None`. This is backward-compatible.

**Step B — Deploy the new worker image (rolling restart)**

All workers can now handle both old payloads (no `customer_email` key) and new payloads (with `customer_email`). Open executions continue without issues.

**Step C — Enable encryption for the field**

Once Step B is stable and all workers are running the new code, update the `DataConverter` to treat any `OrderInput` payload as PII (i.e. encrypt the whole payload as described in §2.2). New workflow executions will store encrypted history. Old open executions will have a mix of plaintext (pre-Step-C history events) and encrypted (post-Step-C events) — the `DataConverter` handles this by checking the `encoding` metadata field and falling back to plaintext deserialisation when `encoding != "binary/encrypted"`.

**Step D — Mark the field as required (future, after all pre-Step-C executions close)**

Once no open execution predates Step C, `customer_email` can be made non-optional and callers that omit it will receive a validation error at the API layer.

### 5.2 Replay fixture regeneration

Any fixture in `tests/fixtures/*.json` recorded before Step A must be regenerated after Step A is deployed, following the same procedure documented in the [Replay Fixtures section of the README](README.md#replay-fixtures). The replay test suite (`test_replay.py`) must pass against the new worker code before Step B is deployed.

### 5.3 Schema change checklist

- [ ] New PII field added to the scope table in §1 of this document
- [ ] Field added as `Optional` with `None` default (Step A)
- [ ] Replay fixtures regenerated and committed
- [ ] All `test_replay.py` tests pass
- [ ] DEK version confirmed available in secrets store
- [ ] `DataConverter` updated to encrypt the containing payload type
- [ ] Codec Server deployed and reachable from Temporal UI before worker rollout
- [ ] Audit log pipeline confirmed active

---

## 6. Implementation reference points

| Component | File | Notes |
|---|---|---|
| `OrderInput` / `WorkflowState` | `python-backend/oms/models.py` | Add `customer_email: str \| None = None` here |
| Worker startup | `python-backend/oms/worker.py` | Pass `data_converter=OmsDataConverter()` to `Client.connect()` |
| Custom DataConverter | `python-backend/oms/codec.py` (to be created) | Implement `PayloadCodec` from `temporalio.converter` |
| Codec Server | `codec-server/` (to be created) | Standalone ASGI app; reuses the same `PayloadCodec` logic |
| DEK loading | `python-backend/oms/secrets.py` (to be created) | Load from env var locally, from Secrets Manager in production |
