# JSON ↔ HL7 FHIR Integration System — Implementation Plan

**Stack:** Python 3.12 · FastAPI · Apache Kafka (aiokafka) · PostgreSQL · Docker / Kubernetes
**Goal:** Accept custom JSON from internal systems (ERP / CRM / Insurance), convert it to HL7 FHIR, exchange it with an external FHIR server, convert the response back to JSON, and update internal systems, all asynchronously and with a full audit trail.

> Note: the original architecture diagram lists TypeScript / Node.js. This plan replaces it with FastAPI (Python). Topics, services and flow stay exactly the same.

---
V
## 1. Architecture Summary

```
Internal System ──HTTP──► [1] Integration API (FastAPI)@app.post("/json/request", tags=["FHIR"])
                               │  validate · auth · log · audit
                               ▼
                        topic: json.request
                               ▼
                    [2] JSON→FHIR Transformer
                               ▼
                        topic: fhir.outgoing
                               ▼
                    [3] FHIR Communication Service ──HTTPS──► External FHIR Server
                               ▼  (response / error)
                        topic: fhir.incoming
                               ▼
                    [4] FHIR→JSON Transformer
                               ▼
                        topic: json.response
                               ▼
                    [5] Internal Processing Service
                        update DB · workflows · notifications
```

### Services

| # | Service | Type | Consumes | Produces |
|---|---------|------|----------|----------|
| 1 | `integration-api` | FastAPI (HTTP) | – | `json.request` |
| 2 | `json-to-fhir` | Kafka worker | `json.request` | `fhir.outgoing` |
| 3 | `fhir-comm` | Kafka worker | `fhir.outgoing` | `fhir.incoming` |
| 4 | `fhir-to-json` | Kafka worker | `fhir.incoming` | `json.response` |
| 5 | `internal-processing` | Kafka worker | `json.response` | – (DB + workflows) |

### Kafka Topics

| Topic | Partitions | Key | Purpose |
|-------|-----------|-----|---------|
| `json.request` | 6 | `patientId` | Internal JSON requests |
| `fhir.outgoing` | 6 | `patientId` | FHIR requests to external server |
| `fhir.incoming` | 6 | `patientId` | FHIR responses from external server |
| `json.response` | 6 | `patientId` | JSON responses for internal system |
| `*.retry` | 3 | same | Delayed retries |
| `*.dlq` | 3 | same | Dead-letter (poison messages) |

Keying by `patientId` keeps events for one patient ordered.

---

## 2. Tech Choices

| Concern | Choice |
|---------|--------|
| Web framework | FastAPI + Uvicorn/Gunicorn |
| Kafka client | `aiokafka` (async, fits FastAPI) |
| Validation | Pydantic v2 |
| FHIR models | `fhir.resources` (R4) |
| HTTP client | `httpx` (async) + `tenacity` for retries |
| DB | PostgreSQL 16 + SQLAlchemy 2 (async) + `asyncpg` + Alembic |
| Auth | OAuth2 / JWT (`python-jose`) or API keys |
| Config | `pydantic-settings` (.env) |
| Logging | `structlog` (JSON logs with `correlation_id`) |
| Observability | OpenTelemetry + Prometheus + Grafana |
| Tests | `pytest`, `pytest-asyncio`, `testcontainers` |
| Packaging | Docker, docker-compose (dev), Kubernetes (prod) |

---

## 3. Project Structure (monorepo)

```
fhir-integration/
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── libs/
│   └── common/                     # shared package
│       ├── config.py               # Settings
│       ├── logging.py
│       ├── kafka/
│       │   ├── producer.py
│       │   ├── consumer.py         # BaseConsumer with retry + DLQ
│       │   └── topics.py
│       ├── schemas/
│       │   ├── envelope.py         # Event envelope
│       │   └── patient.py          # Internal JSON models
│       └── db/
│           ├── base.py
│           └── models.py           # AuditLog, MessageTracking
├── services/
│   ├── integration_api/
│   │   ├── app/main.py
│   │   ├── app/routers/patients.py
│   │   ├── app/deps/auth.py
│   │   └── Dockerfile
│   ├── json_to_fhir/
│   │   ├── app/worker.py
│   │   ├── app/mappers/patient_mapper.py
│   │   └── Dockerfile
│   ├── fhir_comm/
│   │   ├── app/worker.py
│   │   ├── app/fhir_client.py
│   │   └── Dockerfile
│   ├── fhir_to_json/
│   │   ├── app/worker.py
│   │   ├── app/mappers/response_mapper.py
│   │   └── Dockerfile
│   └── internal_processing/
│       ├── app/worker.py
│       ├── app/handlers.py
│       └── Dockerfile
├── migrations/                     # Alembic
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
└── docs/
```

---

## 4. Message Contract (Event Envelope)

Every Kafka message uses one envelope so tracing, retries and audit work uniformly.

```python
# libs/common/schemas/envelope.py
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4
from pydantic import BaseModel, Field

class Envelope(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid4()))
    correlation_id: str                    # stays the same end-to-end
    source: str                            # service that produced it
    event_type: Literal[
        "patient.request", "patient.fhir.outgoing",
        "patient.fhir.incoming", "patient.response",
    ]
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    attempt: int = 0
    payload: dict[str, Any]
    error: dict[str, Any] | None = None
```

Kafka headers: `correlation_id`, `message_id`, `content-type=application/json`.

### Sample flow payloads

**Step 1: internal JSON** (from the document)
```json
{ "patientId": 1001, "name": "John Doe", "gender": "Male", "dob": "1995-05-20" }
```

**Step 2: FHIR Patient (R4)**
```json
{
  "resourceType": "Patient",
  "identifier": [{ "system": "urn:company:patient-id", "value": "1001" }],
  "name": [{ "use": "official", "family": "Doe", "given": ["John"] }],
  "gender": "male",
  "birthDate": "1995-05-20"
}
```

---

## 5. Data Model (PostgreSQL)

```sql
-- Audit trail: every request and state change
CREATE TABLE audit_log (
  id            BIGSERIAL PRIMARY KEY,
  correlation_id UUID NOT NULL,
  service       TEXT NOT NULL,
  action        TEXT NOT NULL,
  client_id     TEXT,
  payload       JSONB,
  created_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX ON audit_log (correlation_id);

-- Message lifecycle tracking (also used for idempotency)
CREATE TABLE message_tracking (
  correlation_id UUID PRIMARY KEY,
  patient_id     TEXT NOT NULL,
  status         TEXT NOT NULL,  -- RECEIVED|TRANSFORMED|SENT|RESPONDED|COMPLETED|FAILED
  fhir_resource_id TEXT,
  last_error     TEXT,
  created_at     TIMESTAMPTZ DEFAULT now(),
  updated_at     TIMESTAMPTZ DEFAULT now()
);

-- Idempotency for consumers (dedupe by message_id per service)
CREATE TABLE processed_messages (
  service     TEXT,
  message_id  UUID,
  processed_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (service, message_id)
);
```

---

## 6. Implementation Details

### 6.1 Shared Kafka helpers

```python
# libs/common/kafka/producer.py
from aiokafka import AIOKafkaProducer
import json

class KafkaProducer:
    def __init__(self, bootstrap: str):
        self._p = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            acks="all",
            enable_idempotence=True,
            value_serializer=lambda v: json.dumps(v, default=str).encode(),
            key_serializer=lambda k: k.encode(),
        )

    async def start(self): await self._p.start()
    async def stop(self):  await self._p.stop()

    async def send(self, topic: str, key: str, envelope: dict):
        headers = [("correlation_id", envelope["correlation_id"].encode())]
        await self._p.send_and_wait(topic, key=key, value=envelope, headers=headers)
```

```python
# libs/common/kafka/consumer.py  — base class used by all workers
import asyncio, json, structlog
from aiokafka import AIOKafkaConsumer

log = structlog.get_logger()
MAX_ATTEMPTS = 3

class BaseWorker:
    topic: str
    group_id: str
    dlq_topic: str

    def __init__(self, bootstrap, producer):
        self.producer = producer
        self.consumer = AIOKafkaConsumer(
            self.topic, bootstrap_servers=bootstrap, group_id=self.group_id,
            enable_auto_commit=False, auto_offset_reset="earliest",
            value_deserializer=lambda b: json.loads(b.decode()),
        )

    async def handle(self, envelope: dict) -> None:
        raise NotImplementedError

    async def run(self):
        await self.consumer.start()
        try:
            async for msg in self.consumer:
                env = msg.value
                try:
                    await self.handle(env)
                except Exception as exc:
                    env["attempt"] = env.get("attempt", 0) + 1
                    env["error"] = {"type": type(exc).__name__, "detail": str(exc)}
                    log.error("handler_failed", correlation_id=env["correlation_id"], err=str(exc))
                    if env["attempt"] >= MAX_ATTEMPTS:
                        await self.producer.send(self.dlq_topic, msg.key.decode(), env)
                    else:
                        await asyncio.sleep(2 ** env["attempt"])   # backoff
                        await self.producer.send(self.topic, msg.key.decode(), env)
                finally:
                    await self.consumer.commit()
        finally:
            await self.consumer.stop()
```

### 6.2 Service 1: Integration API (FastAPI)

Responsibilities: validate, authenticate, log, audit, publish. It returns **202 Accepted** with a `correlation_id`; it does not wait for the FHIR round-trip.

```python
# services/integration_api/app/main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from common.kafka.producer import KafkaProducer
from common.config import settings
from .routers import patients

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.producer = KafkaProducer(settings.kafka_bootstrap)
    await app.state.producer.start()
    yield
    await app.state.producer.stop()

app = FastAPI(title="Integration API", version="1.0", lifespan=lifespan)
app.include_router(patients.router, prefix="/api/v1")

@app.get("/health")
async def health(): return {"status": "ok"}
```

```python
# services/integration_api/app/routers/patients.py
from datetime import date
from typing import Literal
from uuid import uuid4
from fastapi import APIRouter, Depends, Request, status
from pydantic import BaseModel
from common.schemas.envelope import Envelope
from ..deps.auth import get_current_client

router = APIRouter(tags=["patients"])

class PatientIn(BaseModel):
    patientId: int
    name: str
    gender: Literal["Male", "Female", "Other", "Unknown"]
    dob: date

@router.post("/patients", status_code=status.HTTP_202_ACCEPTED)
async def submit_patient(body: PatientIn, request: Request, client=Depends(get_current_client)):
    cid = str(uuid4())
    env = Envelope(correlation_id=cid, source="integration-api",
                   event_type="patient.request", payload=body.model_dump(mode="json"))
    # 1) audit + tracking row (DB) ... 2) publish
    await request.app.state.producer.send("json.request", str(body.patientId), env.model_dump(mode="json"))
    return {"correlation_id": cid, "status": "RECEIVED"}

@router.get("/patients/status/{correlation_id}")
async def get_status(correlation_id: str):
    ...  # read message_tracking
```

### 6.3 Service 2: JSON → FHIR Transformer

```python
# services/json_to_fhir/app/mappers/patient_mapper.py
from fhir.resources.patient import Patient

GENDER = {"Male": "male", "Female": "female", "Other": "other", "Unknown": "unknown"}

def to_fhir_patient(src: dict) -> dict:
    first, *rest = src["name"].split()
    p = Patient.model_validate({
        "resourceType": "Patient",
        "identifier": [{"system": "urn:company:patient-id", "value": str(src["patientId"])}],
        "name": [{"use": "official", "family": " ".join(rest) or first,
                  "given": [first] if rest else []}],
        "gender": GENDER[src["gender"]],
        "birthDate": src["dob"],
    })
    return p.model_dump(mode="json", exclude_none=True)
```

```python
# services/json_to_fhir/app/worker.py
class JsonToFhirWorker(BaseWorker):
    topic, group_id, dlq_topic = "json.request", "json-to-fhir", "json.request.dlq"

    async def handle(self, env):
        fhir = to_fhir_patient(env["payload"])                # raises → retry/DLQ
        out = {**env, "source": "json-to-fhir", "event_type": "patient.fhir.outgoing",
               "payload": fhir, "attempt": 0, "error": None}
        await self.producer.send("fhir.outgoing", env["payload"]["patientId"].__str__(), out)
```

### 6.4 Service 3: FHIR Communication Service

Sends to the external server with retries. Both success **and** failure results are published to `fhir.incoming` so downstream stays uniform.

```python
# services/fhir_comm/app/fhir_client.py
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

class FhirClient:
    def __init__(self, base_url: str, token_provider):
        self._c = httpx.AsyncClient(base_url=base_url, timeout=15,
                                    headers={"Content-Type": "application/fhir+json"})
        self._token = token_provider

    @retry(stop=stop_after_attempt(4), wait=wait_exponential_jitter(1, 10),
           retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)))
    async def upsert_patient(self, patient: dict):
        ident = patient["identifier"][0]
        # Conditional update = idempotent (no duplicate patients on retry)
        r = await self._c.put(
            f"/Patient?identifier={ident['system']}|{ident['value']}",
            json=patient, headers={"Authorization": f"Bearer {await self._token()}"},
        )
        if r.status_code >= 500 or r.status_code == 429:
            r.raise_for_status()          # retryable
        return r
```

```python
# services/fhir_comm/app/worker.py
class FhirCommWorker(BaseWorker):
    topic, group_id, dlq_topic = "fhir.outgoing", "fhir-comm", "fhir.outgoing.dlq"

    async def handle(self, env):
        r = await self.client.upsert_patient(env["payload"])
        result = {"http_status": r.status_code, "body": r.json() if r.content else None}
        out = {**env, "source": "fhir-comm", "event_type": "patient.fhir.incoming",
               "payload": result, "attempt": 0}
        if r.status_code >= 400:
            out["error"] = {"type": "FhirServerError", "detail": r.text}   # 4xx: don't retry
        key = env["payload"]["identifier"][0]["value"]
        await self.producer.send("fhir.incoming", key, out)
```

### 6.5 Service 4: FHIR → JSON Transformer

Maps the FHIR response (`Patient` or `OperationOutcome`) to the internal format.

```python
def to_internal_response(env: dict) -> dict:
    res = env["payload"]
    if env.get("error"):
        return {"status": "FAILED", "reason": env["error"]["detail"]}
    body = res["body"] or {}
    return {
        "status": "SUCCESS",
        "fhirId": body.get("id"),
        "versionId": body.get("meta", {}).get("versionId"),
    }
```
Publishes to `json.response` with `event_type="patient.response"`.

### 6.6 Service 5: Internal Processing

- Update the internal DB (`message_tracking.status = COMPLETED/FAILED`, store `fhir_resource_id`).
- Trigger workflows / notifications (email, webhook to ERP/CRM, Slack).
- Use the **outbox pattern** if it must also emit further events.

---

## 7. Cross-Cutting Concerns

### Reliability
- Producer: `acks=all`, idempotent producer enabled.
- Consumers: manual commit **after** processing (at-least-once).
- **Idempotency:** check `processed_messages (service, message_id)` before handling, so duplicates are harmless.
- **Retries:** exponential backoff in-process (transient errors), then DLQ after 3 attempts.
- **DLQ handling:** admin endpoint / script to inspect and replay DLQ messages.
- **Distinguish errors:** 5xx/429/timeout → retry; 4xx / validation → straight to `fhir.incoming` with error (no retry).

### Security
- Integration API: OAuth2 client-credentials (JWT) or API keys per client; rate limiting (`slowapi`).
- Kafka: TLS + SASL/SCRAM, ACLs per service.
- External FHIR: OAuth2 (SMART Backend Services) or mTLS; secrets in Vault / K8s Secrets.
- **PHI/PII:** mask names, DOB and IDs in logs; encrypt DB at rest; audit all access. (Healthcare data. Check HIPAA / local regulation such as India's DPDP Act and ABDM guidelines if applicable.)

### Observability
- Structured JSON logs including `correlation_id` in every line.
- OpenTelemetry trace propagation through Kafka headers.
- Metrics: consumer lag, messages/sec, transform errors, FHIR latency, DLQ depth.
- Alerts: DLQ depth > 0, lag > threshold, FHIR error rate > 5%.

### Schema evolution
- Version the envelope (`schema_version`) and payloads; consider Schema Registry (Avro/JSON Schema) once more producers exist.

---

## 8. Local Development — docker-compose

```yaml
services:
  kafka:
    image: apache/kafka:3.8.0
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  kafka-ui:
    image: provectuslabs/kafka-ui
    ports: ["8080:8080"]
    environment: { KAFKA_CLUSTERS_0_NAME: local, KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092 }

  postgres:
    image: postgres:16
    environment: { POSTGRES_USER: app, POSTGRES_PASSWORD: app, POSTGRES_DB: integration }
    ports: ["5432:5432"]

  fhir-server:            # local stand-in for the external hospital server
    image: hapiproject/hapi:latest
    ports: ["8090:8080"]

  integration-api:
    build: ./services/integration_api
    ports: ["8000:8000"]
    depends_on: [kafka, postgres]
  json-to-fhir:        { build: ./services/json_to_fhir,        depends_on: [kafka] }
  fhir-comm:           { build: ./services/fhir_comm,           depends_on: [kafka, fhir-server] }
  fhir-to-json:        { build: ./services/fhir_to_json,        depends_on: [kafka] }
  internal-processing: { build: ./services/internal_processing, depends_on: [kafka, postgres] }
```

Use **HAPI FHIR** locally as the "external server" so you can test end-to-end without a real hospital connection.

---

## 9. Testing Strategy

| Level | What | Tools |
|-------|------|-------|
| Unit | Mappers (JSON↔FHIR), validators, error classification | pytest |
| Contract | Output validates against FHIR R4 (`fhir.resources`) | pytest |
| Integration | Each worker against real Kafka/Postgres | testcontainers |
| E2E | POST → … → status `COMPLETED`, using HAPI FHIR | pytest + httpx |
| Failure | Kill FHIR server, bad JSON, duplicate message, broker restart | scripted |
| Load | 100–1000 msg/s, watch consumer lag | Locust / k6 |

---

## 10. Phased Delivery Plan

| Phase | Duration | Deliverables | Done when |
|-------|----------|--------------|-----------|
| **0. Setup** | 2–3 days | Repo, `libs/common`, docker-compose, CI (lint, tests), topic creation script | `docker compose up` gives a running Kafka + Postgres |
| **1. Integration API** | 3–4 days | POST `/patients`, auth, validation, audit table, status endpoint, publish to `json.request` | Message visible in Kafka UI |
| **2. JSON→FHIR** | 3–4 days | Patient mapper, worker, unit tests, FHIR validation | Valid FHIR lands on `fhir.outgoing` |
| **3. FHIR Comm** | 4–5 days | httpx client, OAuth token handling, retry, conditional PUT, error mapping | Patient created in HAPI FHIR |
| **4. FHIR→JSON + Internal Processing** | 3–4 days | Response mapper, DB updates, notifications | Status becomes `COMPLETED` via API |
| **5. Hardening** | 4–5 days | Retry/DLQ topics, idempotency, replay tool, tracing, metrics, dashboards | Failure tests pass |
| **6. Security & Compliance** | 3 days | TLS/SASL, secret management, PHI masking, pen-test checklist | Security review signed off |
| **7. Deploy** | 3–4 days | Dockerfiles, Helm charts / K8s manifests, staging, load test | Staging E2E passes under load |
| **8. Go-live** | 2 days | Runbook, on-call alerts, UAT with the real hospital endpoint | Production release |

**Estimated total: ~5–6 weeks** for one to two developers.

---

## 11. Extensibility (after MVP)

- Add more resources: `Encounter`, `Observation`, `Claim`, `Coverage` → new mapper per resource type, routed by `event_type`.
- Add a mapping-config layer (YAML) so field mappings change without code deploys.
- Inbound flow: external FHIR server → webhook/subscription → `fhir.incoming` directly.
- Schema Registry + Avro when more teams produce events.
- Kafka Connect sink to a data warehouse for analytics.

---

## 12. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Duplicate patients in external server | Conditional update on identifier + idempotent consumers |
| External server downtime | Kafka buffers messages; retries with backoff; DLQ + replay |
| Message ordering issues | Key by `patientId` |
| Mapping errors for edge cases (names, dates) | Strong unit tests, FHIR validation before send, DLQ |
| PHI leakage in logs | Log masking middleware, code review checklist |
| Schema drift between systems | Versioned envelope, contract tests |

---

## 13. Getting Started Checklist

- [ ] Create repo and `libs/common` package
- [ ] Write `docker-compose.yml` and start Kafka, Postgres, HAPI FHIR
- [ ] Create topics (`json.request`, `fhir.outgoing`, `fhir.incoming`, `json.response` + DLQs)
- [ ] Implement Envelope and Kafka producer/consumer base classes
- [ ] Build Integration API `POST /patients` and confirm messages arrive in Kafka UI
- [ ] Build each worker in order (2 → 3 → 4 → 5), testing after each
- [ ] Run the E2E test: POST John Doe → see the Patient in HAPI FHIR → status `COMPLETED`
- [ ] Add observability, security and load tests
