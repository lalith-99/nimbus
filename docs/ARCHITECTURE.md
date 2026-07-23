# Nimbus — Architecture Deep Dive

> A multi-tenant **notification orchestration platform** written in Go. It accepts notification
> requests over REST and gRPC, durably persists them, and delivers them across **email, SMS, and
> webhook** channels with retries, dead-lettering, idempotency, rate limiting, circuit breaking,
> and an optional **AI/RAG** layer for natural-language operations.

This document is the canonical system-design reference. It is written as an **onboarding and
design guide** — every major decision includes the *why* and the *tradeoff*, not just the *what*.

> Want the **code-level** mechanics instead of the diagrams — how each subsystem actually works, line
> by line, explained from first principles? See the [Engineering Internals guide](INTERNALS.md).

---

## Table of Contents

1. [System at a Glance](#1-system-at-a-glance)
2. [C4 Level 1 — System Context](#2-c4-level-1--system-context)
3. [C4 Level 2 — Container View](#3-c4-level-2--container-view)
4. [C4 Level 3 — Internal Components](#4-c4-level-3--internal-components)
5. [Data Model (ERD)](#5-data-model-erd)
6. [Request Lifecycle — Create a Notification](#6-request-lifecycle--create-a-notification)
7. [The Background Worker](#7-the-background-worker)
8. [Notification State Machine](#8-notification-state-machine)
9. [Reliability Patterns](#9-reliability-patterns)
10. [gRPC & Server-Streaming](#10-grpc--server-streaming)
11. [The AI / RAG Subsystem](#11-the-ai--rag-subsystem)
12. [Deployment Topology (AWS)](#12-deployment-topology-aws)
13. [Scaling & Capacity Planning](#13-scaling--capacity-planning)
14. [Failure Modes & Mitigations](#14-failure-modes--mitigations)
15. [Key Design Decisions & Tradeoffs](#15-key-design-decisions--tradeoffs)
16. [Design Principles Summary](#16-design-principles-summary)

---

## 1. System at a Glance

Nimbus solves one hard problem well: **"accept a notification request and guarantee it is
eventually delivered exactly once (or explicitly dead-lettered), across multiple channels, for
thousands of independent tenants."**

| Concern | How Nimbus handles it |
|---|---|
| **Durability** | Every request is written to Postgres *before* we acknowledge the client (transactional outbox). The DB is the single source of truth. |
| **At-least-once delivery** | Two paths deliver pending rows: an event-driven SQS consumer (hot path) and a DB-poll worker (backstop). Both retry with exponential backoff until success or max attempts. |
| **No duplicate sends across replicas or paths** | Every row is claimed with `FOR UPDATE SKIP LOCKED` before sending — the batch poller claims disjoint batches, the SQS consumer claims one row by id. Whichever path wins the atomic claim sends; the loser skips. |
| **Idempotent writes** | Redis-backed idempotency keys collapse client retries into a single notification. |
| **Fair multi-tenancy** | Sliding-window rate limiting per tenant; every query is tenant-scoped. |
| **Downstream resilience** | Each channel sender is wrapped in a circuit breaker that fails fast when a provider is down. |
| **Poison-message safety** | After N failures a notification moves to a Dead Letter Queue for inspection / manual retry. |
| **Observability** | Prometheus metrics, structured zap logs, health + circuit-status endpoints. |
| **Operability via natural language** | Optional AI compose endpoint + a RAG pipeline that answers questions grounded in your own data. |

**Tech stack:** Go 1.23 · Chi (HTTP) · gRPC + Protobuf · PostgreSQL + pgvector · Redis · AWS
(SES / SNS / SQS) · Prometheus · Docker · Terraform (ECS Fargate).

---

## 2. C4 Level 1 — System Context

Who talks to Nimbus, and what does Nimbus talk to?

```mermaid
graph TB
    subgraph Clients
        ext["External Clients<br/>(web apps, 3rd parties)"]
        svc["Internal Microservices"]
        ops["Operators / On-call"]
    end

    subgraph Nimbus["Nimbus Platform"]
        nimbus["Notification<br/>Orchestration Service"]
    end

    subgraph Downstream["Downstream Providers"]
        ses["AWS SES<br/>(Email)"]
        sns["AWS SNS<br/>(SMS)"]
        hook["Customer Webhooks<br/>(HTTP POST)"]
        openai["OpenAI API<br/>(LLM + Embeddings)"]
    end

    ext -->|"REST / JSON<br/>:8080"| nimbus
    svc -->|"gRPC / Protobuf<br/>:9090"| nimbus
    ops -->|"/metrics, /health,<br/>admin endpoints"| nimbus

    nimbus -->|"send email"| ses
    nimbus -->|"send SMS"| sns
    nimbus -->|"deliver event"| hook
    nimbus -->|"compose / embed / answer"| openai
```

**Design note:** REST is the public front door for external clients; gRPC is the internal
hallway between our own services. Same business logic, two transports chosen per audience —
JSON for ubiquity, Protobuf for typing, binary efficiency, and streaming.

---

## 3. C4 Level 2 — Container View

Zooming in: the deployable units and the stateful systems they depend on. The API process and the
worker run **in the same binary today** (a goroutine) but are designed to split into independent
deployments without code changes.

```mermaid
graph TB
    client["Client"]

    subgraph Gateway["Nimbus Gateway (single Go binary)"]
        http["HTTP Server :8080<br/>Chi router + middleware"]
        grpc["gRPC Server :9090<br/>unary + streaming"]
        consumer["SQS Consumer<br/>(hot path, N receivers)"]
        worker["DB-poll Worker<br/>(goroutine, 30s backstop)"]
        breakers["Circuit Breakers<br/>per channel"]
    end

    subgraph State["Stateful Systems"]
        pg[("PostgreSQL<br/>+ pgvector")]
        redis[("Redis<br/>idempotency + rate limit")]
        sqs[["AWS SQS<br/>+ DLQ"]]
    end

    subgraph Providers["Delivery Providers"]
        ses["AWS SES"]
        sns["AWS SNS"]
        hook["Webhooks"]
    end

    client -->|REST| http
    client -->|gRPC| grpc

    http -->|"idempotency,<br/>rate limit"| redis
    http -->|"INSERT pending<br/>(source of truth)"| pg
    http -->|"enqueue"| sqs

    grpc --> pg

    sqs -->|"receive batch"| consumer
    consumer -->|"CLAIM by id"| pg
    worker -->|"CLAIM batch ... SKIP LOCKED"| pg
    consumer --> breakers
    worker --> breakers
    breakers --> ses
    breakers --> sns
    breakers --> hook

    http -->|/metrics| client
```

**Key insight (transactional outbox + hybrid delivery):** the HTTP handler writes the durable
Postgres row first, then enqueues to SQS. The **SQS consumer** picks it up in milliseconds (hot path),
while the **DB-poll worker** runs as a slower reconciliation backstop. Both claim the row atomically
in Postgres before sending, so they never double-send. If SQS is down we still return `201` and the
poller delivers it — **SQS accelerates delivery but is never a hard dependency.**

---

## 4. C4 Level 3 — Internal Components

The Go package layout maps cleanly onto responsibilities. Arrows show compile-time dependencies.

```mermaid
graph TD
    main["cmd/gateway<br/>composition root"]

    subgraph Transport
        api["internal/api<br/>REST handlers + middleware"]
        igrpc["internal/grpc<br/>gRPC server + interceptors"]
    end

    subgraph Core
        worker["internal/worker<br/>poll loop + SQS consumer + senders"]
        cb["internal/circuitbreaker<br/>FSM + protected sender"]
    end

    subgraph Data
        db["internal/db<br/>repository + models"]
        rds["internal/redis<br/>idempotency + ratelimit"]
        sqs["internal/sqs<br/>producer/consumer transport"]
    end

    subgraph Intelligence
        ai["internal/ai<br/>compose + enrichment"]
        rag["internal/rag<br/>embed + retrieve + rerank + guard"]
    end

    subgraph Cross
        cfg["internal/config"]
        obs["internal/observ (zap)"]
        met["internal/metrics (prometheus)"]
    end

    main --> api & igrpc & worker & ai & rag & cfg & obs & met
    api --> db & rds & sqs
    igrpc --> db
    worker --> db & cb & sqs
    cb --> worker
    ai --> db
    rag --> db & ai
```

**Design note:** Each consumer defines its own narrow repository interface (e.g. the gRPC
server's `NotificationRepository` only declares the two methods it needs). That's interface
segregation — it keeps packages loosely coupled and trivially mockable in tests.

---

## 5. Data Model (ERD)

Three tables carry the system. All are tenant-scoped.

```mermaid
erDiagram
    notifications ||--o{ dead_letter_notifications : "fails into"
    notifications {
        uuid id PK
        uuid tenant_id "indexed"
        uuid user_id
        varchar channel "email|sms|webhook"
        jsonb payload
        varchar status "pending|processing|sent|failed|dead_lettered"
        int attempt
        text error_message
        timestamptz next_retry_at
        timestamptz created_at
        timestamptz updated_at
    }

    dead_letter_notifications {
        uuid id PK
        uuid original_notification_id FK
        uuid tenant_id
        uuid user_id
        varchar channel
        jsonb payload
        int attempts
        text last_error
        varchar status "pending|retried|discarded"
        uuid retried_notification_id
        timestamptz created_at
    }

    knowledge_base {
        uuid id PK
        uuid tenant_id "indexed"
        text content
        varchar source_type "notification|faq|doc"
        uuid source_id
        vector embedding "1536-dim, HNSW"
        tsvector content_tsv "GENERATED, GIN"
        timestamptz created_at
    }
```

**Indexing strategy:**

- `idx_notifications_retry (status, next_retry_at, created_at) WHERE status IN ('pending','processing')`
  — a **partial index**. The worker's hot query only scans rows that can actually be claimed, so
  the index stays tiny even when millions of `sent` rows accumulate.
- `idx_notifications_tenant (tenant_id, created_at DESC)` — powers tenant list views with no sort.
- `knowledge_base` carries **two** ANN/search indexes: an **HNSW** index for vector similarity and a
  **GIN** index on the generated `content_tsv` column for full-text — this is what makes *hybrid*
  retrieval possible in a single SQL query.

---

## 6. Request Lifecycle — Create a Notification

The most important flow in the system. Note where durability is established and where idempotency
and rate limiting intercept.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant MW as Rate-Limit MW
    participant H as API Handler
    participant R as Redis
    participant DB as Postgres
    participant Q as SQS
    participant W as Worker

    C->>MW: POST /v1/notifications
    MW->>R: ZADD sliding window (tenant)
    alt over limit
        MW-->>C: 429 Too Many Requests
    end
    MW->>H: forward

    H->>H: validate (UUIDs, channel, JSON)
    H->>R: CheckOrReserve(idempotency key)
    alt key already completed
        R-->>H: cached result
        H-->>C: 201 (X-Idempotency-Replayed: true)
    else key in-flight
        R-->>H: ErrDuplicateRequest
        H-->>C: 409 Conflict
    end

    H->>DB: INSERT notification (status=pending)
    Note over DB: Durable source of truth ✔
    alt insert fails
        H->>R: Release(key)
        H-->>C: 500
    end

    H->>R: Store(idempotency result)
    H-)Q: Enqueue (non-blocking)
    H-->>C: 201 Created {id}

    Note over W,DB: ...asynchronously (two paths, one claim guard)...
    Q-)W: SQS consumer receives (hot path)
    W->>DB: ClaimNotificationByID (atomic)
    W->>W: deliver via channel sender
    Note over W,DB: DB-poll worker (backstop) claims anything SQS missed
```

**Three key properties of this flow:**

1. **Durability boundary:** we return `201` only after the Postgres `INSERT`. The client's
   acknowledgement is a promise we can keep even if everything downstream crashes.
2. **Idempotency release on failure:** if the DB write fails *after* we reserved the key, we
   `Release()` it so the client's retry isn't poisoned with a false `409` for 5 minutes.
3. **Non-blocking enqueue:** the `-)` (async) arrow to SQS never blocks the response and never fails
   the request. The SQS consumer delivers on the hot path; if the enqueue or SQS itself is down, the
   DB-poll backstop still delivers the durable row.

---

## 7. Delivery: the hybrid worker

Delivery runs on **two paths that share one atomic claim guard**, so they can run concurrently
without ever double-sending:

| Path | Trigger | Claim | Role |
|---|---|---|---|
| **SQS consumer** | message received (long-poll) | `ClaimNotificationByID` (one row) | Hot path — sub-second dispatch |
| **DB-poll worker** | ticker (30s when SQS is on, else 5s) | `ClaimPendingNotifications` (batch, SKIP LOCKED) | Reconciliation backstop — delayed retries + anything SQS dropped |

Both paths hand the claimed row to the **same `Dispatcher`**, so send / retry / dead-letter behavior
is identical regardless of how the row was claimed.

```mermaid
flowchart TD
    subgraph Hot["SQS consumer (hot path)"]
        recv(["receive batch (≤10)"]) --> claim1["ClaimNotificationByID"]
        claim1 --> won{"claim won?"}
        won -->|no| ack1["ack (delete msg)"]
        won -->|yes| disp1["Dispatcher.Dispatch"]
        disp1 --> ack1
    end

    subgraph Backstop["DB-poll worker (backstop)"]
        tick(["Tick every 30s"]) --> claim2["ClaimPendingNotifications(limit)"]
        claim2 --> loop["for each notification"]
        loop --> disp2["Dispatcher.Dispatch"]
    end

    disp1 --> send["sender.Send via circuit breaker"]
    disp2 --> send
    send --> ok{"success?"}
    ok -->|yes| sent["status = sent"]
    ok -->|no| retryq{"attempt &lt; maxRetries (5)?"}
    retryq -->|yes| backoff["status = pending<br/>next_retry_at = now + backoff<br/>(1m → 5m → 15m)"]
    retryq -->|no| dlq["MoveToDeadLetter (txn):<br/>1. INSERT dlq row<br/>2. status = dead_lettered"]
```

> **Why the poller stays as a backstop:** an SQS message is one-shot. After the first attempt the
> consumer always acks it. If the send failed but has retries left, `Dispatch` schedules the row back
> to `pending` with a future `next_retry_at` — and the DB poller picks it up when due. The poller also
> catches any message SQS ever drops. Fast path for the common case, durable path for correctness.

### The atomic claim — the heart of horizontal scalability

```sql
UPDATE notifications
SET status = 'processing', updated_at = NOW()
WHERE id IN (
    SELECT id FROM notifications
    WHERE (status = 'pending' AND (next_retry_at IS NULL OR next_retry_at <= NOW()))
       OR (status = 'processing' AND updated_at < NOW() - INTERVAL '5 minutes') -- reclaim stuck rows
    ORDER BY created_at
    LIMIT $1
    FOR UPDATE SKIP LOCKED          -- ⚡ the magic
)
RETURNING ...;
```

- **`FOR UPDATE SKIP LOCKED`** lets N worker replicas pull **disjoint** batches concurrently with
  zero coordination — no Redis lock, no leader election. Postgres is the coordinator.
- **Stuck-row reclamation:** if a worker crashes mid-send, its row is stranded in `processing`.
  The `OR processing older than 5m` clause lets a healthy worker reclaim it — self-healing, no
  manual intervention.
- **Claim = mark processing in one statement:** there's no read-then-write race window, so the
  same row can never be sent twice by two replicas.

**Design note:** This is a database-as-queue pattern. Exactly-once-ish claiming comes for free
from Postgres' row locking, which removes an entire class of distributed-locking bugs. The same
claim guard, scoped to a single id (`ClaimNotificationByID`), is what lets the SQS consumer run as a
parallel hot path without risking a double-send against the poller.

---

## 8. Notification State Machine

Every notification walks this graph. Terminal states are `sent`, `dead_lettered`, and `discarded`.

```mermaid
stateDiagram-v2
    [*] --> pending: API/gRPC create
    pending --> processing: claimed (SQS consumer or poll, atomic)
    processing --> sent: delivery ok
    processing --> pending: delivery failed, attempt < 5 (backoff)
    processing --> dead_lettered: delivery failed, attempt = 5
    processing --> pending: worker crashed (reclaimed after 5m)

    dead_lettered --> pending: operator retry (new notification)
    dead_lettered --> discarded: operator discard

    sent --> [*]
    discarded --> [*]
```

The retry edge `processing → pending` carries an **exponential-ish backoff** (`1m, 5m, 15m`,
capped) stamped into `next_retry_at`, so the worker won't re-pick the row until the delay elapses.

---

## 9. Reliability Patterns

Nimbus stacks four independent safety mechanisms. Each is small; together they make the system
production-grade.

### 9.1 Idempotency (Redis)

```mermaid
sequenceDiagram
    participant C as Client
    participant H as Handler
    participant R as Redis

    C->>H: POST (Idempotency-Key: abc)
    H->>R: SET key=processing NX (CheckOrReserve)
    alt first time
        R-->>H: reserved
        H->>H: do work, then Store(result)
        H-->>C: 201
    else retry while in-flight
        R-->>H: exists=processing
        H-->>C: 409 Conflict
    else retry after completion
        R-->>H: exists=result
        H-->>C: 201 (replayed)
    end
```

- **Auto keys (5 min TTL):** if the client sends no key, we hash `tenant|user|channel|payload`.
  This catches accidental network retries without blocking intentional re-sends.
- **Client keys (24 h TTL):** explicit `Idempotency-Key` header → Stripe-style strong dedup.
- **Compare-and-delete release:** `Release()` only deletes the key if it's still the `processing`
  marker (Lua CAS), so it can never clobber a stored result.

### 9.2 Sliding-Window Rate Limiting (Redis Sorted Sets)

Per-tenant **100 req/min**. We store request timestamps in a `ZSET`, trim everything older than the
window with `ZREMRANGEBYSCORE`, then `ZCARD` to count. Unlike a fixed window, this has **no
burst-at-boundary** problem — a true rolling 60-second view.

### 9.3 Circuit Breaker (per channel)

```mermaid
stateDiagram-v2
    [*] --> Closed
    Closed --> Open: 5 consecutive failures
    Open --> HalfOpen: after 30s recovery timeout
    HalfOpen --> Closed: probe succeeds
    HalfOpen --> Open: probe fails
    note right of Open
        Requests fail fast.
        We stop hammering a
        provider that's already down.
    end note
```

Each sender (SES / SNS / webhook) gets its own breaker, so an SMS outage never blocks email.
`GET /v1/health/circuits` exposes live state; `POST /v1/admin/circuits/{name}/reset` forces a probe.

### 9.4 Dead Letter Queue

After 5 failed attempts a notification is moved to `dead_letter_notifications` **inside a
transaction** (insert DLQ row + flip source status atomically). Operators can inspect, **retry**
(creates a fresh notification), or **discard** via the `/v1/dlq` endpoints.

---

## 10. gRPC & Server-Streaming

Internal services use gRPC for typing, efficiency, and — the headline feature — **server-streaming
delivery updates**.

```mermaid
sequenceDiagram
    autonumber
    participant S as Internal Service
    participant I as Auth Interceptor
    participant G as gRPC Server
    participant DB as Postgres

    S->>I: StreamDeliveryUpdates(id) + Bearer token
    I->>I: token → tenant_id (inject into ctx)
    I->>G: forward (wrapped stream)
    G->>DB: GetNotification(id)
    G->>G: verify owner.tenant == ctx.tenant
    alt not owner
        G-->>S: NOT_FOUND (no enumeration oracle)
    end
    loop every 2s until terminal
        G->>DB: read status
        G-->>S: push DeliveryUpdate
    end
    G-->>S: close stream (sent/failed/dead_lettered)
```

**Why streaming beats polling:** client-side polling fires *one request per interval* regardless of
whether anything changed. Server-streaming inverts control — **one** request, the server pushes only
the cadence it chooses. On a 100K-events/day system that eliminates thousands of redundant requests.

**Security (IDOR defense):** the tenant is taken from the **authenticated token**, never the request
body. Cross-tenant reads return `NOT_FOUND` (not `PERMISSION_DENIED`) so an attacker can't use the
error to discover which IDs exist. This maps to **OWASP API1: Broken Object Level Authorization**.

---

## 11. The AI / RAG Subsystem

Optional (enabled when `OPENAI_API_KEY` is set). Two capabilities:

1. **Compose** (`POST /v1/ai/compose`) — natural language → notifications via LLM function calling.
2. **Ask** (`POST /v1/ai/ask`) — a full **Retrieval-Augmented Generation** pipeline that answers
   questions grounded in the tenant's own notification history, with citations.

```mermaid
flowchart LR
    q["User query"] --> guard1["1. Injection guard<br/>(regex, fail fast)"]
    guard1 --> mask["2. PII mask<br/>email/phone/SSN → tokens"]
    mask --> embed["3. Embed<br/>text-embedding-3-small (1536-d)"]
    embed --> hybrid["4. Hybrid search<br/>vector + full-text → RRF (k=60)<br/>top-20 candidates"]
    hybrid --> rerank["5. Rerank<br/>cross-encoder → top-5"]
    rerank --> prompt["6. Build prompt<br/>numbered context 1..5 + question"]
    prompt --> llm["7. LLM answer<br/>system prompt pinned"]
    llm --> restore["8. Restore PII<br/>tokens → real values"]
    restore --> ans["Cited answer"]
```

**Design highlights:**

- **Hybrid retrieval + Reciprocal Rank Fusion:** vector search nails *semantic* matches; full-text
  (BM25-style) nails *exact keyword* matches. RRF fuses them by **rank position**
  ($\text{score}(d) = \sum_i \frac{1}{k + \text{rank}_i(d)}$, $k=60$) so we never need the two systems
  to share a score scale. Best of both, in one SQL query.
- **Two-stage retrieval (recall → precision):** cheap hybrid search casts a wide net (top-20), then
  a more expensive reranker picks the precise top-5. Standard IR pattern.
- **Defense in depth:** a regex guard *and* a pinned system prompt. Even if an injection slips past
  the guard, the system prompt instructs the model to ignore instruction-overrides.
- **PII never reaches OpenAI:** masking happens *before* the embedding and chat calls; real values
  are restored only in the final local response.
- **pgvector over a dedicated vector DB:** we already run Postgres, so we get ACID, one backup
  strategy, and the ability to JOIN vectors with relational data — zero new infrastructure.

---

## 12. Deployment Topology (AWS)

Provisioned with **Terraform** (`/terraform`). Production target is **ECS Fargate** behind an ALB,
with managed Postgres, Redis, and SQS.

```mermaid
graph TB
    user["Clients"] --> alb["Application Load Balancer<br/>health: /health"]

    subgraph VPC["VPC (2 AZs)"]
        subgraph Public["Public Subnets"]
            alb
        end
        subgraph Private["Private Subnets"]
            ecs1["ECS Fargate Task 1<br/>gateway + SQS consumer + worker"]
            ecs2["ECS Fargate Task N"]
        end
        subgraph DataTier["Data Tier"]
            rds[("RDS PostgreSQL<br/>+ pgvector")]
            redis[("ElastiCache Redis")]
        end
    end

    subgraph Managed["Managed Services"]
        sqs[["SQS + DLQ"]]
        sns["SNS topic"]
        ses["SES"]
        secrets["Secrets Manager<br/>DB creds, DATABASE_URL"]
        ecr["ECR<br/>app + migrator images"]
    end

    alb --> ecs1 & ecs2
    ecs1 & ecs2 --> rds & redis & sqs & sns & ses & secrets
    ecr -.->|image pull| ecs1 & ecs2
```

- **Migrations** run as a separate one-shot ECS task from a dedicated `migrator` image — schema
  changes are decoupled from app rollout.
- **Secrets** (DB password, connection string) live in **Secrets Manager**, injected as env vars —
  never baked into images or Terraform state in plaintext.
- CI/CD via **GitHub Actions** (`.github/`) builds, tests, pushes to ECR, and updates the service.

---

## 13. Scaling & Capacity Planning

| Dimension | Current | How it scales |
|---|---|---|
| **API throughput** | 1 task | Stateless → add ECS tasks behind the ALB. |
| **Delivery throughput** | in-process SQS consumer (N receivers × batch-of-10) + poll worker | Raise `Receivers`/`Concurrency`, or split delivery into its own deployment; `SKIP LOCKED` + per-id claim mean N nodes need **zero** coordination. |
| **Write durability** | single RDS | Multi-AZ failover; read replicas for list endpoints. |
| **Rate-limit / idempotency** | single Redis | Cluster mode; keys are already namespaced by tenant. |
| **Vector search** | HNSW in Postgres | Good to ~tens of millions of vectors before a dedicated ANN store is warranted. |

**Back-of-envelope:** at **10M notifications/day ≈ 116/sec average** (higher in bursts). The SQS
consumer defaults — 3 receivers pulling batches of 10, up to 10 concurrent sends each — absorb that
with large headroom, and the DB-poll backstop (30s tick, 10 rows/batch) mops up delayed retries. Both
levers scale horizontally well past 10M/day.

---

## 14. Failure Modes & Mitigations

| Failure | Blast radius | Mitigation |
|---|---|---|
| SQS unavailable | none (slower) | Enqueue is non-blocking; the DB-poll backstop still delivers every row. |
| Redis unavailable | degraded | Idempotency + rate limiting disabled, requests still served (logged warn). |
| A provider (e.g. SES) down | that channel only | Circuit breaker opens → fail fast → retries/DLQ; other channels unaffected. |
| Worker/consumer crash mid-send | one batch/message | Row stuck in `processing` is reclaimed after 5 min; unacked SQS messages reappear after the visibility timeout. |
| SQS redelivers an already-sent message | none | `ClaimNotificationByID` returns "not claimable"; the consumer just acks — no re-send. |
| Poison message (always fails) | one notification | Moves to DLQ after 5 attempts; never blocks the queue. |
| Duplicate client retry | none | Idempotency key collapses it to one notification. |
| Two workers/paths, same row | none | Atomic claim (`FOR UPDATE SKIP LOCKED` / claim-by-id) guarantees a single winner. |
| Prompt injection (AI) | none | Regex guard + pinned system prompt (defense in depth). |
| PII leak to OpenAI | none | Masked before any external API call. |

---

## 15. Key Design Decisions & Tradeoffs

> Each decision below names the alternative that was considered and the cost that was accepted.

| Decision | Why | Tradeoff we accepted |
|---|---|---|
| **Transactional outbox** (DB first, then enqueue) | Durability without a distributed transaction across DB + queue | Delivery is eventually-consistent, not inline |
| **Hybrid delivery** (SQS consumer hot path + DB-poll backstop, one claim guard) | Sub-second dispatch *and* guaranteed delivery even if SQS drops or is down | Two delivery paths to reason about (kept safe by the shared atomic claim) |
| **DB-as-queue with `SKIP LOCKED`** | Exactly-once claiming, horizontal scaling, no extra infra | Backstop polling latency; not suited to millions of msgs/sec (we're far below that) |
| **Two ports: REST :8080 + gRPC :9090** | Clean protocol separation; no h2c multiplexing complexity | Two listeners to operate |
| **Per-channel circuit breakers** | Isolate provider failures | More breaker state to monitor |
| **Idempotency in Redis, not DB unique constraint** | Sub-ms checks, TTL-based expiry, no schema bloat | Redis becomes a soft dependency (gracefully degraded) |
| **pgvector, not Pinecone/Weaviate** | Zero new infra, ACID, JOIN vectors with relational data | ~10s-of-millions vector ceiling |
| **Hybrid search + RRF** | Robust across semantic *and* keyword queries | Slightly more complex SQL than pure-vector |
| **Server-streaming gRPC for status** | ~90% fewer requests vs polling | gRPC-only feature (REST clients still poll) |
| **`NOT_FOUND` on cross-tenant access** | No enumeration oracle (OWASP API1) | Slightly less precise error for legitimate 404s |

---

## 16. Design Principles Summary

The core principles that drive the system's design:

- **The database is the source of truth and the queue.** Outbox pattern + `FOR UPDATE SKIP LOCKED`
  gives durability and lock-free horizontal scaling in one move.
- **SQS accelerates delivery but is never a hard dependency.** The consumer is the hot path; if it's
  down, the DB poll backstop still delivers, and a write is never failed because a queue hiccupped.
- **Idempotency is two-tier.** Auto content-hash keys (5 min) for accidental retries, explicit
  client keys (24 h) for strong dedup — the same model Stripe uses.
- **Sliding window, not fixed window.** Redis sorted sets eliminate the boundary-burst problem.
- **Circuit breakers are per-channel.** A single provider outage can't cascade to the other channels.
- **Crashes self-heal.** Stuck `processing` rows are reclaimed after 5 minutes.
- **RAG is grounded and guarded.** Hybrid retrieval + RRF for relevance; regex guard + pinned
  prompt + PII masking for safety; citations for verifiability.
- **Security is tenant-from-token, never tenant-from-body.** Closes the IDOR hole and returns
  `NOT_FOUND` to avoid leaking IDs.

---

*See also: [README](../README.md) for quick start, and [API Reference](API.md) for endpoint and
gRPC contract details.*
