# Nimbus — Engineering Internals

A **code-level** guide to how Nimbus actually works. The [Architecture](ARCHITECTURE.md) doc shows
the *shape* of the system with diagrams; this doc explains the *mechanics* — what each piece of code
does, line by line, and **why it was built that way** (including the alternatives that were rejected
and the cost of each choice).

It assumes you can read Go but does **not** assume you already know gRPC, the outbox pattern,
`SKIP LOCKED`, circuit breakers, or RAG. Each of those is explained from scratch the first time it
appears.

> **How to read this:** go top-to-bottom once. The order is deliberate — we start from the problem,
> then the data model, then follow a single request all the way through the system. After that, use
> it as a reference. Every section ends with a **Why / Alternatives / Tradeoff** block.

---

## Table of Contents

1. [The one idea the whole system is built on](#1-the-one-idea-the-whole-system-is-built-on)
2. [What problem are we actually solving?](#2-what-problem-are-we-actually-solving)
3. [How the code is laid out (Go project anatomy)](#3-how-the-code-is-laid-out-go-project-anatomy)
4. [The data model — everything starts here](#4-the-data-model--everything-starts-here)
5. [The write path: `POST /v1/notifications` line by line](#5-the-write-path-post-v1notifications-line-by-line)
6. [The async path: the background worker](#6-the-async-path-the-background-worker)
7. [Delivery: senders, routing, and circuit breakers](#7-delivery-senders-routing-and-circuit-breakers)
8. [Reliability primitives: idempotency & rate limiting](#8-reliability-primitives-idempotency--rate-limiting)
9. [gRPC — what it is and why it's here](#9-grpc--what-it-is-and-why-its-here)
10. [The AI / RAG subsystem](#10-the-ai--rag-subsystem)
11. [Cross-cutting concerns](#11-cross-cutting-concerns)
12. [The big tradeoffs, consolidated](#12-the-big-tradeoffs-consolidated)
13. [Run it and watch each piece work](#13-run-it-and-watch-each-piece-work)
14. [A path to go deeper](#14-a-path-to-go-deeper)

---

## 1. The one idea the whole system is built on

If you remember nothing else, remember this:

> **The PostgreSQL database is both the source of truth *and* the work queue.**

When a request arrives, we write a row to Postgres with `status = 'pending'` and immediately tell the
client "got it." A separate background **worker** later reads pending rows and does the slow work
(actually sending the email/SMS/webhook). Everything else in Nimbus — retries, the dead-letter queue,
idempotency, circuit breakers — exists to make that two-step dance correct and reliable.

This is called the **transactional outbox pattern**. The name will make sense by the end of §5.

Everything branches from this single decision. Keep it in mind as you read.

---

## 2. What problem are we actually solving?

"Send a notification" sounds trivial. It is not, and understanding *why* is the foundation for every
design choice below.

Imagine the naive version — send the email directly inside the HTTP handler:

```go
func CreateNotification(w, r) {
    email := parse(r)
    sendEmailViaSES(email)   // ❌ what could go wrong?
    w.WriteHeader(201)
}
```

Four hard truths break this:

1. **The network is unreliable.** SES might take 8 seconds, or time out. Your client is now blocked
   for 8 seconds, or gets a 500 even though the email might actually have been sent.
2. **Downstream providers fail.** If SES has an outage, *every* request fails. You have no record of
   what you were supposed to send, so you can't retry later.
3. **Clients retry.** When a client gets a timeout, it retries — and now you've sent the same email
   twice. At scale, "twice" becomes "the same user got 5 password-reset emails."
4. **You have many tenants.** One tenant looping a buggy script can exhaust your SES quota and starve
   everyone else.

Nimbus's job is to absorb all four. The solution to (1) and (2) is **decouple acceptance from
delivery**: persist the request durably, acknowledge fast, deliver asynchronously with retries. The
solution to (3) is **idempotency**. The solution to (4) is **per-tenant rate limiting**. Those three
ideas drive most of the codebase.

---

## 3. How the code is laid out (Go project anatomy)

Two top-level folders matter:

- **`cmd/`** holds *entry points* (programs you can run). Each subfolder is one binary.
  - [cmd/gateway](../cmd/gateway/main.go) — the main server (HTTP + gRPC + worker).
  - [cmd/migrator](../cmd/migrator/main.go) — a one-shot program that applies SQL migrations.
- **`internal/`** holds the actual logic, split into packages by responsibility. Go enforces that
  `internal/` can only be imported by code in this repo — it's private by language rule.

| Package | Owns |
|---|---|
| [internal/api](../internal/api) | HTTP/REST handlers + middleware |
| [internal/grpc](../internal/grpc) | gRPC server + auth interceptors |
| [internal/worker](../internal/worker) | the delivery layer: DB-poll loop, SQS consumer, shared Dispatcher, and the channel senders (SES/SNS/webhook) |
| [internal/circuitbreaker](../internal/circuitbreaker) | the failure-isolation state machine |
| [internal/db](../internal/db) | the connection pool, models, and all SQL |
| [internal/redis](../internal/redis) | idempotency + rate limiting |
| [internal/sqs](../internal/sqs) | the SQS transport — producer (enqueue) + consumer (batch receive/delete) |
| [internal/ai](../internal/ai) | LLM "compose" + content enrichment |
| [internal/rag](../internal/rag) | embeddings, vector search, reranking, safety guard |
| [internal/config](../internal/config) | reads environment variables into a `Config` struct |
| [internal/observ](../internal/observ) · [internal/metrics](../internal/metrics) | logging + Prometheus |

### The "composition root"

[cmd/gateway/main.go](../cmd/gateway/main.go) is where everything is wired together. It reads config,
opens the DB pool, constructs each component, injects dependencies, and starts the servers. This is a
deliberate pattern: **nothing constructs its own dependencies; they're passed in.** That's *dependency
injection*, and it's what makes every package testable in isolation.

### Why each package defines its own interface

You'll notice the gRPC server defines its *own* small `NotificationRepository` interface listing only
the two methods it uses, even though the real [Repository](../internal/db/repository.go) has a dozen
methods:

```go
// internal/grpc/server.go
type NotificationRepository interface {
    CreateNotification(ctx context.Context, notif *db.Notification) error
    GetNotification(ctx context.Context, id uuid.UUID) (*db.Notification, error)
}
```

**Why:** in Go, interfaces are satisfied implicitly. By depending on a *narrow* interface instead of
the concrete `*db.Repository`, the gRPC package (a) can't accidentally call methods it shouldn't, and
(b) is trivial to test with a 10-line mock. This is the *Interface Segregation Principle*. The cost is
a little duplication (the same method signatures appear in a few places). It's worth it.

---

## 4. The data model — everything starts here

Reading the schema first makes the rest of the code obvious, because every function is just moving
rows between the states defined here. The schema lives in
[migrations/001_create_notifications.up.sql](../migrations/001_create_notifications.up.sql).

### The `notifications` table

```sql
CREATE TABLE notifications (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID NOT NULL,      -- which customer owns this
    user_id       UUID NOT NULL,      -- who it's for
    channel       VARCHAR(20) NOT NULL,  -- 'email' | 'sms' | 'webhook'
    payload       JSONB NOT NULL,        -- channel-specific data (to, subject, body…)
    status        VARCHAR(20) NOT NULL DEFAULT 'pending',
    attempt       INT NOT NULL DEFAULT 0, -- how many delivery tries so far
    error_message TEXT,                   -- last failure reason
    next_retry_at TIMESTAMPTZ,            -- don't retry before this time
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

The Go mirror of this row is [db.Notification](../internal/db/models.go).

### `status` is the spine of the whole system

Every notification walks a small state machine. *This is the single most important thing to
internalize* — almost all the code is just transitioning rows between these states:

```mermaid
stateDiagram-v2
    [*] --> pending: API/gRPC creates it
    pending --> processing: worker claims it
    processing --> sent: delivery succeeded
    processing --> pending: failed, attempt < 5 (wait, then retry)
    processing --> dead_lettered: failed, attempt = 5 (give up)
    sent --> [*]
    dead_lettered --> [*]
```

- `pending` — written, not yet delivered. The worker looks for these.
- `processing` — a worker has claimed it and is sending right now.
- `sent` — done. Terminal.
- `dead_lettered` — failed too many times; parked in the DLQ table for a human. Terminal.

### The index that makes the worker cheap

```sql
CREATE INDEX idx_notifications_retry
ON notifications(status, next_retry_at, created_at)
WHERE status IN ('pending', 'processing');
```

That `WHERE` clause makes it a **partial index** — it only contains rows that are `pending` or
`processing`. Once a notification is `sent`, it drops *out* of the index. So even if you have 50
million `sent` rows, the worker's "find me pending work" query scans an index containing only the few
thousand rows that actually need attention. This is the difference between a query that stays fast
forever and one that degrades as the table grows.

### The dead-letter table

[migrations/002](../migrations/002_create_dead_letter_queue.up.sql) adds
`dead_letter_notifications`: a copy of a notification that exhausted its retries, plus `attempts` and
`last_error`. It's a separate table (not just a status) so operators can inspect failures, retry them,
or discard them without scanning the giant main table.

**Why a relational DB at all (vs. a queue like Kafka/SQS as the primary store)?** Because we need to
*query* state ("show me this tenant's failed notifications", "how many are pending") and update
individual records. Queues are great at FIFO delivery but terrible at "give me row X and change its
status." Postgres gives us durable storage, rich queries, transactions, *and* — as the next section
shows — a usable queue, all in one system we already operate. The tradeoff: a single Postgres won't
match Kafka's throughput ceiling (millions/sec), but we're nowhere near that.

---

## 5. The write path: `POST /v1/notifications` line by line

Now we follow one request through the system. The handler is
[CreateNotification](../internal/api/handler.go).

### Step 0 — Middleware runs first

Before the handler, the request passes through the chi middleware stack wired in
[main.go](../cmd/gateway/main.go): request-ID tagging, panic recovery, a 30-second timeout, Prometheus
timing, and — the important one — **rate limiting**
([RateLimitMiddleware](../internal/api/middleware.go)). If the tenant is over budget, the request dies
here with `429 Too Many Requests` and never reaches the handler. (How the limiter itself works is §8.)

### Step 1 — Parse and validate

```go
dec := json.NewDecoder(r.Body)
dec.DisallowUnknownFields()        // reject typos like {"tennant_id": …}
if err := dec.Decode(&req); err != nil { … 400 … }
```

`DisallowUnknownFields()` is a small but real choice: it makes the API strict, so a client that
misspells a field gets a clear `400` instead of silently having the field ignored. Then we check the
required fields are present, the channel is one of `email|sms|webhook`, the payload is valid JSON, and
the `tenant_id`/`user_id` parse as UUIDs. **Validation happens before any expensive work** — fail
cheap, fail early.

### Step 2 — Idempotency check (the anti-duplicate guard)

This is the answer to "clients retry." The full mechanism is explained in §8; here's its role in the
flow:

```go
if idempotencyKey == "" && h.idempotency != nil {
    idempotencyKey = generateContentHash(req)   // hash(tenant|user|channel|payload)
}
cachedResult, err := h.idempotency.CheckOrReserve(ctx, req.TenantID, idempotencyKey)
```

Three outcomes:
- **Seen before and finished** → we return the *original* response (with header
  `X-Idempotency-Replayed: true`). No second notification is created.
- **Identical request currently in flight** → `409 Conflict`.
- **New** → we "reserve" the key (claim it) and continue.

If the client sent an `Idempotency-Key` header we use it (retained 24h); otherwise we *derive* a key
from the content (retained 5 min) to absorb accidental network retries.

### Step 3 — The durable write (the "outbox")

```go
notif := &db.Notification{ ID: uuid.New(), …, Status: db.StatusPending, Attempt: 0 }
if err := h.repo.CreateNotification(ctx, notif); err != nil {
    h.idempotency.Release(ctx, …)    // ← undo the reservation on failure
    … 500 …
}
```

This single `INSERT` ([repository.CreateNotification](../internal/db/repository.go)) is the
**durability boundary**. The moment it commits, the notification is safe — even if the process crashes
one nanosecond later, the row is in Postgres and the worker will deliver it. *This is why we can
honestly return `201` so quickly: we're not promising the email was sent, we're promising it's been
durably recorded and will be.*

Notice the `Release()` on the failure path. We took a lock in Step 2 (reserved the idempotency key).
If the DB write fails, we must hand that lock back, or the key stays "in progress" for 5 minutes and
the client's retry gets a false `409`. **Any lock you take, you release on every error path.** This is
the kind of detail that separates a toy from a real service.

### Step 4 — Enqueue to SQS (the hot path's trigger)

```go
if h.producer != nil {
    if _, err := h.producer.Enqueue(ctx, notif); err != nil {
        h.logger.Warn("sqs enqueue failed; relying on DB-poll delivery", …)
        // NOTE: we do NOT fail the request
    }
}
```

Enqueuing to SQS is what lets the **SQS consumer** (§6) pick the work up in milliseconds instead of
waiting for the next DB poll. But — and this is the heart of the outbox pattern — **the durable row in
Step 3 is the source of truth.** If the enqueue fails (or SQS is down), we log a warning and still
return `201`, because the DB-poll backstop (§6) will deliver it anyway. We never fail a write because
a queue hiccupped, and we never treat the queue as a parallel truth.

> **The "transactional outbox", finally named.** The classic distributed-systems bug is the
> *dual write*: write to the DB **and** publish to a queue, as two separate operations. If the second
> fails after the first succeeds, your DB and queue disagree forever. The outbox pattern avoids this:
> write **only** to the DB; treat the DB row itself as the outgoing message; have a separate process
> read the DB and forward to the queue/provider. Nimbus's worker *is* that process. SQS is downstream
> of the truth, never a parallel truth.

### Step 5 — Store result & respond

We cache the response under the idempotency key (so a retry replays it) and return `201 {"id": …}`.

**Why / Alternatives / Tradeoff for the write path**
- *Why:* durability + fast ack + dedup, with no dependency on the queue being up.
- *Alternative:* send inline (rejected — §2). Or dual-write to DB+queue (rejected — split-brain).
- *Tradeoff:* delivery is *eventually* done, not instant. For notifications, a few seconds of latency
  is fine; in exchange we get reliability and the ability to retry.

---

## 6. The async path: hybrid delivery

Delivery runs on **two paths that share one atomic claim guard**, both started as goroutines in
[main.go](../cmd/gateway/main.go):

| Path | Started as | Claim | Role |
|---|---|---|---|
| **SQS consumer** | `go sqsConsumer.Start(ctx)` | `ClaimNotificationByID` (one row) | Hot path — sub-second dispatch |
| **DB-poll worker** | `go w.Start(ctx)` | `ClaimPendingNotifications` (batch) | Backstop — delayed retries + anything SQS dropped |

The DB-poll worker's whole job is the loop in [worker.go](../internal/worker/worker.go). When SQS is
configured it polls every **30s** (a relaxed backstop); without SQS it stays the primary path at 5s:

```go
func (w *Worker) Start(ctx context.Context) {
    ticker := time.NewTicker(w.config.PollInterval) // 30s backstop when SQS is on, else 5s
    for {
        select {
        case <-ctx.Done():        // graceful shutdown
            return
        case <-ticker.C:
            w.processBatch(ctx)   // claim a batch, dispatch each row
        }
    }
}
```

### The claim query — where correctness is won or lost

`processBatch` calls [ClaimPendingNotifications](../internal/db/repository.go). This is the most
important query in the codebase, so we'll dwell on it.

**The bug it avoids.** The obvious implementation is two statements: `SELECT` the pending rows, then
`UPDATE` them to `processing`. But picture two worker replicas (you run several for availability) both
polling at the same instant. Both `SELECT` the *same* rows before either `UPDATE`s. Now both deliver
those notifications. The user gets duplicates. This is a classic **read-then-write race**.

**The fix** — do the claim in *one atomic statement*:

```sql
UPDATE notifications
SET status = 'processing', updated_at = NOW()
WHERE id IN (
    SELECT id FROM notifications
    WHERE (status = 'pending' AND (next_retry_at IS NULL OR next_retry_at <= NOW()))
       OR (status = 'processing' AND updated_at < NOW() - ($2 * INTERVAL '1 second'))
    ORDER BY created_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED        -- ← the magic
)
RETURNING id, tenant_id, …;
```

Two clauses make this work:

- **`FOR UPDATE`** locks each selected row for the duration of the statement.
- **`SKIP LOCKED`** says: *if a row is already locked by another transaction, don't wait — skip it.*

Together they mean: when ten worker replicas run this simultaneously, Postgres hands each one a
**disjoint** set of rows. No two workers ever claim the same notification, with **zero coordination**
— no Redis lock, no leader election, no ZooKeeper. The database is the coordinator. This is the
canonical "queue on top of Postgres" technique (the same approach used by job libraries like River,
Que, and GoodJob).

**The crash-recovery clause.** The second half of the `WHERE` —
`OR (status = 'processing' AND updated_at < NOW() - interval)` — reclaims rows stuck in `processing`
for more than 5 minutes ([stuckProcessingTimeout](../internal/db/repository.go)). If a worker claims a
row and then crashes mid-send, that row would otherwise be orphaned in `processing` forever. This
clause lets a healthy worker pick it back up. **The system self-heals from worker crashes.**

> One subtle, real bug that's already been fixed here: the interval is passed as
> `$2 * INTERVAL '1 second'` with an integer number of seconds, *not* as Go's `Duration.String()`
> ("5m0s"). Postgres reads a bare `m` in an interval string as **months**, so "5m0s" would mean
> "5 months" — a nasty silent bug. Passing seconds avoids it entirely.

### Processing one notification

Both paths hand each claimed row to the **same** [Dispatcher.Dispatch](../internal/worker/worker.go),
so send / retry / dead-letter behavior is identical no matter how the row was claimed:

```go
err := d.sender.Send(ctx, notif)   // try to deliver (via circuit breaker — §7)
newAttempt := notif.Attempt + 1
if err != nil {
    if newAttempt >= d.maxRetries {        // 5
        d.repo.MoveToDeadLetter(ctx, notif, err.Error())
    } else {
        nextRetry := d.calculateNextRetry(newAttempt) // 1m, 5m, 15m
        d.repo.UpdateNotificationStatus(ctx, notif.ID, "pending", newAttempt, &errMsg, &nextRetry)
    }
} else {
    d.repo.UpdateNotificationStatus(ctx, notif.ID, "sent", newAttempt, nil, nil)
}
```

- **Success** → `sent`. Done.
- **Failure, tries left** → back to `pending`, but stamp `next_retry_at = now + backoff`. The claim
  query's `next_retry_at <= NOW()` check means the row won't be re-picked until the delay elapses.
  Backoff is `1m → 5m → 15m` ([calculateNextRetry](../internal/worker/worker.go)) — **exponential
  backoff** so a struggling provider isn't hammered.
- **Failure, out of tries** → dead-letter it.

The row was already marked `processing` by the atomic claim, so there's no separate "mark as
processing" write here — that would reintroduce a race. Claim *is* the mark.

### The SQS consumer path

The [SQSConsumer](../internal/worker/sqs_consumer.go) runs `Receivers` concurrent long-poll loops.
Each pulls a batch of up to 10 messages ([internal/sqs/consumer.go](../internal/sqs/consumer.go)) and
processes them concurrently (bounded by `Concurrency`). Per message it:

1. Parses the notification id (a malformed id is a poison message → ack and drop).
2. Calls [ClaimNotificationByID](../internal/db/repository.go) — the single-row twin of the batch
   claim. If the row is already `sent`, already claimed by the poller, or a retry that isn't due yet,
   the claim returns "not claimable" and the consumer simply **acks** the message (no re-send).
3. On a winning claim, hands the row to the shared `Dispatcher`.
4. **Always acks after the first attempt** (an SQS message is one-shot). If the send failed but has
   retries left, `Dispatch` schedules it back to `pending` with a future `next_retry_at`, and the
   **DB-poll backstop** picks it up when due.

> **The no-double-send guarantee.** Two delivery paths run concurrently, but the atomic DB claim is
> the single source of dedup truth: whichever path wins the `pending → processing` transition sends
> the row; the loser sees zero rows and skips. On a transient DB error the consumer does *not* ack, so
> the message reappears after the SQS visibility timeout and is retried safely.

### Dead-lettering is transactional

[MoveToDeadLetter](../internal/db/repository.go) does two writes — insert into the DLQ table **and**
flip the original's status to `dead_lettered` — inside a single `BEGIN/COMMIT`:

```go
tx, _ := r.db.Pool().Begin(ctx)
defer tx.Rollback(ctx)            // rolls back unless we Commit
tx.QueryRow(ctx, insertDLQ, …)
tx.Exec(ctx, updateOriginal, …)
tx.Commit(ctx)
```

**Why a transaction:** without it, a crash between the two writes could leave a notification that's in
the DLQ *and* still `processing` (so it'd get re-sent), or flipped to `dead_lettered` with no DLQ
record (lost forever). The transaction makes the pair atomic: both happen or neither does. The
`defer tx.Rollback` is a Go idiom — `Rollback` after a successful `Commit` is a harmless no-op, so
this guarantees we never leak an open transaction on any error path.

**Why / Alternatives / Tradeoff for delivery**
- *Why hybrid:* the SQS consumer gives sub-second latency for the common case, while the DB-poll
  backstop guarantees eventual delivery of retries and anything SQS drops — all deduped by one claim.
- *Alternative:* Postgres `LISTEN/NOTIFY` (push instead of poll). Rejected because it needs a
  dedicated, non-pooled DB connection per listener and doesn't survive reconnects gracefully.
- *Tradeoff:* two delivery paths to reason about. The shared atomic claim keeps them safe, and the
  poller interval is relaxed to 30s when SQS is carrying the load.

---

## 7. Delivery: senders, routing, and circuit breakers

### The `Sender` interface (Strategy pattern)

All three channels implement one interface ([senders.go](../internal/worker/senders.go)):

```go
type Sender interface {
    Send(ctx context.Context, notif *db.Notification) error
    SupportsChannel(channel string) bool
}
```

Concrete implementations: [SESSender](../internal/worker/ses_sender.go) (email),
[SNSSender](../internal/worker/sns_sender.go) (SMS),
[WebhookSender](../internal/worker/webhook_sender.go) (HTTP POST). This is the **Strategy pattern** —
the worker doesn't know or care *how* a channel delivers; it just calls `Send`. Adding a 4th channel
(say, Slack) is a new struct implementing two methods, with zero changes to the worker.

### `MultiSender` — the router

[MultiSender](../internal/worker/senders.go) holds all the senders and routes by channel:

```go
func (m *MultiSender) Send(ctx, notif) error {
    for _, sender := range m.senders {
        if sender.SupportsChannel(notif.Channel) {
            return sender.Send(ctx, notif)
        }
    }
    return fmt.Errorf("no sender found for channel: %s", notif.Channel)
}
```

The worker only ever holds *one* `Sender` (this router). Clean.

### Circuit breakers — failing fast when a provider is down

Here's the scenario the circuit breaker exists for: SES goes down. Without protection, every one of
your worker's send attempts waits for SES to time out (say 10s), fails, retries, waits again… Your
workers are now all blocked waiting on a dead service, and healthy channels (SMS, webhook) get starved
because the workers are tied up. One provider's outage **cascades** into a total outage.

A **circuit breaker** is a tiny state machine that wraps a flaky dependency and "trips" when it
detects trouble — exactly like an electrical breaker. The implementation is
[circuitbreaker.go](../internal/circuitbreaker/circuitbreaker.go). Three states:

```mermaid
stateDiagram-v2
    [*] --> Closed
    Closed --> Open: 5 consecutive failures
    Open --> HalfOpen: after 30s recovery timeout
    HalfOpen --> Closed: probe request succeeds
    HalfOpen --> Open: probe request fails
```

- **Closed** = healthy. Requests pass through. We count consecutive failures.
- **Open** = tripped. After 5 failures (`MaxFailures`), we flip to Open and **reject requests
  instantly** without even calling SES (`return ErrCircuitOpen`). This is "failing fast" — we stop
  wasting time on a service we know is down.
- **HalfOpen** = testing recovery. After 30s (`RecoveryTimeout`) we allow *one* probe request through.
  If it succeeds → back to Closed (recovered). If it fails → back to Open (still down).

The state is guarded by a mutex (`sync.RWMutex`) because multiple worker goroutines call it
concurrently. `Allow()`, `RecordSuccess()`, and `RecordFailure()` are the three entry points, and they
lock the state while reading/transitioning it.

### `ProtectedSender` — the Decorator that glues them together

How does the breaker wrap a sender without the sender knowing? The **Decorator pattern**
([protected_sender.go](../internal/circuitbreaker/protected_sender.go)):

```go
func (p *ProtectedSender) Send(ctx, notif) error {
    if !p.breaker.Allow() {                    // circuit open?
        return ErrCircuitOpen                   // fail fast, never touch SES
    }
    err := p.sender.Send(ctx, notif)            // call the real sender
    if err != nil {
        p.breaker.RecordFailure()
        return err
    }
    p.breaker.RecordSuccess()
    return nil
}
```

`ProtectedSender` *is* a `Sender` (same interface) that happens to wrap another `Sender`. So in
[main.go](../cmd/gateway/main.go) each real sender is wrapped before being handed to the
`MultiSender`:

```go
protectedEmail := circuitbreaker.NewProtectedSender(sesSender, sesBreaker, logger)
```

**Crucially, each channel gets its own breaker.** An SES outage trips only the email breaker; SMS and
webhook keep flowing. Failures are *isolated*, not shared.

You can watch this live: `GET /v1/health/circuits` returns every breaker's current state and counters.

**Why / Alternatives / Tradeoff**
- *Why:* isolate downstream failures so one bad provider can't take down the whole worker fleet.
- *Alternative:* just rely on retries + timeouts. Rejected — under a real outage that pegs every
  worker at the timeout and cascades.
- *Tradeoff:* while a breaker is Open, even requests that *might* have succeeded are rejected for up to
  30s. We accept a few false negatives in exchange for not melting down.

---

## 8. Reliability primitives: idempotency & rate limiting

Both live in [internal/redis](../internal/redis) and both lean on a single property of Redis: it's a
fast, single-threaded, in-memory data store where individual operations are atomic.

### Idempotency ([idempotency.go](../internal/redis/idempotency.go))

"Idempotent" means *doing it twice has the same effect as doing it once.* The goal: a client that
retries a create request must not produce two notifications.

The mechanism is a key in Redis per `(tenant, idempotency-key)`, holding one of two things:
- the marker string `"processing"` — a request with this key is in flight, **or**
- the JSON of the original response — a request with this key already finished.

The entry point is `CheckOrReserve`:

```go
func (s *IdempotencyService) CheckOrReserve(ctx, tenantID, key) (*IdempotencyResult, error) {
    result, _ := s.Check(...)              // GET the key
    if result != nil { return result, nil } // finished before → replay it
    reserved, _ := s.Reserve(...)          // try to claim it
    if !reserved { return nil, ErrDuplicateRequest } // someone else is mid-flight → 409
    return nil, nil                        // we own it → proceed
}
```

`Reserve` is where the atomicity lives:

```go
set, _ := s.client.rdb.SetNX(ctx, key, processingMarker, processingTTL).Result()
```

`SetNX` = "**SET** if **N**ot e**X**ists." It's atomic: of N concurrent identical requests, *exactly
one* `SetNX` returns true. That one proceeds; the rest see the key already exists and get `409`. No
lock, no race. The `processingTTL` (5 min) means a crashed request can't hold the key forever.

**Releasing the reservation safely.** Recall from §5 that a failed DB write must release the key. But
there's a subtlety: by the time we release, another request might have legitimately *completed* and
overwritten the key with a real result. We must not delete *that*. The solution is a compare-and-delete
done atomically with a Lua script:

```lua
if redis.call("GET", KEYS[1]) == ARGV[1] then   -- only if it's still "processing"
    return redis.call("DEL", KEYS[1])
end
return 0
```

Redis runs the whole script atomically, so "check it's still the marker, then delete" can't be
interrupted. `Release` is therefore safe to call on any error path — it's a no-op if a real result is
already stored.

**Two TTLs, on purpose.** Client-provided keys live 24h (the client explicitly controls dedup, à la
Stripe). Auto-derived content-hash keys live 5 min (just long enough to absorb network retries,
without blocking an intentional re-send of the same content later).

### Rate limiting ([ratelimit.go](../internal/redis/ratelimit.go))

Goal: cap each tenant at 100 requests / 60 seconds, *fairly* — no nasty bursts at minute boundaries.

The naive "fixed window" (a counter that resets every minute) has a flaw: a client can send 100
requests at 11:00:59 and another 100 at 11:01:00 — 200 requests in one second, because the window
reset between them. Nimbus uses a **sliding window** instead, built on a Redis **sorted set** (a set
where every member has a numeric score):

```go
windowStart := now.Add(-r.config.Window)              // 60s ago
pipe.ZRemRangeByScore(ctx, key, "0", windowStart)     // 1. drop entries older than the window
countCmd := pipe.ZCard(ctx, key)                      // 2. count what remains
// …if count + n > limit → reject…
pipe2.ZAdd(ctx, key, redis.Z{Score: now, Member: …}) // 3. record this request at score=now
pipe2.Expire(ctx, key, window+time.Second)            // 4. let idle keys self-clean
```

We store each request's timestamp as the score. To check the limit we (1) evict everything older than
60 seconds, (2) count what's left, (3) if there's room, add the new request. Because the window is
always "the last 60 seconds relative to *now*," there's no boundary to game. The `Expire` ensures a
tenant who goes quiet has their key garbage-collected automatically.

The HTTP glue is [RateLimitMiddleware](../internal/api/middleware.go), which also sets the standard
`X-RateLimit-*` and `Retry-After` response headers, and **fails open** — if Redis itself is
unreachable, it logs and lets the request through rather than taking the whole API down over a rate
limiter.

**Why / Alternatives / Tradeoff**
- *Why Redis:* sub-millisecond atomic ops, shared across all API replicas, with TTL-based cleanup.
- *Alternative for idempotency:* a unique constraint in Postgres. Works, but bloats the schema, needs
  cleanup of old keys, and costs a DB round-trip on the hot path. Redis is faster and self-expiring.
- *Alternative for rate limiting:* fixed-window counter (simpler, but allows 2× bursts) or a token
  bucket. Sliding-window sorted set is the accuracy/complexity sweet spot.
- *Tradeoff:* Redis becomes a dependency — so both features are written to **degrade gracefully** when
  it's down rather than fail the request.

---

## 9. gRPC — what it is and why it's here

This is the part you specifically wanted from the ground up, so we'll start with zero assumptions.

### What is RPC, gRPC, and Protobuf?

**RPC** = *Remote Procedure Call*. The idea: calling a function on another server should feel like
calling a local function. You call `client.GetNotification(id)`, and under the hood it sends a network
request to a server, which runs the real function and sends back the result.

**REST** (what the HTTP API uses) is one style of doing this: you model everything as *resources* with
*URLs* (`GET /v1/notifications/123`) and pass JSON. It's universal and human-readable.

**gRPC** is a different style, from Google. Instead of URLs + JSON, you:
1. Write a **contract** describing your service's methods and message shapes in a `.proto` file.
2. Run a compiler that **generates** client and server code in your language from that contract.
3. Messages travel as **Protocol Buffers** (Protobuf) — a compact *binary* format — over **HTTP/2**.

**Protobuf** is the binary encoding. Where JSON sends `{"tenant_id":"abc","attempt":3}` as text (every
field name spelled out, every value a string), Protobuf sends field *numbers* and packed binary. It's
roughly 5× smaller on the wire and faster to parse, at the cost of not being human-readable.

### Why does Nimbus have *both* REST and gRPC?

They serve different audiences:

| | REST (`:8080`) | gRPC (`:9090`) |
|---|---|---|
| Audience | external clients, browsers, third parties | our own internal services |
| Encoding | JSON (readable, universal) | Protobuf (compact, typed) |
| Killer feature | works everywhere, curl-able | **server streaming** + strong typing |

The decisive reason gRPC earns its place is **server streaming** — something REST can't do cleanly. We
get to that below. The cost of supporting both is running two listeners; we accept it for the clean
split between "public front door" and "internal hallway."

### The contract: [notification.proto](../proto/notification/v1/notification.proto)

You hand-write this. It declares three methods:

```protobuf
service NotificationService {
  rpc CreateNotification(CreateNotificationRequest) returns (CreateNotificationResponse);
  rpc GetNotification(GetNotificationRequest) returns (Notification);
  rpc StreamDeliveryUpdates(StreamDeliveryUpdatesRequest) returns (stream DeliveryUpdate);
}
```

Note the `stream` keyword on the third one. Two of these are **unary** (one request → one response,
like a normal function). The third is **server-streaming** (one request → *many* responses over time).

Messages define the typed payloads, with numbered fields (the `= 1`, `= 2` are the Protobuf field tags
that get encoded instead of names):

```protobuf
message CreateNotificationRequest {
  string tenant_id = 1;
  string user_id   = 2;
  string channel   = 3;
  bytes  payload   = 4;
}
```

### The generated code (don't edit it)

Running `protoc` against the `.proto` generates two files in
[proto/notification/v1](../proto/notification/v1): `notification.pb.go` (the message structs + their
binary encode/decode) and `notification_grpc.pb.go` (the client/server interfaces and the dispatch
glue). The command:

```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/notification/v1/notification.proto
```

The generated server side gives us three things we use: a `NotificationServiceServer` interface (one
Go method per RPC), an `UnimplementedNotificationServiceServer` stub, and a
`RegisterNotificationServiceServer` function.

### Our implementation: [server.go](../internal/grpc/server.go)

```go
type Server struct {
    notificationv1.UnimplementedNotificationServiceServer  // embedded
    repo   NotificationRepository
    logger *zap.Logger
}
```

**Why embed `UnimplementedNotificationServiceServer`?** Forward compatibility. It provides default
"not implemented" versions of every RPC. So if you later add a 4th method to the `.proto` and
regenerate, this struct *still compiles* — you implement the new method when ready instead of the build
breaking immediately.

Each method follows the same shape: **get tenant from the authenticated context → validate → call repo
→ map DB model to Protobuf** (via the `toProto` helper). The security rule is the same as everywhere:
the tenant comes from the auth token, never from the request body.

### Authentication: interceptors ([interceptor.go](../internal/grpc/interceptor.go))

An **interceptor** is gRPC's version of HTTP middleware — code that runs before every RPC. Its job: read
the auth token and put the tenant into the request context so handlers can trust it.

gRPC needs two interceptor types because unary and streaming have different signatures:
`AuthInterceptor` (unary) and `StreamAuthInterceptor` (streaming). Both call the shared
`extractAndValidate`, which pulls the `authorization` header from gRPC **metadata** (the gRPC analog of
HTTP headers), strips `Bearer `, and looks the token up to find the tenant.

The streaming case has a neat wrinkle worth understanding. A stream's context is read-only — you can't
attach a value to it directly the way you can in the unary path. So the code wraps the stream:

```go
type wrappedServerStream struct {
    grpc.ServerStream             // embeds the real stream — Send/Recv inherited
    ctx context.Context           // …but we override Context()
}
func (w *wrappedServerStream) Context() context.Context { return w.ctx }
```

Now the streaming handler reads the tenant the exact same way (`TenantIDFromContext`) as the unary one.

### The streaming RPC — why it's the whole point

`StreamDeliveryUpdates` lets a caller watch a notification's status change in real time. Compare two
ways to do that:

- **Client polling (REST):** the client calls `GET /notifications/123` every 2 seconds asking "done
  yet? done yet?" That's a request every 2s *whether or not anything changed*.
- **Server streaming (gRPC):** the client opens **one** stream and the *server* pushes an update
  whenever the status changes, then closes the stream when it reaches a terminal state.

The server side ([server.go](../internal/grpc/server.go)) is a loop on a 2-second ticker that reads the
row and pushes:

```go
ticker := time.NewTicker(2 * time.Second)
for {
    select {
    case <-ctx.Done():                       // client hung up
        return ctx.Err()
    case <-ticker.C:
        notif, _ := s.repo.GetNotification(ctx, id)
        stream.Send(&DeliveryUpdate{ Status: notif.Status, … })  // push
        if terminalStates[notif.Status] {    // sent / failed / dead_lettered
            return nil                        // close the stream
        }
    }
}
```

Ownership is checked **once** before the loop opens (so a caller can't stream another tenant's
notification by guessing an ID), and the stream auto-closes on a terminal state so neither side has to
time out. On a high-volume system this replaces thousands of poll requests with a handful of pushed
updates.

> **Wiring recap** ([main.go](../cmd/gateway/main.go)): create the gRPC server with the two
> interceptors attached, `RegisterNotificationServiceServer` our implementation, `net.Listen` on
> `:9090`, and `grpcServer.Serve(listener)` in a goroutine so it runs alongside HTTP. On shutdown,
> `grpcServer.GracefulStop()` drains in-flight RPCs (important for those long-lived streams) before
> exit.

**Why / Alternatives / Tradeoff**
- *Why gRPC at all:* typed contracts + binary efficiency for internal callers, and streaming for live
  updates without polling.
- *Alternative:* REST + WebSockets for the streaming need. Rejected — gRPC gives streaming, codegen,
  and typing in one consistent tool for service-to-service.
- *Tradeoff:* a second protocol/port to operate, and Protobuf isn't curl-friendly (you need
  `grpcurl`). Worth it for the internal path.

---

## 10. The AI / RAG subsystem

This part is optional — it only activates if `OPENAI_API_KEY` is set. Two capabilities: **compose**
(natural language → notifications) and **ask** (question answering grounded in your own data). The
interesting one is *ask*, which is a **RAG** pipeline.

### What problem RAG solves

An LLM like GPT only knows what it was trained on — public internet text up to some cutoff. It knows
*nothing* about your notification history. If you ask it "did my order email to Alice get delivered?"
it will *hallucinate* a confident, plausible, wrong answer.

**RAG** (Retrieval-Augmented Generation) fixes this by *retrieving* the relevant facts from your data
first, then handing them to the LLM as context with an instruction: "answer using only these
documents." The answer becomes grounded and verifiable (with citations) instead of invented.

### What is an embedding / a vector?

To "retrieve relevant facts," we need to find text by *meaning*, not just keywords. An **embedding** is
a list of numbers (here, 1536 of them) that represents the *meaning* of a piece of text, produced by a
model (OpenAI's `text-embedding-3-small`). Texts with similar meaning have vectors that are close
together in 1536-dimensional space. "delivery failed" and "the message was not received" end up near
each other even though they share no words. We store these vectors in Postgres using the **pgvector**
extension, in the [knowledge_base](../migrations/003_add_pgvector.up.sql) table.

### The pipeline ([pipeline.go](../internal/rag/pipeline.go))

A query flows through 8 steps:

1. **Injection guard** — reject prompt-injection attempts (below).
2. **PII mask** — replace emails/phones/SSNs with placeholders *before* anything leaves our process.
3. **Embed** — turn the query into a 1536-dim vector.
4. **Hybrid search** — find the top-20 candidate documents (below).
5. **Rerank** — narrow 20 → the best 5 ([reranker.go](../internal/rag/reranker.go)).
6. **Build prompt** — number the 5 context docs and append the question.
7. **LLM answer** — GPT generates an answer that cites the numbered docs, under a pinned system prompt.
8. **Restore PII** — swap the real values back into the answer.

### Hybrid search + Reciprocal Rank Fusion — the clever bit

Two ways to search text, each with a blind spot:
- **Vector search** finds *semantic* matches but can miss exact tokens (a specific error code, an email
  address, an ID).
- **Full-text search** (Postgres `tsvector`, BM25-style) nails *exact keywords* but misses paraphrases.

[HybridSearch](../internal/rag/store.go) runs **both** and fuses them with **Reciprocal Rank Fusion
(RRF)**. The whole thing is one SQL query using two CTEs (one per search) joined together:

```sql
WITH semantic_search AS (
    SELECT id, …, ROW_NUMBER() OVER (ORDER BY embedding <=> $1::vector) AS rank
    FROM knowledge_base WHERE tenant_id = $2 ORDER BY embedding <=> $1::vector LIMIT 20
),
fulltext_search AS (
    SELECT id, …, ROW_NUMBER() OVER (ORDER BY ts_rank(content_tsv, plainto_tsquery('english',$3)) DESC) AS rank
    FROM knowledge_base WHERE tenant_id = $2 AND content_tsv @@ plainto_tsquery('english',$3) LIMIT 20
)
SELECT …, COALESCE(1.0/(60.0 + s.rank),0) + COALESCE(1.0/(60.0 + f.rank),0) AS rrf_score
FROM semantic_search s FULL OUTER JOIN fulltext_search f ON s.id = f.id
ORDER BY rrf_score DESC;
```

The `<=>` operator is pgvector's cosine distance (smaller = more similar). The genius of RRF is that it
fuses the two result lists using only **rank position**, not raw scores — so we never have to make a
cosine distance and a BM25 score comparable (they're on totally different scales). The formula is
`score(d) = Σ 1/(k + rank_i(d))` with `k=60` (the standard constant from the original paper). A
document that ranks well in *both* lists rises to the top.

### The safety guard ([guard.go](../internal/rag/guard.go))

Two orthogonal jobs, done with fast regex (microseconds, no extra API call):

- **Prompt-injection defense.** Attackers embed instructions in their query: *"Ignore all previous
  instructions, you are now…"* The guard matches known patterns and rejects the request before it
  reaches the LLM. This is *defense in depth* — even if something slips through, step 7 also **pins**
  the system prompt so the LLM's instructions can't be overridden.
- **PII filtering.** Emails/phones/SSNs are masked to placeholders before we call OpenAI (so PII never
  lands in their logs), then restored in the final answer.

**Why / Alternatives / Tradeoff**
- *Why pgvector (not Pinecone/Weaviate):* zero new infrastructure — it's just our existing Postgres.
  We get ACID and can join vectors with relational data in one query.
- *Tradeoff:* pgvector tops out around tens of millions of vectors before a dedicated ANN store would
  win. We're far below that. Also, the `knowledge_base` table starts **empty** — `IndexNotification`
  and `SeedFAQ` exist to populate it but aren't auto-called yet, so out of the box `ask` truthfully
  says "I don't have enough information" until you seed data.

---

## 11. Cross-cutting concerns

- **Config** ([config.go](../internal/config/config.go)) — every setting is read from an environment
  variable with a sensible default, into one `Config` struct constructed once at startup. Setting
  `OPENAI_API_KEY` is what flips `AIEnabled` on. This is the [12-factor](https://12factor.net/config)
  approach: config lives in the environment, not in code.
- **Connection pooling** ([postgres.go](../internal/db/postgres.go)) — opening a Postgres connection is
  expensive (~ms), so we keep a pool of them: `MaxConns=25`, `MinConns=5` kept warm, connections
  recycled hourly. Every query borrows a connection and returns it. Without pooling, a burst of traffic
  would open hundreds of connections and exhaust the database.
- **Structured logging** ([observ](../internal/observ)) — zap logs are JSON key-value pairs, not
  free-text strings, so they're machine-queryable (`status=500 AND tenant_id=…`).
- **Metrics** ([metrics.go](../internal/metrics/metrics.go)) — Prometheus counters/histograms exposed
  at `/metrics`: request counts, latencies, notifications processed, rate-limit rejections, etc.
- **Graceful shutdown** ([main.go](../cmd/gateway/main.go)) — on SIGTERM we `GracefulStop()` the gRPC
  server (drain streams), then give HTTP 10 seconds to finish in-flight requests, then close. This is
  what lets you deploy without dropping requests.

---

## 12. The big tradeoffs, consolidated

Each row is a decision, *why* it was made, the main alternative, and the cost accepted. This is the
"defend your design" reference.

| Decision | Why | Alternative (rejected) | Cost accepted |
|---|---|---|---|
| **Transactional outbox** (DB first, queue downstream) | Durability + fast ack, no split-brain | Dual-write to DB *and* queue | DB/queue can briefly lag; worker reconciles |
| **DB-as-queue with `SKIP LOCKED`** | Exactly-once claim, horizontal scale, no extra infra | Dedicated broker (Kafka/SQS) as primary store | Throughput ceiling far below a real broker (fine for us) |
| **Polling (5s) for the worker** | Simple, crash-tolerant, scales via `SKIP LOCKED` | `LISTEN/NOTIFY` push | Up to 5s pickup latency (SQS fast-path covers it) |
| **Per-channel circuit breakers** | Isolate provider outages | Rely on timeouts/retries only | Some false rejections for ~30s while Open |
| **Idempotency in Redis** | Sub-ms atomic `SETNX`, TTL auto-expiry | DB unique constraint | Redis is a soft dependency (degrades gracefully) |
| **Sliding-window rate limit** | No boundary bursts | Fixed-window counter | A bit more Redis work per request |
| **REST + gRPC (two ports)** | Right tool per audience; streaming | One protocol for both | Two listeners to operate |
| **Server-streaming for status** | ~Eliminates polling traffic | REST polling / WebSockets | gRPC-only; needs `grpcurl` to test |
| **pgvector for RAG** | Zero new infra, ACID, SQL joins | Pinecone/Weaviate | ~10s-of-millions vector ceiling |
| **Monolith with in-process worker** | One binary to deploy; simple | Separate API + worker services | Shared fate today (but worker is split-ready) |
| **`NOT_FOUND` on cross-tenant reads** | No "which IDs exist" oracle | `PERMISSION_DENIED` | Slightly less precise legit 404s |

---

## 13. Run it and watch each piece work

The fastest way to *understand* this is to run it and watch the logs narrate the lifecycle.

```bash
cd nimbus

# Dependencies (Postgres + Redis) in Docker
docker compose up -d postgres redis

# Apply the schema
DATABASE_URL="postgres://nimbus:nimbus123@localhost:5432/nimbus?sslmode=disable" go run ./cmd/migrator

# Run the gateway (DB creds must match the Docker Postgres)
DB_USER=nimbus DB_PASSWORD=nimbus123 go run ./cmd/gateway/main.go
```

Then, in another terminal, create one notification and **watch the gateway logs**:

```bash
curl -X POST http://localhost:8080/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"00000000-0000-0000-0000-000000000001",
       "user_id":"00000000-0000-0000-0000-000000000002",
       "channel":"email","payload":{"to":"you@example.com","subject":"Hi","body":"Hello"}}'
```

What you'll observe in the logs, mapping directly to this doc:
1. `notification created` — the durable INSERT (§5, the outbox).
2. Within ~5s, the worker claims it (§6, the `SKIP LOCKED` claim).
3. Without AWS credentials the SES send *fails* — you'll see it go back to `pending` with backoff
   (§6, retries), and after 5 attempts land in the DLQ (§6, dead-lettering). **That's the full
   lifecycle working correctly** — you're just watching it without a real email provider attached.

Inspect state as it moves:

```bash
curl "http://localhost:8080/v1/notifications?tenant_id=00000000-0000-0000-0000-000000000001"
curl  "http://localhost:8080/v1/dlq?tenant_id=00000000-0000-0000-0000-000000000001"
curl  http://localhost:8080/v1/health/circuits   # watch a breaker trip after 5 failures (§7)
curl  http://localhost:8080/metrics | grep nimbus # the Prometheus counters (§11)
```

To see the integrations fully working, set `AWS_*` + `SES_FROM_EMAIL` (real email) and/or
`OPENAI_API_KEY` (compose + RAG).

---

## 14. A path to go deeper

A suggested reading order for the code itself, now that you have the map:

1. [db/models.go](../internal/db/models.go) + [migrations/001](../migrations/001_create_notifications.up.sql) — the data and its states.
2. [api/handler.go](../internal/api/handler.go) `CreateNotification` — the write path end to end.
3. [db/repository.go](../internal/db/repository.go) `ClaimPendingNotifications` + `MoveToDeadLetter` — the queue mechanics.
4. [worker/worker.go](../internal/worker/worker.go) — the loop that ties it together.
5. [circuitbreaker/circuitbreaker.go](../internal/circuitbreaker/circuitbreaker.go) — the FSM.
6. [redis/idempotency.go](../internal/redis/idempotency.go) + [ratelimit.go](../internal/redis/ratelimit.go) — the reliability primitives.
7. [grpc/server.go](../internal/grpc/server.go) + [interceptor.go](../internal/grpc/interceptor.go) — the second transport.
8. [rag/pipeline.go](../internal/rag/pipeline.go) + [store.go](../internal/rag/store.go) — the AI layer.

**Experiments that build real intuition** (the best way to internalize the tradeoffs is to break
things):
- Run **two** gateways pointed at the same DB, create many notifications, and confirm no notification
  is delivered twice — that's `SKIP LOCKED` in action.
- `kill -9` a worker mid-send and watch a healthy one reclaim the stuck `processing` row after 5
  minutes.
- Point the webhook channel at a URL that always 500s and watch the circuit breaker go
  Closed → Open → HalfOpen at `/v1/health/circuits`.
- Fire the same request 5× in one second with a fixed `Idempotency-Key` and confirm only one
  notification is created.

---

*See also: [ARCHITECTURE.md](ARCHITECTURE.md) for the system-design diagrams, and [API.md](API.md) for
the endpoint and gRPC contract reference.*
