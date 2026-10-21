# IoT telemetry pipeline: design notes (interview cheat sheet)

**One-line pitch:** *"I built an MQTT telemetry pipeline end to end: an ESP32-C3 reporting its own health,
plus a chaos simulator, into an async Python ingest worker with idempotent batched writes to TimescaleDB, and a FastAPI on top.
I verified zero loss and zero duplicates while restarting the database and the worker mid-run."*

## Architecture

```
ESP32-C3 ──┐  MQTT QoS 1, LWT + retained status
simulator ─┴──▶ Mosquitto ──▶ ingest worker ──batch──▶ TimescaleDB ◀── FastAPI (REST) ◀── clients
                               receive → bounded queue → validate → insert
```
- **Device:** samples health (chip temp, RSSI, heap, uptime). `boot_id` lives in flash, `seq` counts per boot,
  `msg_id = device-boot-seq`. Buffers in a ring buffer when offline and replays on reconnect. Last Will publishes `offline` (retained).
- **Worker:** asyncio, 3 tasks. Receive (never touches the DB) → bounded `asyncio.Queue` → writer (batch 200 messages or 1 s).
- **DB:** hypertable with 1-day chunks; `UNIQUE (msg_id, ts)`; index `(device_id, ts DESC)`; `dead_letters` for rejects.
- **API:** async FastAPI + asyncpg pool; cursor pagination; `time_bucket` aggregates; consistent error body.

## Key decisions and trade-offs

| Decision | Why | Cost / alternative |
|---|---|---|
| MQTT, not HTTP, for devices | Persistent connection (server can push commands), ~2-byte overhead, QoS, LWT, retained | HTTP is simpler and firewall-friendly; fine for rare batch uploads |
| QoS 1 + idempotent insert | At-least-once delivery + dedupe in the DB = effectively exactly-once storage | QoS 2 is slower and still isn't end-to-end (worker can crash before the commit) |
| Dedupe in the DB (`ON CONFLICT DO NOTHING`) | Durable, survives restarts, works with N workers | An in-memory or Redis TTL cache only as a fast pre-filter |
| `UNIQUE (msg_id, ts)` | Hypertable unique indexes must include the partition column (uniqueness is per chunk) | Strict global uniqueness → a separate `seen_messages` table with retention |
| Hybrid schema: fixed columns + JSONB metrics | Different device types, no ALTER per new metric, 1 row per message | JSONB queries are untyped and slower; promote hot metrics to columns |
| Store device `ts` AND server `received_at` | Device time = when measured (offline replay); server time = when we got it (clock-skew checks, lag) | |
| Reject `ts` > now+5 min or < 2024 | Allows normal drift and offline replay; blocks broken clocks (1970) | |
| Bounded queue between receive and write | Bursts are absorbed; a stuck DB slows receiving instead of exhausting memory (backpressure) | |
| Batch insert via `unnest` arrays | One round trip per batch, and still supports `ON CONFLICT` | `COPY` is faster but can't do `ON CONFLICT` (use COPY → temp table → INSERT…SELECT) |
| Transient DB errors: retry forever, backoff capped at 30 s | DB down 5 min = nothing lost, auto catch-up | Permanent errors: row-by-row + dead-letter (no poison batch) |
| Persistent MQTT session + fixed client id | Broker queues QoS 1 messages while the worker restarts | Broker queue limit (`max_queued_messages`) must be sized |
| Cursor pagination `(ts, msg_id)` | Stable while new data arrives; index seek instead of OFFSET scan | Can't jump to page N |
| Offline = LWT **and** `last_seen` timeout | LWT is fast but not guaranteed (broker node crash, device dies at the same time) | |

## Failure modes: what happens when…

| Failure | Behaviour |
|---|---|
| Duplicate delivery (QoS 1) | Same `msg_id` + `ts` → skipped by `ON CONFLICT` |
| Device offline 10 min | Ring buffer, replay on reconnect; old `ts` accepted; `seq` continuous → no gap |
| Device reboots | `boot_id` +1, `seq` restarts at 1; `reset_reason` says why (`panic` = crash) |
| Garbage payload | Pydantic rejects → `dead_letters` with the raw payload + reason |
| DB down | Writer retries the same batch; queue fills → backpressure; nothing dropped |
| Worker restarted gracefully | Drains the queue on SIGTERM; broker holds new messages (persistent session) |
| Worker `kill -9` | **Known gap:** messages already acked to the broker but still in the in-memory queue are lost. Fix: manual ack after commit (paho `manual_ack`), or Kafka with offsets committed after the write |
| Broker node crash | Retained `online` might lie → `last_seen` timeout marks the device offline |

## Scaling path (when the numbers demand it)
10k devices / 5 s ≈ 2k rows/s → one Postgres with batching is plenty.
1. More workers via **shared subscriptions** (`$share/ingest/...`). Stateless; DB dedupe keeps it correct.
2. **Broker cluster** (EMQX/HiveMQ) or AWS IoT Core behind one endpoint.
3. **Kafka** between broker and workers at large scale: replay, many consumers, per-device order via partition key.
4. DB: compression + retention policies, continuous aggregates for dashboards, **read replicas for reads** (writes
   always go to the primary), vertical scaling, then sharding (Citus) or ClickHouse at ~1M rows/s. Cold data → S3.

## Testing and delivery
- Unit tests for validation (pytest, parametrize); end-to-end correctness via simulator + `verify.py` (ground truth).
- Next: integration tests with testcontainers (real Postgres + Mosquitto); CI = ruff → mypy → pytest → docker build.
- Multi-stage Dockerfile, non-root user, dependency layer cached.

## How I used AI (honest version)
Used it for boilerplate (Docker, FastAPI skeleton, SQL syntax) and explanations. Verified with the simulator +
`verify.py`, unit tests, and reading the docs.
**TODO (you):** put 2–3 *real* examples here from your own `AI_LOG.md` or firmware work: something the AI got wrong and how you caught it.
Only use things that actually happened to you. Interviewers dig into details, and invented stories fall apart.
Generic patterns worth watching for: library APIs from an older version, blocking calls inside `async def`, defaults that
silently fail (e.g. a buffer too small for the payload), retry code that gives up and drops data.

## My production stories (Univini, 2 min each, STAR)
1. **PG 12 → 17 upgrade + replicas + PgBouncer**: dump/restore, 10 GB → 4 GB (bloat), replicas for read scaling, pooling for connections.
2. **Bull/Redis pipeline**: per-chapter chunks for granular retries, text-hash cache to cut AI API cost. Same reliability ideas: idempotency, retries, dead letters.
3. **Test suite from zero**: ~5,400 tests, I wrote 2,200+, a dedicated PG/Redis test DB, shared fixtures.
