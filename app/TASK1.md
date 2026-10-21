# Task 1: code tour + bug hunt

The ingest worker is written. **It contains 5 planted bugs.** Each bug is a classic
mistake in telemetry pipelines and maps to an interview question. Your job: read the code,
reproduce each bug, find the line, fix it, and explain why your fix is right.

## 1. Run it

```bash
# from the repo root, in WSL, with your venv active
pip install -r app/requirements.txt -r tools/requirements.txt
docker compose up -d                  # restart mosquitto if it was already running: the conf changed
docker compose exec -T db psql -U iot -d iot < app/migrations/001_init.sql
python -m app.ingest.worker           # terminal 1: the worker
python tools/simulator.py --devices 20 --duration 120   # terminal 2
python tools/verify.py                # after the simulator ends (wait ~3 s)
```
Before every new test: `docker compose exec db psql -U iot -d iot -c "TRUNCATE telemetry, dead_letters;"`
Also keep your C3 running. Its rows should land in `telemetry` too.

Useful queries:
```sql
SELECT reason, count(*) FROM dead_letters GROUP BY 1 ORDER BY 2 DESC;
SELECT * FROM devices ORDER BY id;
SELECT device_id, ts, received_at, seq, boot_id, metrics FROM telemetry
WHERE device_id = 'c3-health-01' ORDER BY ts DESC LIMIT 5;
```

## 2. Code tour: where each concept lives

| Concept (interview words) | Where |
|---|---|
| Schema decision: hybrid columns + JSONB, and why | `migrations/001_init.sql`, comment at the top |
| Hypertable, chunks | `001_init.sql`, `create_hypertable(...)` |
| Idempotency key / dedupe in the DB | `001_init.sql` unique index + `worker.py` `INSERT_TELEMETRY` (`ON CONFLICT ... DO NOTHING`) |
| Contract validation (Pydantic v2, strict types, custom rules) | `models.py`, `Metrics`, `Telemetry`, `ts_in_window` |
| Device must publish as itself | `models.py`, `validate()`: topic vs payload `device_id` |
| Dead-letter queue | `worker.py` `write_batch()` → `dead_letters` table |
| Decouple receive from write; backpressure | `worker.py` docstring, `receive_loop`, bounded `asyncio.Queue` |
| Batching (size or time) | `worker.py` `collect_batch` (your kata 6) |
| One round trip per batch (`unnest`), why not `COPY` | `INSERT_TELEMETRY` (COPY can't do `ON CONFLICT`) |
| Transactions: all or nothing per batch | `write_rows()` |
| Transient vs permanent errors, poison batch | `TRANSIENT_DB_ERRORS`, the two `except` branches in `write_batch` |
| Exponential backoff (kata 7) | `write_batch` |
| Persistent MQTT session, fixed client id | `config.py` `MQTT_CLIENT_ID`, `MQTT_CLEAN_SESSION` |
| Auto-register devices, deadlock-safe ordering | `UPSERT_DEVICE`, `device_rows()` |
| "Are we falling behind?" | `report_loop` (queue size), `stats` |
| Graceful shutdown (drain the queue on Ctrl+C / SIGTERM) | `main()` |
| Config from env vars | `config.py` |

## 3. Bug hunt

Fix them in order. Each one has a **symptom**, a way to **reproduce** it, and the **concept** it's about.
No line numbers: finding the line is the exercise. If you're stuck for more than 10 minutes, ask me for a bigger hint.

**Bug 1: the migration fails.** - SOLVED
- Reproduce: apply `001_init.sql`. Read the error message carefully, because it tells you exactly what's wrong.
- After fixing it, check that the worker's `ON CONFLICT (...)` matches your index. They must agree.
- Concept: unique constraints on hypertables.

**Bug 2: `verify.py` says "SHOULD HAVE BEEN REJECTED".**
- Reproduce: a normal simulator run, then `verify.py`. Look up which reason those msg_ids had in `sim_out/rejected.jsonl`.
- Concept: contract validation. Compare the code with `CONTRACT.md` word for word.

**Bug 3: data is lost when the DATABASE restarts.**
- Reproduce: start a 120 s simulator run. After ~30 s run `docker compose restart db`. When the simulator ends, run `verify.py` → MISSING.
  Watch the worker's log while the DB is down.
- Concept: "What happens if the DB is down for 5 minutes?" The answer must be "nothing is lost".
- Hint: one variable in that code is computed but never used.

**Bug 4: data is lost when the WORKER restarts.**
- Reproduce (only after fixing bug 3): start a 120 s run, stop the worker with Ctrl+C after ~30 s, wait 15 s, start it again → MISSING.
  The broker *could* have kept those messages for you. Why didn't it?
- Concept: persistent sessions (`clean_session`), and why the client id must be fixed.
- Follow-up question to answer: even after the fix, if you `kill -9` the worker (no graceful shutdown), can messages
  still be lost? Where are they at that moment? (Think about *when* the worker acknowledges a QoS 1 message, compared
  with when the row is committed.)

**Bug 5: no test catches this one. Find it by reading.**
- The contract says: store the device time **and the time the server received the message**.
  One of those two values is captured in the wrong place.
- Ask yourself: after a 30 s DB outage (bug 3's test), what would `received_at` say for messages that arrived during the outage?
- Concept: "store both timestamps", and getting each one at the right moment.

**Done when:** all 5 are fixed; a 120 s chaos run with a DB restart **and** a worker restart in the middle gives `verify.py` PASS;
and you can explain each fix in one or two sentences. Send me the diff (or push), plus your answer to the bug 4 follow-up question.
