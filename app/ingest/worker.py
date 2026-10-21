"""Ingest worker: MQTT -> validate -> dedupe -> TimescaleDB.

Run from the repo root:   python -m app.ingest.worker

Shape of the program (3 asyncio tasks, connected by one queue):

    receive_loop  --put-->  asyncio.Queue (bounded)  --batch-->  writer_loop  -->  Postgres
    (fast: never touches the DB)                                 (slow: validate + insert)
    report_loop: logs queue size etc. every 10 s, so you can see if we fall behind.

Why split receive and write?  The MQTT loop must keep running to answer keep-alive pings and
acknowledge messages. If it waited on a slow INSERT, the broker could think we're dead and
messages would pile up. The queue absorbs bursts; because it's bounded, a stuck DB eventually
slows down receiving instead of eating all RAM (= backpressure).
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from dataclasses import dataclass
from datetime import datetime, timezone

import aiomqtt
import asyncpg

from . import config
from .models import Telemetry, parse_topic, validate

log = logging.getLogger("ingest")

# Errors where retrying later makes sense (DB restarting, network blip).
# Anything else from Postgres is a data problem: retrying the same batch would fail forever.
TRANSIENT_DB_ERRORS = (
    OSError,  # connection refused / reset
    asyncio.TimeoutError,
    asyncpg.exceptions.InterfaceError,  # connection closed mid-query
    asyncpg.exceptions.PostgresConnectionError,
    asyncpg.exceptions.OperatorInterventionError,  # "the database system is shutting down / starting up"
)


@dataclass
class Incoming:
    topic: str
    payload: bytes
    received_at: datetime  # server time, captured the moment MQTT delivered it


stats = {
    "received": 0,
    "inserted": 0,
    "duplicates": 0,
    "dead": 0,
    "batches": 0,
    "last_batch_ms": 0.0,
}


# --------------------------------------------------------------------------- receive
async def receive_loop(queue: asyncio.Queue) -> None:
    """Connect, subscribe, push every message onto the queue. Reconnect forever on errors."""
    while True:
        try:
            async with aiomqtt.Client(
                hostname=config.MQTT_HOST,
                port=config.MQTT_PORT,
                identifier=config.MQTT_CLIENT_ID,
                clean_session=config.MQTT_CLEAN_SESSION,
                keepalive=30,
            ) as client:
                await client.subscribe(config.MQTT_TOPIC, qos=1)
                log.info(
                    "MQTT connected to %s:%s, subscribed to %s (clean_session=%s)",
                    config.MQTT_HOST,
                    config.MQTT_PORT,
                    config.MQTT_TOPIC,
                    config.MQTT_CLEAN_SESSION,
                )
                async for message in client.messages:
                    payload = message.payload
                    if isinstance(payload, str):
                        payload = payload.encode()
                    item = Incoming(
                        topic=message.topic.value,
                        payload=bytes(payload or b""),
                        received_at=datetime.now(timezone.utc),
                    )
                    stats["received"] += 1
                    await queue.put(item)  # blocks if the queue is full (backpressure)
        except aiomqtt.MqttError as e:
            log.warning("MQTT connection lost (%s), reconnecting in 2 s", e)
            await asyncio.sleep(2)


# --------------------------------------------------------------------------- batching
async def collect_batch(
    queue: asyncio.Queue, max_size: int, max_wait: float
) -> list[Incoming]:
    """Wait for the first item, then collect until max_size items or max_wait seconds (kata 6)."""
    batch = [await queue.get()]
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max_wait
    while len(batch) < max_size:
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        try:
            batch.append(await asyncio.wait_for(queue.get(), remaining))
        except asyncio.TimeoutError:
            break
    return batch


# --------------------------------------------------------------------------- SQL
UPSERT_DEVICE = """
INSERT INTO devices (id, site, first_seen, last_seen, last_boot_id)
VALUES ($1, $2, $3, $3, $4)
ON CONFLICT (id) DO UPDATE SET
    last_seen    = GREATEST(devices.last_seen, EXCLUDED.last_seen),
    last_boot_id = GREATEST(devices.last_boot_id, EXCLUDED.last_boot_id)   -- GREATEST ignores NULLs
"""

# One round trip for the whole batch: pass each column as an array, unnest() turns the arrays
# back into rows. ON CONFLICT DO NOTHING = a duplicate msg_id is silently skipped, never an error.
INSERT_TELEMETRY = """
INSERT INTO telemetry (ts, received_at, device_id, msg_id, seq, boot_id, reset_reason, temperature, metrics)
SELECT ts, received_at, device_id, msg_id, seq, boot_id, reset_reason, temperature, metrics::jsonb
FROM unnest($1::timestamptz[], $2::timestamptz[], $3::text[], $4::text[], $5::int[],
            $6::int[], $7::text[], $8::float8[], $9::text[])
     AS t(ts, received_at, device_id, msg_id, seq, boot_id, reset_reason, temperature, metrics)
ON CONFLICT (msg_id, ts) DO NOTHING
"""

INSERT_DEAD = "INSERT INTO dead_letters (received_at, topic, payload, reason) VALUES ($1, $2, $3, $4)"


def telemetry_columns(rows: list[tuple[Incoming, Telemetry]]) -> list[list]:
    # now = datetime.now(timezone.utc)
    cols: list[list] = [[] for _ in range(9)]
    for item, msg in rows:
        values = (
            msg.ts,
            item.received_at,
            msg.device_id,
            msg.msg_id,
            msg.seq,
            msg.boot_id,
            msg.reset_reason,
            msg.metrics.temperature,
            json.dumps(msg.metrics.model_dump(exclude_none=True)),
        )
        for col, v in zip(cols, values):
            col.append(v)
    return cols


def device_rows(rows: list[tuple[Incoming, Telemetry]]) -> list[tuple]:
    """One upsert per device per batch, sorted by id. A consistent lock order means two
    workers upserting the same devices can't deadlock each other."""
    latest: dict[str, tuple] = {}
    for item, msg in rows:
        site, _ = parse_topic(item.topic)
        prev = latest.get(msg.device_id)
        seen = max(item.received_at, prev[2]) if prev else item.received_at
        boot = max(filter(None, [msg.boot_id, prev[3] if prev else None]), default=None)
        latest[msg.device_id] = (msg.device_id, site, seen, boot)
    return [latest[k] for k in sorted(latest)]


async def write_rows(conn: asyncpg.Connection, valid, dead) -> int:
    """Everything for one batch in ONE transaction: all or nothing."""
    async with conn.transaction():
        if valid:
            await conn.executemany(
                UPSERT_DEVICE, device_rows(valid)
            )  # devices first (foreign key)
            status = await conn.execute(INSERT_TELEMETRY, *telemetry_columns(valid))
            inserted = int(status.split()[-1])  # "INSERT 0 187" -> 187
        else:
            inserted = 0
        if dead:
            await conn.executemany(INSERT_DEAD, dead)
    return inserted


# --------------------------------------------------------------------------- writer
async def write_batch(pool: asyncpg.Pool, batch: list[Incoming]) -> None:
    valid: list[tuple[Incoming, Telemetry]] = []
    dead: list[tuple] = []
    for item in batch:
        msg, reason = validate(item.topic, item.payload)
        if msg:
            valid.append((item, msg))
        else:
            dead.append(
                (
                    item.received_at,
                    item.topic,
                    item.payload.decode("utf-8", "replace"),
                    reason,
                )
            )

    attempt = 0
    while True:
        try:
            async with pool.acquire() as conn:
                inserted = await write_rows(conn, valid, dead)
            stats["inserted"] += inserted
            stats["duplicates"] += len(valid) - inserted
            stats["dead"] += len(dead)
            return
        except TRANSIENT_DB_ERRORS as e:
            attempt += 1
            delay = min(60.0, 0.5 * 2**attempt)  # exponential backoff, capped (kata 7)
            await asyncio.sleep(delay)
            log.warning(
                "DB unavailable (%s). Batch of %d have try %d attempts",
                type(e).__name__,
                len(batch),
                attempt,
            )
        except asyncpg.PostgresError as e:
            # A data error (e.g. a constraint we didn't expect). Retrying the same batch would fail
            # forever: a "poison batch". Write row by row so one bad row can't block the rest.
            log.error(
                "Batch failed with %s: %s. Falling back to row-by-row",
                type(e).__name__,
                e,
            )
            await write_one_by_one(pool, valid, dead)
            return


async def write_one_by_one(pool: asyncpg.Pool, valid, dead) -> None:
    async with pool.acquire() as conn:
        for row in valid:
            try:
                inserted = await write_rows(conn, [row], [])
                stats["inserted"] += inserted
                stats["duplicates"] += 1 - inserted
            except asyncpg.PostgresError as e:
                item, _ = row
                dead.append(
                    (
                        item.received_at,
                        item.topic,
                        item.payload.decode("utf-8", "replace"),
                        f"db error: {type(e).__name__}: {e}",
                    )
                )
        if dead:
            await write_rows(conn, [], dead)
            stats["dead"] += len(dead)


async def writer_loop(queue: asyncio.Queue, pool: asyncpg.Pool) -> None:
    loop = asyncio.get_running_loop()
    while True:
        batch = await collect_batch(queue, config.BATCH_MAX_SIZE, config.BATCH_MAX_WAIT)
        t0 = loop.time()
        try:
            await write_batch(pool, batch)
        except Exception:
            log.exception("Unexpected error writing a batch of %d", len(batch))
        finally:
            stats["batches"] += 1
            stats["last_batch_ms"] = round((loop.time() - t0) * 1000, 1)
            for _ in batch:
                queue.task_done()


async def report_loop(queue: asyncio.Queue) -> None:
    """The 'are we falling behind?' signal: if queue size keeps growing, the writer is too slow."""
    while True:
        await asyncio.sleep(10)
        log.info("queue=%d %s", queue.qsize(), stats)


# --------------------------------------------------------------------------- main
async def connect_db() -> asyncpg.Pool:
    while True:
        try:
            pool = await asyncpg.create_pool(
                config.DATABASE_URL, min_size=1, max_size=4
            )
            log.info("DB connected")
            return pool
        except TRANSIENT_DB_ERRORS as e:
            log.warning("DB not reachable yet (%s), retrying in 2 s", e)
            await asyncio.sleep(2)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    pool = await connect_db()
    queue: asyncio.Queue = asyncio.Queue(maxsize=config.QUEUE_MAX_SIZE)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows: Ctrl+C will just raise KeyboardInterrupt
            pass

    receiver = asyncio.create_task(receive_loop(queue))
    writer = asyncio.create_task(writer_loop(queue, pool))
    reporter = asyncio.create_task(report_loop(queue))

    await stop.wait()
    # Graceful shutdown: stop taking new messages, write what's already queued, then exit.
    log.info("Shutting down: draining %d queued messages", queue.qsize())
    receiver.cancel()
    try:
        await asyncio.wait_for(queue.join(), timeout=15)
    except asyncio.TimeoutError:
        log.warning("Drain timed out with %d messages left", queue.qsize())
    writer.cancel()
    reporter.cancel()
    await pool.close()
    log.info("Bye. %s", stats)


if __name__ == "__main__":
    asyncio.run(main())
