"""Telemetry REST API (Task 2).

Run from the repo root:   uvicorn app.api.main:app --reload --port 8000
Then open http://localhost:8000/docs (Swagger UI, generated from the type hints below).

Every endpoint is `async def` because every slow thing in it is awaited (asyncpg).
Nothing blocking is called anywhere, so one event loop serves all requests.
"""
from __future__ import annotations

import base64
import binascii
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Optional

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from streamlit import metric

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://iot:iot@localhost:5432/iot")
OFFLINE_AFTER = timedelta(seconds=int(os.getenv("OFFLINE_AFTER_S", "30")))  # ~3x the C3's 5 s interval
MAX_RANGE = timedelta(days=7)

# Allow-lists: the ONLY values that may end up inside SQL text.
BUCKETS = {"1m": timedelta(minutes=1), "5m": timedelta(minutes=5),
           "15m": timedelta(minutes=15), "1h": timedelta(hours=1), "1d": timedelta(days=1)}
JSONB_METRICS = {"humidity", "battery_v", "rssi", "free_heap", "min_free_heap", "uptime_s", "wifi_reconnects"}
ALLOWED_METRICS = {"temperature"} | JSONB_METRICS


# --------------------------------------------------------------------------- lifespan + DB
async def _init_connection(conn: asyncpg.Connection) -> None:
    # asyncpg returns jsonb as a plain string by default. Decode it into dicts automatically.
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Runs once at startup (before the first request) and once at shutdown.
    # One pool for the whole app: opening a DB connection per request would be slow.
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10, init=_init_connection)
    yield
    await app.state.pool.close()


app = FastAPI(title="IoT Telemetry API", version="1.0", lifespan=lifespan)


async def get_conn(request: Request) -> AsyncIterator[asyncpg.Connection]:
    """Dependency: borrow a connection for one request, give it back afterwards (even on errors)."""
    async with request.app.state.pool.acquire() as conn:
        yield conn


# --------------------------------------------------------------------------- consistent errors
def error_body(status: int, message: str, details: Any = None) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"status": status, "message": message, "details": details}})


@app.exception_handler(HTTPException)
async def http_error(_: Request, exc: HTTPException) -> JSONResponse:
    return error_body(exc.status_code, str(exc.detail))


@app.exception_handler(RequestValidationError)
async def validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    details = [{"field": ".".join(str(p) for p in e["loc"]), "message": e["msg"]} for e in exc.errors()]
    return error_body(422, "invalid request", details)


# --------------------------------------------------------------------------- response models
class Device(BaseModel):
    id: str
    site: str
    first_seen: datetime
    last_seen: datetime
    last_boot_id: Optional[int]
    online: bool


class Reading(BaseModel):
    ts: datetime
    received_at: datetime
    msg_id: str
    seq: int
    boot_id: Optional[int]
    reset_reason: Optional[str]
    temperature: float
    metrics: dict[str, Any]


class ReadingPage(BaseModel):
    items: list[Reading]
    next_cursor: Optional[str]   # pass this back as ?cursor= to get the next page; null = no more


class Bucket(BaseModel):
    bucket: datetime
    min: float
    avg: float
    max: float
    count: int


class Boot(BaseModel):
    boot_id: int
    reset_reason: Optional[str]
    first_ts: datetime
    last_ts: datetime
    readings: int


class DeadLetter(BaseModel):
    id: int
    received_at: datetime
    topic: str
    payload: str
    reason: str


# --------------------------------------------------------------------------- helpers
def as_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def time_range(from_: Optional[datetime], to: Optional[datetime]) -> tuple[datetime, datetime]:
    """Defaults: the last hour. Validates from < to and a maximum range."""
    to = as_utc(to) if to else datetime.now(timezone.utc)
    from_ = as_utc(from_) if from_ else to - timedelta(hours=1)
    if from_ >= to:
        raise HTTPException(422, "'from' must be before 'to'")
    if to - from_ > MAX_RANGE:
        raise HTTPException(422, f"range too large (max {MAX_RANGE.days} days)")
    return from_, to


def encode_cursor(ts: datetime, msg_id: str) -> str:
    raw = json.dumps([ts.isoformat(), msg_id]).encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        ts, msg_id = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return datetime.fromisoformat(ts), msg_id
    except (ValueError, TypeError, binascii.Error):
        raise HTTPException(422, "invalid cursor")


async def ensure_device(conn: asyncpg.Connection, device_id: str) -> None:
    if not await conn.fetchval("SELECT 1 FROM devices WHERE id = $1", device_id):
        raise HTTPException(404, f"device {device_id!r} not found")


# --------------------------------------------------------------------------- endpoints
@app.get("/health")
async def health(conn: asyncpg.Connection = Depends(get_conn)) -> dict:
    await conn.fetchval("SELECT 1")
    return {"status": "ok"}


DEVICE_SQL = """
SELECT id, site, first_seen, last_seen, last_boot_id,
       (last_seen > now() - $1::interval) AS online
FROM devices
"""


@app.get("/devices", response_model=list[Device])
async def list_devices(conn: asyncpg.Connection = Depends(get_conn)):
    rows = await conn.fetch(DEVICE_SQL + " ORDER BY id", OFFLINE_AFTER)
    return [dict(r) for r in rows]


@app.get("/devices/{device_id}", response_model=Device)
async def get_device(device_id: str, conn: asyncpg.Connection = Depends(get_conn)):
    await ensure_device(conn, device_id)
    row = await conn.fetchrow(DEVICE_SQL + " WHERE id = $2", OFFLINE_AFTER, device_id)
    return dict(row) if row else None


READING_COLS = "ts, received_at, msg_id, seq, boot_id, reset_reason, temperature, metrics"


@app.get("/devices/{device_id}/latest", response_model=Reading)
async def latest(device_id: str, conn: asyncpg.Connection = Depends(get_conn)):
    await ensure_device(conn, device_id)
    # Uses the (device_id, ts DESC) index: one index lookup, not a scan.
    row = await conn.fetchrow(
        f"SELECT {READING_COLS} FROM telemetry WHERE device_id = $1 ORDER BY ts DESC LIMIT 1", device_id)
    if not row:
        raise HTTPException(404, "no readings yet")
    return dict(row)


@app.get("/devices/{device_id}/telemetry", response_model=ReadingPage)
async def telemetry(
    device_id: str,
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = None,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = None,
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Newest first, cursor pagination.

    Why a cursor and not OFFSET? `OFFSET 50000` makes Postgres read and throw away 50,000 rows,
    and if new rows arrive between two page requests, offset pages shift: you see rows twice or skip them.
    A cursor says "continue after this exact row", which is fast (index seek) and stable.
    Why (ts, msg_id) and not just ts? Two readings can share a ts; msg_id breaks the tie.
    """
    await ensure_device(conn, device_id)
    from_dt, to_dt = time_range(from_, to)
    args: list[Any] = [device_id, from_dt, to_dt, limit + 1]   # fetch one extra to know if there's a next page
    where = "device_id = $1 AND ts >= $2 AND ts < $3"
    if cursor:
        c_ts, c_msg = decode_cursor(cursor)
        print(c_ts, c_msg)
        where += " AND (ts, msg_id) < ($5, $6)"
        args += [c_ts, c_msg]
    rows = await conn.fetch(
        f"SELECT {READING_COLS} FROM telemetry WHERE {where} ORDER BY ts DESC, msg_id DESC LIMIT $4", *args)
    items = [dict(r) for r in rows[:limit]]
    
    next_cursor = encode_cursor(items[-1]["ts"], items[-1]["msg_id"]) if len(rows) > limit else None
    return {"items": items, "next_cursor": next_cursor}


@app.get("/devices/{device_id}/telemetry/agg", response_model=list[Bucket])
async def telemetry_agg(
    device_id: str,
    metric: str = "temperature",
    bucket: str = "5m",
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = None,
    conn: asyncpg.Connection = Depends(get_conn),
):
    """min/avg/max per time bucket, e.g. for a chart.
    Note: avg only makes sense for gauges (temperature, free_heap, rssi). For counters that reset
    on reboot (wifi_reconnects, uptime_s), look at max per bucket, or the increase, instead."""
    if bucket not in BUCKETS:
        raise HTTPException(422, f"bucket must be one of {sorted(BUCKETS)}")
    await ensure_device(conn, device_id)
    from_dt, to_dt = time_range(from_, to)

    # Column names can't be passed as $ parameters, so this part IS string-built.
    
    if metric not in ALLOWED_METRICS:
        raise HTTPException(422, f"metric must be one of {sorted(ALLOWED_METRICS)}")
    
    # 1. Define an allowlist or handle known columns explicitly
    if metric == "temperature":
        # If temperature is a dedicated column
        expr = "temperature"
        rows = await conn.fetch(f"""
            SELECT time_bucket($2::interval, ts) AS bucket,
                min(v) AS min, avg(v) AS avg, max(v) AS max, count(*) AS count
            FROM (SELECT ts, {expr} AS v FROM telemetry
                WHERE device_id = $1 AND ts >= $3 AND ts < $4) s
            WHERE v IS NOT NULL
            GROUP BY 1 ORDER BY 1""", device_id, BUCKETS[bucket], from_dt, to_dt)
    else:
        # 2. For JSONB metrics, pass the metric name as a bind parameter ($5)
        # PostgreSQL allows JSONB text-field extraction via parameterization.
        rows = await conn.fetch("""
            SELECT time_bucket($2::interval, ts) AS bucket,
                min(v) AS min, avg(v) AS avg, max(v) AS max, count(*) AS count
            FROM (SELECT ts, (metrics->>$5)::float8 AS v FROM telemetry
                WHERE device_id = $1 AND ts >= $3 AND ts < $4) s
            WHERE v IS NOT NULL
            GROUP BY 1 ORDER BY 1""", device_id, BUCKETS[bucket], from_dt, to_dt, metric)
    return [dict(r) for r in rows]


@app.get("/devices/{device_id}/boots", response_model=list[Boot])
async def boots(device_id: str, limit: int = Query(20, ge=1, le=200),
                conn: asyncpg.Connection = Depends(get_conn)):
    """One row per boot. A boot whose reset_reason isn't 'sw' or 'poweron' deserves a look (panic, brownout...)."""
    await ensure_device(conn, device_id)
    rows = await conn.fetch("""
        SELECT boot_id,
               (array_agg(reset_reason ORDER BY ts))[1] AS reset_reason,
               min(ts) AS first_ts, max(ts) AS last_ts, count(*) AS readings
        FROM telemetry
        WHERE device_id = $1 AND boot_id IS NOT NULL
        GROUP BY boot_id ORDER BY boot_id DESC LIMIT $2""", device_id, limit)
    return [dict(r) for r in rows]


@app.get("/rejected", response_model=list[DeadLetter])
async def rejected(limit: int = Query(50, ge=1, le=500), conn: asyncpg.Connection = Depends(get_conn)):
    rows = await conn.fetch(
        "SELECT id, received_at, topic, payload, reason FROM dead_letters ORDER BY id DESC LIMIT $1", limit)
    return [dict(r) for r in rows]
