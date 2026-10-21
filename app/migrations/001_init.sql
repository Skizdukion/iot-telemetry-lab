-- 001_init.sql: schema for the ingest pipeline.
-- Apply:  docker compose exec -T db psql -U iot -d iot < app/migrations/001_init.sql
--
-- DESIGN DECISION (the "two kinds of device" question):
--   Hybrid = fixed columns for what EVERY message has (ts, device, msg_id, seq, temperature)
--          + one JSONB column with all metrics.
--   Why not wide (one column per metric)?  Every new device type = ALTER TABLE + mostly-NULL columns.
--   Why not narrow (ts, device_id, metric, value)?  One message becomes 6-8 rows: 6-8x more rows,
--          bigger indexes, and "give me temperature AND rssi at time T" needs a pivot.
--   Hybrid cost: querying a JSONB metric is slower and untyped: (metrics->>'free_heap')::bigint.
--          If one JSONB metric becomes hot in dashboards, promote it to a real column later.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- One row per device. Auto-registered by the worker on its first valid message.
CREATE TABLE IF NOT EXISTS devices (
    id            text PRIMARY KEY,
    site          text        NOT NULL,
    first_seen    timestamptz NOT NULL,   -- server time of first valid message
    last_seen     timestamptz NOT NULL,   -- server time of latest valid message
    last_boot_id  integer                 -- NULL for devices that don't send boot_id
);

CREATE TABLE IF NOT EXISTS telemetry (
    ts            timestamptz      NOT NULL,              -- DEVICE clock (when it was measured)
    received_at   timestamptz      NOT NULL,              -- SERVER clock (when we got it)
    device_id     text             NOT NULL REFERENCES devices(id),
    msg_id        text             NOT NULL,              -- idempotency key
    seq           integer          NOT NULL,
    boot_id       integer,                                -- optional (see CONTRACT.md)
    reset_reason  text,
    temperature   double precision NOT NULL,              -- the one metric every device sends
    metrics       jsonb            NOT NULL               -- all metrics, incl. device-specific ones
);

-- Hypertable: TimescaleDB splits the table into 1-day chunks by ts.
SELECT create_hypertable('telemetry', 'ts',
                         chunk_time_interval => interval '1 day',
                         if_not_exists => TRUE);

-- Deduplication key. ON CONFLICT in the worker relies on this index.
CREATE UNIQUE INDEX IF NOT EXISTS telemetry_msg_uq ON telemetry (msg_id, ts);

-- "Latest / range of readings for one device": the most common query.
CREATE INDEX IF NOT EXISTS telemetry_device_ts ON telemetry (device_id, ts DESC);

-- Everything we refused, with the raw payload and why, so it can be inspected or replayed.
CREATE TABLE IF NOT EXISTS dead_letters (
    id           bigserial   PRIMARY KEY,
    received_at  timestamptz NOT NULL,
    topic        text        NOT NULL,
    payload      text        NOT NULL,
    reason       text        NOT NULL
);
