-- SQL drill dataset. Plain PostgreSQL (works in the TimescaleDB container too).
-- Load:  docker compose exec -T db psql -U iot -d iot < sql/seed.sql
--
-- 10 devices, 2 days of 1-minute readings, with realistic problems baked in:
--   * offline gaps (whole periods with no rows)
--   * duplicate rows (same device + ts inserted twice; deliberately NO unique constraint)
--   * a few temperature spikes
DROP SCHEMA IF EXISTS drill CASCADE;
CREATE SCHEMA drill;
SET search_path = drill;
SELECT setseed(0.42);

CREATE TABLE sites (
    id   text PRIMARY KEY,
    name text NOT NULL,
    tz   text NOT NULL
);

CREATE TABLE devices (
    id           text PRIMARY KEY,
    site_id      text NOT NULL REFERENCES sites(id),
    model        text NOT NULL,
    installed_at timestamptz NOT NULL
);

CREATE TABLE readings (
    device_id   text NOT NULL REFERENCES devices(id),
    ts          timestamptz NOT NULL,
    temperature double precision,
    humidity    double precision
);

INSERT INTO sites VALUES
  ('hcm-1', 'Ho Chi Minh Factory', 'Asia/Ho_Chi_Minh'),
  ('hn-1',  'Ha Noi Warehouse',    'Asia/Ho_Chi_Minh');

INSERT INTO devices
SELECT format('dev-%s', lpad(i::text, 3, '0')),
       CASE WHEN i <= 6 THEN 'hcm-1' ELSE 'hn-1' END,
       CASE WHEN i % 3 = 0 THEN 'TH-200' ELSE 'TH-100' END,
       timestamptz '2026-10-01 00:00+07' + (i || ' days')::interval
FROM generate_series(1, 10) i;

-- 2 days of minute readings
INSERT INTO readings
SELECT d.id,
       t,
       round((26 + (substr(d.id, 5)::int % 4)
              + 4 * sin(extract(epoch FROM t) / 86400 * 2 * pi())
              + (random() - 0.5))::numeric, 2),
       round((55 + 10 * random())::numeric, 1)
FROM devices d
CROSS JOIN generate_series(timestamptz '2026-10-17 00:00+07',
                           timestamptz '2026-10-18 23:59+07',
                           interval '1 minute') t;

-- offline gaps
DELETE FROM readings WHERE device_id = 'dev-003' AND ts >= '2026-10-17 10:00+07' AND ts < '2026-10-17 12:30+07';
DELETE FROM readings WHERE device_id = 'dev-003' AND ts >= '2026-10-18 02:15+07' AND ts < '2026-10-18 02:40+07';
DELETE FROM readings WHERE device_id = 'dev-007' AND ts >= '2026-10-18 14:00+07' AND ts < '2026-10-18 20:00+07';
DELETE FROM readings WHERE device_id = 'dev-009' AND ts >= '2026-10-18 22:00+07';           -- went dark, never came back
DELETE FROM readings WHERE device_id = 'dev-005' AND ts >= '2026-10-17 08:00+07' AND ts < '2026-10-17 08:03+07';  -- short blip

-- duplicates (QoS-1 redelivery that nobody deduplicated)
INSERT INTO readings
SELECT * FROM readings WHERE device_id IN ('dev-002', 'dev-006') AND random() < 0.02;

-- spikes
UPDATE readings SET temperature = 88.5 WHERE device_id = 'dev-004' AND ts = '2026-10-17 15:30+07';
UPDATE readings SET temperature = 91.2 WHERE device_id = 'dev-004' AND ts = '2026-10-17 15:31+07';
UPDATE readings SET temperature = 86.0 WHERE device_id = 'dev-008' AND ts = '2026-10-18 09:10+07';

ANALYZE;
SELECT count(*) AS readings_loaded FROM readings;
