# Tasks

You're building a small version of a real telemetry platform:

```
 ESP32-C3 ─────┐   (your board, reporting its own health)
               ├─MQTT─> Mosquitto ──> ingest worker ──> TimescaleDB <── FastAPI (REST + WebSocket) <── browser
 simulator ────┘          ▲                 │                                │
 (fake fleet, chaos)      └──── commands ───┴────────────────────────────────┘
```

Each task has **Goal → Requirements → Done when → Hints → Interview questions it prepares you for.**
The hints point you in a direction. They are not solutions. When a task is done, send me
the code (or push it to GitHub and send me the link) plus your answers to its interview questions, and I'll review it like an interviewer would.

Suggested time: Task 0–2 are the core (about 1 to 1.5 days). Do 3–6 as time allows. 7 is stretch.

---

## Task 0: ESP32-C3 health monitor (no extra parts)

**Your hardware:** ESP32-C3 DevKitM-1 (Arduino core 3.x via pioarduino), just the bare board on USB. **Unplug the yaw wires**: you don't need them.
The board monitors **itself**: chip temperature, Wi-Fi signal, memory, uptime and reboots. It sounds boring, but this is exactly what
fleet-health monitoring looks like at every IoT company. To make the data move, the device also accepts commands that change
its own health on purpose (CPU stress, a fake memory leak, a reboot), so you have real events to detect.

**Goal:** a proper telemetry device, `device_id = "c3-health-01"`, that follows `CONTRACT.md`.

Start a new PlatformIO project in `firmware/c3-health` by copying the `[env:c3]` section from your `blick-test/platformio.ini`.
Library choice: `256dpi/MQTT` (arduino-mqtt) or `bertmelis/espMqttClient`. Both support publishing at QoS 1 on core 3.x.
The popular `PubSubClient` library can only **publish at QoS 0**. Work out why that matters here before you choose.

Milestones (do them in order and commit after each one):

- **0a. Read the health data (serial only, no MQTT yet).** Every 2 s, print:
  - chip temperature: `temperatureRead()`
  - free heap and lowest-ever free heap: `ESP.getFreeHeap()`, `ESP.getMinFreeHeap()`
  - uptime: `millis()`
  - the reason for the last reset: `esp_reset_reason()`. Map it to a string (`poweron`, `sw`, `panic`, `wdt`, `brownout`…)
  
  Then connect to Wi-Fi (the C3 is 2.4 GHz only) and add `WiFi.RSSI()`.
  Find out: `millis()` is a 32-bit counter. When does it wrap around, and does that break your uptime?
- **0b. Hello telemetry.** Sync time with NTP and publish every N seconds (default 5) to
  `site-1/devices/c3-health-01/telemetry` on the Mosquitto on your laptop (your laptop's LAN IP, port 1883).
  - Include `boot_id` (a counter stored in flash with `Preferences`, incremented on every boot) and `reset_reason` (both are top-level fields, see the contract).
  - Watch it arrive: `docker compose exec mosquitto mosquitto_sub -t '#' -v`.
- **0c. Presence.** Set a Last Will (`offline`, retained) and publish a retained `online` on connect.
  Unplug the USB cable and time how long until `offline` appears. Explain the number using your keep-alive setting.
- **0d. Survive outages.** Run `docker compose stop mosquitto` for 2 minutes, then start it again. Also switch off your hotspot,
  or **power the board from a power bank and walk away from the router until the Wi-Fi drops**. That's the best test, because
  you'll watch RSSI fall in the data just before the gap.
  - The device keeps sampling and buffers readings (a RAM ring buffer is fine; LittleFS is the stretch goal).
  - It reconnects with backoff, replays the backlog, and counts `wifi_reconnects`.
  - You decide what happens when the buffer is full: drop the oldest or the newest? Explain why.
- **0e. Commands.** Subscribe to `.../cmd`, publish an ack for every command, and support these (details in `CONTRACT.md`):
  - `set_interval {seconds}`: changes how often telemetry is sent.
  - `led {r, g, b}`: the DevKitM-1 has an RGB LED on GPIO8 (`rgbLedWrite`). A quick visual check that commands work.
  - `stress {seconds}`: keep the CPU busy so the chip temperature rises. **Trap:** the C3 has a *single* core.
    A plain busy loop also starves Wi-Fi and MQTT, so the broker thinks the device died. Make it work without the device dropping offline.
  - `leak {kb}` / `free`: allocate memory and hold onto it, then release it. Watch `free_heap` drop in the data. Cap the total so you can't crash the board,
    unless you want to see what `reset_reason = panic` looks like.
  - `reboot`: ack first, then restart. After the reboot, `boot_id` goes up and `seq` starts again at 1.
  - Never execute a `cmd_id` twice: remember the last few ids. Think about why this matters most for `reboot`.
  - Test by hand: `mosquitto_pub -t site-1/devices/c3-health-01/cmd -q 1 -m '{"cmd_id":"t1","type":"stress","args":{"seconds":30}}'`

**Done when:** the board runs for 30+ minutes including a broker outage and a Wi-Fi outage; once Task 1 is built,
`SELECT boot_id, count(*), min(seq), max(seq) FROM telemetry WHERE device_id='c3-health-01' GROUP BY boot_id` shows no gaps you can't explain;
and `stress` visibly raises the chip temperature without the device going offline.

**Think about (write answers in your notes):**
- `seq` resets to 1 on reboot. If `msg_id` were `device_id-seq`, what breaks? How does `boot_id` fix it?
- What timestamp do you send for readings taken **before** NTP has synced, e.g. buffered right after boot with no Wi-Fi?
- How can the backend tell "the device rebooted" from "messages were lost"? (`boot_id`, `uptime_s` going down, `reset_reason`.)
- A QoS 1 `reboot` command can arrive **twice**, or arrive again after the reboot if the broker re-sends it. What happens without deduplication?
  Where do you store the "already executed" ids so they survive the reboot itself?
- Keep-alive of 60 s vs 10 s: the trade-off between traffic and how fast you detect that the device is offline.

**Interview questions:** QoS 0/1/2 with real examples · LWT and retained messages · how devices behave offline ·
clock problems on devices · detecting reboots and data gaps · safe remote commands (idempotency, timeouts) ·
why MQTT beats HTTP for a device like this.

---

## Task 1: Schema + ingest worker

**Goal:** MQTT → validated → deduplicated → stored, without losing anything.

Requirements:
- Design the schema: a telemetry table (make it a **hypertable**), a devices table, and a dead-letter table for rejected messages. Use migrations (Alembic, or plain numbered SQL files).
- A Python worker (`aiomqtt` or `paho-mqtt`) subscribes to `+/devices/+/telemetry`, validates with **Pydantic v2** per `CONTRACT.md`, and **batch-inserts** (e.g. 200 rows or 1 s, whichever comes first; kata 6 is this).
- Idempotent writes: a duplicate `msg_id` must not create a second row and must not crash the batch.
- Rejected messages go to the dead-letter table with the raw payload and the reason.
- Store **both** the device timestamp and the time your server received the message.
- Auto-register unknown devices on their first valid message (or reject them, but decide which and explain why).
- **Two kinds of device, different metrics.** The simulator sends `temperature/humidity/battery_v`; your C3 sends
  `temperature/rssi/free_heap/min_free_heap/uptime_s/wifi_reconnects`. Choose how to store them and defend it:
  a wide table (one column per metric, lots of NULLs), a narrow table (`ts, device_id, metric, value`), or fixed columns plus a `JSONB` column.
  This is a classic interview question, and now you'll have felt the trade-off yourself.

**Done when:**
```
python tools/simulator.py --devices 20 --duration 120
python tools/verify.py            # → PASS
```
with the default chaos settings, while `c3-health-01` is landing rows at the same time.

**Hints:** `INSERT ... ON CONFLICT DO NOTHING` (what must the unique constraint include on a hypertable?) ·
`executemany` vs `COPY` · keep the MQTT receive loop free of slow work (`asyncio.Queue` between receiving and writing) ·
use a persistent session (`clean_session=False` / `clean_start=False`) with a fixed client id, so the broker queues messages while your worker restarts.

**Interview questions:** at-least-once vs exactly-once · where you would deduplicate, and why in the DB and not only in memory ·
what happens if the DB is down for 5 minutes · how you would know the worker is falling behind.

---

## Task 2: REST API (FastAPI)

**Goal:** a clean API over the data.

Endpoints (design the exact shapes yourself and write them down):
- `GET /devices`: list devices with their last-seen time and online/offline state.
- `GET /devices/{id}`
- `GET /devices/{id}/latest`: the latest reading.
- `GET /devices/{id}/telemetry?from=&to=&limit=&cursor=`: raw readings, **cursor pagination**.
- `GET /devices/{id}/telemetry/agg?from=&to=&bucket=5m&metric=temperature`: min/avg/max per bucket (`time_bucket`).
- `GET /rejected?limit=`: browse dead letters.

Requirements: Pydantic response models, proper status codes (404, 422), a consistent error body,
an async DB driver (`asyncpg`, or SQLAlchemy 2.0 async), a connection pool created in `lifespan`, and validation of `from < to`, max range and a bucket allow-list.

**Done when:** you can open `/docs`, page through 10k readings without duplicates or skipped rows, and the agg endpoint
shows a `stress` run as a bump in `metric=temperature&bucket=1m` for your C3, and a `leak` as a dip in `metric=free_heap`.
Add `GET /devices/{id}/boots`: one row per `boot_id` with first/last seen, `reset_reason` and reading count.
(Think: `free_heap` is a gauge, but `wifi_reconnects` is a counter that resets on reboot. Why is `avg` meaningless for a counter, and what do you show instead?)

**Hints:** a cursor is an encoded `(ts, msg_id)`. Ask yourself why `ts` alone isn't enough as a cursor. ·
Never call a blocking driver inside `async def`.

**Interview questions:** offset vs cursor pagination · `async def` vs `def` in FastAPI · how `Depends` works · N+1 queries · how you'd version this API.

---

## Task 3: Device state, alerts and commands

**Goal:** close the loop, from the device to the backend and back to the device.

- Consume the `status` topic and keep a `device_state` table (online/offline, `last_seen`, `last_status_change`).
  Also mark a device offline if it has sent no telemetry for 3× its interval, even when no LWT arrived. Explain why both mechanisms are needed.
- Alerts, two rules:
  - Simulator devices: temperature > 80 opens an alert, and dropping below 75 closes it (**hysteresis**, so a value flapping around 80 doesn't create 100 alerts).
  - Your C3, three rules:
    - **low memory:** opens when `free_heap` drops below 50% of that device's normal level (pick how you define "normal").
    - **weak signal:** opens when `rssi < -80` for 30 s continuously, so one bad reading doesn't count.
    - **unexpected reboot:** an event (not an open/close alert) when `boot_id` changes and `reset_reason` is not `sw`.
      A reboot *you* commanded is expected; a `panic` or `brownout` is not.
  - Store the episodes. `GET /alerts?open=true`.
- Commands: `POST /devices/{id}/commands {type, args}` publishes to `.../cmd` and returns a `cmd_id` with status `sent`.
  When the ack arrives, update the status to `acked` or `failed`. A background check marks commands with no ack after 30 s as `timeout`.
  `GET /commands/{cmd_id}`.
  - Validate on the server too (e.g. `stress.seconds ≤ 60`, `leak.kb` capped), and **reject commands to a device that is offline** with `409`. Don't queue them.
    Explain why a `reboot` delivered 10 minutes late is a bad surprise, and what you'd use instead for settings like `set_interval`
    (hint: a retained "desired config" message the device applies whenever it connects).
  - Rate-limit commands per device.

**Done when:** from `/docs` you can send `leak` and watch the low-memory alert open, then `free` and watch it close; `reboot` produces no
"unexpected reboot" event, while pressing the board's RST button (or a `leak` big enough to cause a panic) produces one; walking away with the power bank opens
the weak-signal alert; unplugging the C3 flips it to offline within the time you predicted in Task 0c; and a command sent while it's offline gets a 409.

**Interview questions:** how you detect offline devices · how you'd send a command to a device that is offline right now ·
why alerts need hysteresis · how you'd make sure a command runs only once on the device ·
command vs desired state (the "device shadow" / "digital twin" pattern).

---

## Task 4: WebSocket live view

**Goal:** a browser page showing live readings.

- `WS /ws/devices/{id}` (and/or `/ws/stream` with a subscribe message) pushes new readings and alerts.
- The worker publishes "new reading" events to **Redis pub/sub** and the API fans them out to WebSocket clients.
  Don't let the API poll the DB.
- Handle disconnects, heartbeats (ping/pong) and slow clients (a bounded per-client queue: drop or disconnect, your choice, explain it).
- A minimal HTML page with a live chart (Chart.js from a CDN is fine). Keep the front end small; the backend is what you're practising.

**Done when:** 2 API instances behind `docker compose` both serve live data (proving the Redis fan-out works), and
your C3's chart updates live: send `set_interval 1`, then `stress 30`, and watch the temperature climb in real time.
Bonus: buttons on the page for stress / leak / free / reboot through your Task 3 API, so you can demo the whole loop in the interview.

**Interview questions:** scaling WebSockets horizontally · WebSocket vs SSE vs polling · authenticating a WebSocket · backpressure.

---

## Task 5: Time-series features + HTTP ingest

- A **continuous aggregate** for 1-minute and 1-hour rollups, and switch `/telemetry/agg` to use them for long ranges.
- A **compression** policy (chunks older than 1 day) and a **retention** policy (raw data older than 7 days; keep the rollups). Measure the table size before and after compression.
- `POST /ingest/batch` (`{"readings": [...]}`, up to 500): the same validation and dedupe as MQTT, returning per-item results.
  Idempotent: posting the same batch twice changes nothing.
  Test it: `python tools/simulator.py --replay-http http://localhost:8000/ingest/batch`, then `verify.py` must still PASS.
- Bonus: make the C3 upload its offline backlog over HTTP instead of MQTT, and argue which is better.

**Interview questions:** hypertables and chunks · continuous aggregates vs materialized views · retention vs compression ·
narrow vs wide schema · when you'd choose InfluxDB or ClickHouse instead.

---

## Task 6: Tests, Docker, CI

- **pytest:** unit tests for validation and batching; integration tests for the API against a real Postgres
  (testcontainers, or a compose test DB); a test for the ingest worker that publishes to a real Mosquitto and checks the DB.
- **Docker:** a multi-stage Dockerfile for the API and worker, running as non-root. Add both to `docker-compose.yml` with healthchecks and `depends_on: condition: service_healthy`.
- **CI (GitHub Actions):** ruff → mypy → pytest (with service containers for Postgres and Mosquitto) → build the image.
  Bonus: a job that runs the simulator for 30 s plus `verify.py` as an end-to-end test.

**Interview questions:** unit vs integration vs e2e, and what you'd mock · fixtures and their scope · Docker layer caching ·
how you run migrations in CI/CD · how you'd roll back a bad deploy.

---

## Task 7 (stretch): Break it

- Kill the worker (`docker compose kill worker`) in the middle of a simulator run and restart it. Does `verify.py` still pass? If not, find out why (hint: when do you ack, and do you use a persistent session?).
- Run **2 workers** with an MQTT shared subscription (`$share/ingest/+/devices/+/telemetry`). Still PASS?
- Load: `--devices 500 --interval 0.5`. Find the bottleneck and measure it (ingest rate, batch latency, DB CPU).
- Security: turn off anonymous access in Mosquitto, give each device a username/password, and add an ACL so `dev-001` can only publish to its own topics. Bonus: TLS on port 8883 for the C3. This matters here because anyone on the network could otherwise send your board `reboot`.
- Write a 1-page `DESIGN.md`: architecture, the trade-offs you made, and what you'd change at 100k devices. **This is your interview cheat sheet.**
