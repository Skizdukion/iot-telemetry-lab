# Message contract

Every producer follows this contract: the simulator, your ESP32, and any test you write.
Your backend must accept exactly this and reject everything else.

## Topics

| Topic | Direction | QoS | Retained | Payload |
|---|---|---|---|---|
| `{site}/devices/{device_id}/telemetry` | device → backend | 1 | no | Telemetry JSON (below) |
| `{site}/devices/{device_id}/status` | device → backend | 1 | **yes** | `online` / `offline` (plain text). `offline` is also the device's **Last Will** |
| `{site}/devices/{device_id}/cmd` | backend → device | 1 | no | Command JSON (Task 3) |
| `{site}/devices/{device_id}/cmd/ack` | device → backend | 1 | no | Ack JSON (Task 3) |

The simulator uses `site-1` and `dev-001 … dev-NNN`. Your ESP32-C3 is `c3-health-01`.

## Telemetry payload

```json
{
  "msg_id": "6f1c2c1e-7d1b-4a57-9a36-2a3f0c1d9e11",
  "device_id": "dev-001",
  "seq": 42,
  "ts": "2026-10-19T04:25:21.572+00:00",
  "metrics": { "temperature": 25.38, "humidity": 54.1, "battery_v": 4.091 }
}
```

| Field | Rule |
|---|---|
| `msg_id` | Required string, 1–64 chars, unique per reading. **This is the idempotency key.** The simulator uses UUIDv4. On the ESP32, a UUID from `esp_random()` or `"{device_id}-{boot_id}-{seq}"` both work (Task 0 asks you to think about which). |
| `device_id` | Required. Must match the `{device_id}` in the topic. |
| `seq` | Required int ≥ 1, increases per reading, may reset when the device reboots. |
| `ts` | Required ISO-8601 with timezone, the **device's** clock. Reject if more than 5 minutes in the future (relative to server time). Old timestamps are VALID (offline replay). |
| `metrics.temperature` | Required number, −40 to 125 (the C3 sends its internal chip temperature) |
| `metrics.humidity` | Optional number, 0 to 100 |
| `metrics.battery_v` | Optional number, 0 to 15 |
| `metrics.rssi` | Optional number, −120 to 0 (dBm) |
| `metrics.free_heap`, `min_free_heap` | Optional integers ≥ 0, bytes. Rule: `min_free_heap ≤ free_heap` |
| `metrics.uptime_s` | Optional integer ≥ 0, seconds since this boot |
| `metrics.wifi_reconnects` | Optional integer ≥ 0, a counter since this boot |
| `boot_id` (top level) | Optional integer ≥ 1. Goes up by one on every boot; stored in the device's flash |
| `reset_reason` (top level) | Optional string: why the device last booted (`poweron`, `sw`, `panic`, `int_wdt`, `task_wdt`, `wdt`, `brownout`, `deepsleep`, `ext`, `unknown`) |
| other keys in `metrics` | Your decision: reject the message, or accept and store the extra keys. Write down which you chose and why |

Example from the C3:
```json
{
  "msg_id": "c3-health-01-17-305",
  "device_id": "c3-health-01",
  "seq": 305,
  "boot_id": 17,
  "reset_reason": "sw",
  "ts": "2026-10-19T05:10:02.000+00:00",
  "metrics": { "temperature": 41.2, "rssi": -58, "free_heap": 214532, "min_free_heap": 198220,
               "uptime_s": 1525, "wifi_reconnects": 0 }
}
```

## Command payload (Task 0e / Task 3)

```json
{ "cmd_id": "c-123", "type": "set_interval", "args": { "seconds": 5 } }
{ "cmd_id": "c-124", "type": "led",          "args": { "r": 0, "g": 40, "b": 0 } }
{ "cmd_id": "c-125", "type": "stress",       "args": { "seconds": 30 } }
{ "cmd_id": "c-126", "type": "leak",         "args": { "kb": 64 } }
{ "cmd_id": "c-127", "type": "free",         "args": {} }
{ "cmd_id": "c-128", "type": "reboot",       "args": {} }
```

| Type | Rules |
|---|---|
| `set_interval` | `seconds` 1–3600 |
| `led` | `r`, `g`, `b` 0–255 |
| `stress` | `seconds` 1–60. The device must stay connected while it runs |
| `leak` | `kb` 1–128 per command. The device caps its total leaked memory (pick a safe cap, e.g. 50% of free heap at boot) |
| `free` | releases everything `leak` allocated |
| `reboot` | the device acks **first**, then restarts |

Ack from the device: `{ "cmd_id": "c-123", "ok": true, "error": null }`. An unknown type gets `ok: false` with an error message.
A command whose `cmd_id` the device has already executed is acked again but **not executed again**.
