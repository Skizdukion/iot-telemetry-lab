# Task 2: REST API, tests, Docker (speed-run version)

## 1. Run it

```bash
pip install -r app/requirements.txt            # adds fastapi, uvicorn, pytest
pytest -q                                      # unit tests for the validation: should pass (15 tests)
uvicorn app.api.main:app --reload --port 8002  # keep the worker + simulator/C3 running so there's data
```
Open **http://localhost:8002/docs** and try every endpoint from the browser.

## 2. Code tour (`app/api/main.py`)

| Concept (interview words) | Where |
|---|---|
| `async def` everywhere, because every slow call is awaited (asyncpg) | all endpoints |
| One connection pool, created at startup and closed at shutdown | `lifespan()` |
| Dependency injection with cleanup: borrow a connection per request | `get_conn()` + `Depends(get_conn)` |
| Response models: validation + OpenAPI docs from type hints | `Device`, `Reading`, `ReadingPage`… and `response_model=` |
| One consistent error format | `error_body()` + the two `exception_handler`s |
| Status codes: 404 unknown device, 422 bad input | `ensure_device()`, `time_range()`, `Query(ge=, le=)` |
| Cursor pagination, and why not OFFSET | `telemetry()` docstring, `encode_cursor` / `decode_cursor` |
| Time-series aggregation | `telemetry_agg()`: `time_bucket` |
| Querying a JSONB metric | `(metrics->>'free_heap')::float8` in `telemetry_agg()` |
| Gauge vs counter (why avg is wrong for counters) | `telemetry_agg()` docstring |
| Online/offline from last_seen (the backup to LWT) | `DEVICE_SQL`, `OFFLINE_AFTER` |
| Reboot history | `boots()` |
| Unit tests, parametrize, fixtures-by-function | `tests/test_models.py` |
| Multi-stage Docker build, layer caching, non-root | `app/Dockerfile` |

## 3. Bug hunt: 3 planted bugs

**Bug A: unknown device gives 500.**
- Reproduce: `curl -i localhost:8002/devices/nope`. You get `500 Internal Server Error`; it should be a 404 with our error body.
  Compare with `curl -i localhost:8002/devices/nope/latest`, which is correct.
- Concept: status codes, and why a 500 for user input is a bug (it pages the on-call engineer for nothing).

**Bug B: page 2 is wrong.**
- Reproduce:
  ```bash
  curl -s "localhost:8002/devices/c3-health-01/telemetry?limit=5" | python -m json.tool   # note msg_ids + next_cursor
  curl -s "localhost:8002/devices/c3-health-01/telemetry?limit=5&cursor=PASTE_IT_HERE" | python -m json.tool
  ```10010
  Page 2 should continue with **older** rows. Look at what you actually get.
- Concept: cursor pagination ("continue after this exact row" must match the sort direction).

**Bug C: a security hole. Find it by reading.**
- Look at `telemetry_agg()`. An allow-list exists in this file… is it used everywhere it should be?
- Question to answer: what could someone send as `?metric=...`, and what would happen? Why can't you just pass
  the column name as a `$1` parameter like the other values?
- Concept: SQL injection, parameters vs identifiers, allow-lists.

## 4. Optional: run everything in Docker (5 minutes, good talking point)

Add to `docker-compose.yml` under `services:`
```yaml
  api:
    build: { context: ., dockerfile: app/Dockerfile }
    environment:
      DATABASE_URL: postgresql://iot:iot@db:5432/iot
    ports: ["8002:8002"]
    depends_on: { db: { condition: service_healthy } }

  worker:
    build: { context: ., dockerfile: app/Dockerfile }
    command: ["python", "-m", "app.ingest.worker"]
    environment:
      DATABASE_URL: postgresql://iot:iot@db:5432/iot
      MQTT_HOST: mosquitto
    depends_on: { db: { condition: service_healthy }, mosquitto: { condition: service_started } }
```
Then `docker compose up -d --build`. Inside Docker, services reach each other **by service name**
(`db`, `mosquitto`), not `localhost`. That's a classic interview question too.
