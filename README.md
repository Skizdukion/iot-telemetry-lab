# IoT Telemetry Lab

Hands-on prep for a Python backend + IoT telemetry interview. You build the backend
(and the ESP32 firmware) yourself. This repo gives you only the **test equipment**:

| Path | What it is | Who writes it |
|---|---|---|
| `TASKS.md` | Task specs with acceptance criteria and interview questions | given |
| `CONTRACT.md` | MQTT topics + payload rules every component follows | given |
| `docker-compose.yml` | Mosquitto, TimescaleDB, Redis | given (you add your services in Task 6) |
| `tools/simulator.py` | Fake device fleet that injects duplicates, reordering, garbage and outages | given |
| `tools/verify.py` | Checks your DB against what the simulator sent: lost / duplicated / wrongly accepted | given |
| `katas/` | 7 Python live-coding exercises: empty stubs + tests | **you** |
| `sql/` | A seeded dataset + 17 SQL questions | **you** |
| `app/` | Your backend | **you** |
| `firmware/` | Your ESP32 code | **you** |
| `AI_LOG.md` | Your log of AI use (see below) | **you** |

## Setup

```bash
docker compose up -d
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r tools/requirements.txt pytest

# Is the broker alive? Watch everything:
docker compose exec mosquitto mosquitto_sub -t '#' -v
# In another terminal, a quick happy-path run with 2 devices:
python tools/simulator.py --devices 2 --duration 10 --clean
```

**ESP32-C3 networking:** the C3 connects to your laptop's **LAN IP** (`ipconfig` on Windows, the IPv4 address of your Wi-Fi adapter),
port 1883, on the same **2.4 GHz** Wi-Fi (the C3 can't see 5 GHz networks). If it can't connect:
- Windows Defender Firewall is probably blocking inbound 1883. Allow it for **Private** networks, and make sure your Wi-Fi is set to Private.
- University and office Wi-Fi often isolate clients from each other, so a phone hotspot (set to 2.4 GHz) is the easy fix.

Keep Wi-Fi credentials and the broker IP out of git: put them in a `secrets.h` that is listed in `.gitignore`, or in `build_flags` from an untracked ini file.

## Suggested order

1. Day 1 morning: `katas/` 1–4 (warm-up, about 1.5 h), then **Task 0a–0b** (C3 health data over serial, then published to the broker).
2. Day 1 afternoon: **Task 1** (ingest + dedupe). Get `verify.py` to PASS.
3. Day 2: **Task 2** (API), the SQL drills, **Task 0c–0f**, katas 5–7.
4. Then Task 3 → 6 → 4 → 5, as time allows. Before the interview, write `DESIGN.md` (Task 7).

Running the katas: `cd katas && pytest -q`

## Using the simulator

```bash
python tools/simulator.py --help
python tools/simulator.py --devices 20 --duration 120          # default chaos
python tools/simulator.py --clean                              # no chaos, for debugging
python tools/simulator.py --dup-rate 0.3 --offline-rate 0.05   # more pain
python tools/simulator.py --dry-run --devices 1 --duration 5   # print messages, no broker needed
```
After a run: `python tools/verify.py` (TRUNCATE your telemetry table before each run).

## AI rule (practise the 3rd interview topic)

Using AI is allowed, because the interview asks **how** you use it. For every non-trivial piece of AI-generated code,
add an entry to `AI_LOG.md`:

```
## <date> – <what>
Prompt (short):
What it got right:
What was wrong or invented (wrong API, missing edge case, blocking call in async…):
How I verified: (test written, docs checked, ran it against the simulator…)
```
Three or four honest entries give you concrete, true stories to tell the interviewer.
**Katas and SQL: no AI.** Those train your live-coding ability.

## Review

Send me one task at a time: the code, or a GitHub link (I can read your repo), plus your answers to that task's
interview questions. I'll review it like an interviewer: correctness, reliability holes, Python/FastAPI idioms,
schema and SQL, tests, and how well you explain your choices.
