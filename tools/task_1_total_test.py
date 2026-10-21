"""Task 1 "total" chaos test: DB outage + worker restart in one simulator run, then verify.

Automates the manual recipe:
  1. TRUNCATE telemetry, dead_letters
  2. start the worker, then the simulator (--devices 20 --duration 120)
  3. ~30 s: docker compose stop db, wait ~30 s, docker compose start db
  4. ~80 s: Ctrl+C (SIGINT) the worker, wait ~10 s, start it again
  5. after the simulator ends: let the worker drain, stop it gracefully, run verify.py

Run from the repo root with your venv active:
  python tools/task_1_total_test.py
  python tools/task_1_total_test.py --duration 60 --db-down-at 15 --worker-down-at 40

Worker and simulator output go to sim_out/logs/ (use --show-worker to also stream the worker log).
Exit code is verify.py's: 0 = PASS, 1 = FAIL.
"""
import argparse
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable
T0 = time.monotonic()


def log(msg: str) -> None:
    print(f"[{time.monotonic() - T0:6.1f}s] {msg}", flush=True)


def compose(*args: str, check: bool = True, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, check=check, **kw)


def psql(sql: str) -> None:
    compose("exec", "-T", "db", "psql", "-U", "iot", "-d", "iot", "-v", "ON_ERROR_STOP=1", "-c", sql,
            stdout=subprocess.DEVNULL)


def wait_db_ready(timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = compose("exec", "-T", "db", "pg_isready", "-U", "iot", "-d", "iot",
                    check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if r.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("database did not become ready in time")


class Worker:
    """python -m app.ingest.worker, stopped with SIGINT exactly like Ctrl+C."""

    def __init__(self, log_path: Path, show: bool):
        self.log_path = log_path
        self.show = show
        self.proc: subprocess.Popen | None = None
        self.starts = 0

    def start(self) -> None:
        self.starts += 1
        log(f"worker: start #{self.starts}")
        # Own session, so a Ctrl+C on this script doesn't also hit the worker behind our back.
        self.proc = subprocess.Popen([PY, "-u", "-m", "app.ingest.worker"], cwd=ROOT,
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     text=True, start_new_session=True)
        threading.Thread(target=self._pump, args=(self.proc, self.starts), daemon=True).start()

    def _pump(self, proc: subprocess.Popen, n: int) -> None:
        with open(self.log_path, "a") as f:
            f.write(f"\n===== worker start #{n} =====\n")
            for line in proc.stdout:
                f.write(line)
                f.flush()
                if self.show:
                    print(f"    [worker#{n}] {line}", end="", flush=True)

    def stop(self, timeout: float = 30) -> None:
        if not self.proc or self.proc.poll() is not None:
            return
        log("worker: SIGINT (graceful shutdown, drains the queue)")
        self.proc.send_signal(signal.SIGINT)
        try:
            rc = self.proc.wait(timeout)
            log(f"worker: exited with code {rc}")
        except subprocess.TimeoutExpired:
            log(f"worker: still running after {timeout}s, killing it (messages may be lost!)")
            self.proc.kill()
            self.proc.wait()

    def alive(self) -> bool:
        return bool(self.proc) and self.proc.poll() is None


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--devices", type=int, default=20)
    p.add_argument("--duration", type=float, default=120, help="simulator run length, seconds")
    p.add_argument("--db-down-at", type=float, default=30, help="seconds after sim start to stop the db")
    p.add_argument("--db-down-for", type=float, default=30)
    p.add_argument("--worker-down-at", type=float, default=80, help="seconds after sim start to Ctrl+C the worker")
    p.add_argument("--worker-down-for", type=float, default=10)
    p.add_argument("--no-db-outage", action="store_true")
    p.add_argument("--no-worker-restart", action="store_true")
    p.add_argument("--warmup", type=float, default=3, help="seconds between worker start and simulator start")
    p.add_argument("--settle", type=float, default=5, help="seconds to let the worker catch up after the sim ends")
    p.add_argument("--show-worker", action="store_true", help="stream worker output to the console")
    a = p.parse_args()

    log_dir = ROOT / "sim_out" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    worker_log, sim_log = log_dir / "worker.log", log_dir / "simulator.log"
    worker_log.write_text("")

    # Events, in seconds after simulator start.
    events: list[tuple[float, str]] = []
    if not a.no_db_outage:
        events += [(a.db_down_at, "db_stop"), (a.db_down_at + a.db_down_for, "db_start")]
    if not a.no_worker_restart:
        events += [(a.worker_down_at, "worker_stop"), (a.worker_down_at + a.worker_down_for, "worker_start")]
    events.sort()
    for t, name in events:
        if t >= a.duration:
            log(f"warning: {name} at {t:.0f}s is after the simulator ends ({a.duration:.0f}s)")

    worker = Worker(worker_log, a.show_worker)
    sim: subprocess.Popen | None = None
    db_stopped = False
    try:
        log("infra: docker compose up -d")
        compose("up", "-d", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_db_ready()
        log("db: TRUNCATE telemetry, dead_letters")
        psql("TRUNCATE telemetry, dead_letters;")

        worker.start()
        time.sleep(a.warmup)
        if not worker.alive():
            log(f"worker died on startup, see {worker_log}")
            return 2

        log(f"simulator: {a.devices} devices for {a.duration:.0f}s (log: {sim_log})")
        sim_t0 = time.monotonic()
        with open(sim_log, "w") as f:
            sim = subprocess.Popen([PY, "-u", "tools/simulator.py", "--devices", str(a.devices),
                                    "--duration", str(a.duration)],
                                   cwd=ROOT, stdout=f, stderr=subprocess.STDOUT)

        for t, name in events:
            delay = sim_t0 + t - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            if name == "db_stop":
                log("db: docker compose stop db")
                compose("stop", "db", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                db_stopped = True
            elif name == "db_start":
                log("db: docker compose start db")
                compose("start", "db", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                db_stopped = False
            elif name == "worker_stop":
                worker.stop()
            elif name == "worker_start":
                worker.start()
            if not worker.alive() and name != "worker_stop":
                log(f"warning: worker is not running (crashed?), see {worker_log}")

        rc = sim.wait()
        log(f"simulator: finished with code {rc}")
        if rc != 0:
            log(f"simulator failed, see {sim_log}")
            return 2

        wait_db_ready()
        log(f"waiting {a.settle:.0f}s for the worker to catch up")
        time.sleep(a.settle)
        if not worker.alive():
            log(f"warning: worker is not running at the end, see {worker_log}")
        worker.stop()

        log("verify: python tools/verify.py")
        print("-" * 60, flush=True)
        result = subprocess.run([PY, "tools/verify.py"], cwd=ROOT)
        print("-" * 60, flush=True)
        log(f"worker log: {worker_log}")
        return result.returncode
    except KeyboardInterrupt:
        log("interrupted, cleaning up")
        return 130
    finally:
        if sim and sim.poll() is None:
            sim.terminate()
        worker.stop()
        if db_stopped:
            log("db: restoring (docker compose start db)")
            compose("start", "db", check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    sys.exit(main())
