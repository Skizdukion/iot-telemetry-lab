#!/usr/bin/env python3
"""
IoT device fleet simulator: deliberately messy, like real devices.

Each simulated device publishes telemetry over MQTT (QoS 1) and injects the
problems your backend has to survive:

  * duplicates       - the same message published twice (QoS 1 redelivery, device retry)
  * reordering       - a message is held back and sent after the next one
  * malformed data   - broken JSON, missing fields, wrong types, out-of-range values,
                       timestamps in the future
  * offline periods  - the device drops its TCP connection uncleanly (so the broker
                       fires its Last Will), buffers readings locally, then replays
                       the backlog when it reconnects (late + out-of-order data)
  * clock skew       - optional per-device clock offset
  * spikes           - occasional (valid) temperature spikes, for alerting

Everything that SHOULD end up in your database is written to <out>/truth.jsonl.
Messages that parsed but MUST be rejected are written to <out>/rejected.jsonl.
Use tools/verify.py to compare your DB against these files.

Usage:
  python tools/simulator.py --devices 20 --duration 60
  python tools/simulator.py --dry-run --devices 2 --duration 5      # no broker, prints
  python tools/simulator.py --replay-http http://localhost:8000/ingest/batch   # Task 5
"""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

TEMP_MIN, TEMP_MAX = -40.0, 125.0  # sensor's physical range. Outside = invalid.


# --------------------------------------------------------------------------- publishers
class DryRunPublisher:
    def __init__(self, device_id: str, args, will_topic: str):
        self.device_id = device_id
        self.will_topic = will_topic

    def connect(self):
        print(f"[{self.device_id}] CONNECT (will -> {self.will_topic}=offline)")

    def publish(self, topic: str, payload: str, retain: bool = False):
        print(f"PUB {topic}{' [retained]' if retain else ''} {payload}")

    def drop(self):
        print(f"[{self.device_id}] UNCLEAN DISCONNECT (broker would publish LWT)")

    def close(self):
        print(f"[{self.device_id}] DISCONNECT")


class MqttPublisher:
    def __init__(self, device_id: str, args, will_topic: str):
        import paho.mqtt.client as mqtt  # imported lazily so --dry-run needs no deps

        self._mqtt = mqtt
        self.device_id = device_id
        self.args = args
        self.will_topic = will_topic
        self.client = None
        self._pending = []

    def connect(self):
        mqtt = self._mqtt
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.device_id)
        c.will_set(self.will_topic, payload="offline", qos=1, retain=True)
        c.connect(self.args.host, self.args.port, keepalive=10)
        c.loop_start()
        self.client = c

    def publish(self, topic: str, payload: str, retain: bool = False):
        info = self.client.publish(topic, payload, qos=1, retain=retain)
        self._pending.append(info)
        if len(self._pending) > 200:
            self._flush()

    def _flush(self):
        for info in self._pending:
            try:
                info.wait_for_publish(timeout=5)
            except Exception:
                pass
        self._pending.clear()

    def drop(self):
        """Kill the TCP socket without sending DISCONNECT, so the broker publishes the LWT."""
        self._flush()
        c = self.client
        c.loop_stop()
        try:
            sock = c.socket()
            if sock:
                sock.close()
        except Exception:
            pass
        self.client = None

    def close(self):
        self._flush()
        self.client.disconnect()
        self.client.loop_stop()


# --------------------------------------------------------------------------- recorder
class Recorder:
    def __init__(self, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)
        self._lock = threading.Lock()
        self._truth = open(os.path.join(out_dir, "truth.jsonl"), "w")
        self._rejected = open(os.path.join(out_dir, "rejected.jsonl"), "w")
        self.stats = {k: 0 for k in (
            "valid", "duplicates", "reordered", "malformed", "offline_events",
            "replayed", "spikes", "http_batches")}

    def truth(self, msg: dict):
        with self._lock:
            self._truth.write(json.dumps({k: msg[k] for k in ("msg_id", "device_id", "seq", "ts")}) + "\n")
            self.stats["valid"] += 1

    def rejected(self, msg_id: str, reason: str):
        with self._lock:
            self._rejected.write(json.dumps({"msg_id": msg_id, "reason": reason}) + "\n")

    def inc(self, key: str, n: int = 1):
        with self._lock:
            self.stats[key] += n

    def close(self):
        self._truth.close()
        self._rejected.close()


# --------------------------------------------------------------------------- device
class Device(threading.Thread):
    def __init__(self, idx: int, args, rec: Recorder, rng_seed: int):
        super().__init__(daemon=True)
        self.args = args
        self.rec = rec
        self.rng = random.Random(rng_seed)
        self.device_id = f"dev-{idx:03d}"
        base = f"{args.site}/devices/{self.device_id}"
        self.t_topic = f"{base}/telemetry"
        self.s_topic = f"{base}/status"
        self.skew = timedelta(seconds=self.rng.uniform(-args.skew_max, args.skew_max))
        self.temp_offset = self.rng.uniform(-3, 3)
        self.battery = self.rng.uniform(3.9, 4.2)
        self.seq = 0
        pub_cls = DryRunPublisher if args.dry_run else MqttPublisher
        self.pub = pub_cls(self.device_id, args, self.s_topic)

    # -- data generation
    def _reading(self) -> dict:
        self.seq += 1
        now = datetime.now(timezone.utc) + self.skew
        temp = 25 + self.temp_offset + 3 * math.sin(time.time() / 60) + self.rng.gauss(0, 0.3)
        if self.rng.random() < self.args.spike_rate:
            temp = self.rng.uniform(80, 95)
            self.rec.inc("spikes")
        self.battery = max(3.3, self.battery - self.rng.uniform(0, 0.0005))
        return {
            "msg_id": str(uuid.uuid4()),
            "device_id": self.device_id,
            "seq": self.seq,
            "ts": now.isoformat(timespec="milliseconds"),
            "metrics": {
                "temperature": round(temp, 2),
                "humidity": round(self.rng.uniform(40, 70), 1),
                "battery_v": round(self.battery, 3),
            },
        }

    def _malformed(self) -> str:
        kind = self.rng.choice(["bad_json", "missing_field", "wrong_type", "out_of_range", "future_ts"])
        self.rec.inc("malformed")
        if kind == "bad_json":
            return '{"device_id": "' + self.device_id + '", "metrics": {temperature: 21'
        msg = self._reading()
        self.seq -= 1  # a malformed message does not consume a sequence number
        if kind == "missing_field":
            del msg["device_id"]
        elif kind == "wrong_type":
            msg["metrics"]["temperature"] = "hot"
        elif kind == "out_of_range":
            msg["metrics"]["temperature"] = 999.0
        elif kind == "future_ts":
            msg["ts"] = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        self.rec.rejected(msg["msg_id"], kind)
        return json.dumps(msg)

    # -- sending
    def _send_valid(self, msg: dict):
        payload = json.dumps(msg)
        self.pub.publish(self.t_topic, payload)
        if self.rng.random() < self.args.dup_rate:
            self.pub.publish(self.t_topic, payload)  # exact same bytes, same msg_id
            self.rec.inc("duplicates")

    def _replay_http(self, batch: list[dict]):
        body = json.dumps({"readings": batch}).encode()
        sends = 2 if self.rng.random() < self.args.dup_rate else 1  # client retry after "timeout"
        for _ in range(sends):
            for attempt in range(3):
                try:
                    req = urllib.request.Request(
                        self.args.replay_http, data=body,
                        headers={"Content-Type": "application/json"}, method="POST")
                    urllib.request.urlopen(req, timeout=5).read()
                    self.rec.inc("http_batches")
                    break
                except (urllib.error.URLError, OSError) as e:
                    print(f"[{self.device_id}] HTTP replay failed ({e}), attempt {attempt + 1}/3")
                    time.sleep(1)

    def run(self):
        a = self.args
        self.pub.connect()
        self.pub.publish(self.s_topic, "online", retain=True)
        held: dict | None = None
        offline_until = 0.0
        backlog: list[dict] = []
        end = time.time() + a.duration

        while time.time() < end:
            tick_start = time.time()

            # ---- offline handling
            if offline_until and tick_start >= offline_until:
                self.pub.connect()
                self.pub.publish(self.s_topic, "online", retain=True)
                if backlog:
                    self.rec.inc("replayed", len(backlog))
                    if a.replay_http:
                        for i in range(0, len(backlog), 50):
                            self._replay_http(backlog[i:i + 50])
                    else:
                        for m in backlog:
                            self._send_valid(m)
                backlog = []
                offline_until = 0.0
            elif not offline_until and self.rng.random() < a.offline_rate:
                self.rec.inc("offline_events")
                if held:
                    self._send_valid(held)
                    held = None
                self.pub.drop()
                offline_until = tick_start + self.rng.uniform(0.5, 1.5) * a.offline_seconds

            # ---- produce one reading
            if self.rng.random() < a.malformed_rate and not offline_until:
                self.pub.publish(self.t_topic, self._malformed())
            else:
                msg = self._reading()
                self.rec.truth(msg)
                if offline_until:
                    backlog.append(msg)
                elif held is not None:
                    self._send_valid(msg)   # newer one first...
                    self._send_valid(held)  # ...then the older one
                    held = None
                elif self.rng.random() < a.reorder_rate:
                    held = msg
                    self.rec.inc("reordered")
                else:
                    self._send_valid(msg)

            time.sleep(max(0.0, a.interval - (time.time() - tick_start)))

        # ---- shutdown: flush whatever is left
        if offline_until:
            self.pub.connect()
            for m in backlog:
                self._send_valid(m)
            self.rec.inc("replayed", len(backlog))
        if held:
            self._send_valid(held)
        self.pub.publish(self.s_topic, "offline", retain=True)
        self.pub.close()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--site", default="site-1")
    p.add_argument("--devices", type=int, default=20)
    p.add_argument("--interval", type=float, default=1.0, help="seconds between readings per device")
    p.add_argument("--duration", type=float, default=60, help="seconds to run")
    p.add_argument("--dup-rate", type=float, default=0.05)
    p.add_argument("--reorder-rate", type=float, default=0.05)
    p.add_argument("--malformed-rate", type=float, default=0.02)
    p.add_argument("--offline-rate", type=float, default=0.01, help="chance per tick to go offline")
    p.add_argument("--offline-seconds", type=float, default=10)
    p.add_argument("--skew-max", type=float, default=0.0, help="max device clock skew, seconds")
    p.add_argument("--spike-rate", type=float, default=0.005)
    p.add_argument("--replay-http", default=None, help="POST offline backlog here instead of MQTT (Task 5)")
    p.add_argument("--out", default="sim_out")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--clean", action="store_true", help="disable all chaos (happy path)")
    p.add_argument("--dry-run", action="store_true", help="print instead of publishing")
    a = p.parse_args()
    if a.clean:
        a.dup_rate = a.reorder_rate = a.malformed_rate = a.offline_rate = 0.0

    rec = Recorder(a.out)
    devices = [Device(i + 1, a, rec, a.seed * 1000 + i) for i in range(a.devices)]
    print(f"Simulating {a.devices} devices for {a.duration}s -> {'stdout' if a.dry_run else f'{a.host}:{a.port}'}")
    for d in devices:
        d.start()
        time.sleep(0.01)
    for d in devices:
        d.join()
    rec.close()
    print("\nDone. Stats:", json.dumps(rec.stats, indent=2))
    print(f"Ground truth: {a.out}/truth.jsonl   Must-reject: {a.out}/rejected.jsonl")


if __name__ == "__main__":
    main()
