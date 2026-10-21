"""Unit tests for the contract validation (no MQTT, no DB needed: fast, run them on every commit).

Run from the repo root:   pytest -q
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from app.ingest.models import parse_topic, validate

TOPIC = "site-1/devices/c3-health-01/telemetry"


def payload(**overrides):
    """A valid C3 message. Tests change one thing at a time."""
    msg = {
        "msg_id": "c3-health-01-17-1",
        "device_id": "c3-health-01",
        "seq": 1,
        "boot_id": 17,
        "reset_reason": "sw",
        "ts": datetime.now(timezone.utc).isoformat(),
        "metrics": {"temperature": 41.2, "rssi": -58, "free_heap": 214532, "min_free_heap": 198220},
    }
    metrics = overrides.pop("metrics", {})
    msg.update(overrides)
    msg["metrics"].update(metrics)
    return json.dumps(msg).encode()


def test_valid_message_is_accepted():
    msg, reason = validate(TOPIC, payload())
    assert reason is None
    assert msg.device_id == "c3-health-01"
    assert msg.metrics.free_heap == 214532


def test_boot_id_is_optional():
    raw = json.loads(payload())
    del raw["boot_id"]
    msg, reason = validate(TOPIC, json.dumps(raw).encode())
    assert reason is None and msg.boot_id is None


@pytest.mark.parametrize("bad, expected_in_reason", [
    ({"metrics": {"temperature": 999}}, "temperature"),           # out of range
    ({"metrics": {"temperature": "hot"}}, "temperature"),         # wrong type
    ({"metrics": {"temperature": "25"}}, "temperature"),          # strict: no string -> number coercion
    ({"seq": 0}, "seq"),                                          # seq starts at 1
    ({"ts": "2026-10-20T05:10:02"}, "ts"),                        # no timezone
    ({"ts": "1970-01-01T00:00:05+00:00"}, "too old"),             # unsynced device clock
    ({"metrics": {"free_heap": 10, "min_free_heap": 20}}, "min_free_heap"),
    ({"metrics": {"foo": "bar"}}, "foo"),                         # extra metric must be numeric
])
def test_invalid_messages_are_rejected_with_a_reason(bad, expected_in_reason):
    msg, reason = validate(TOPIC, payload(**bad))
    assert msg is None
    assert expected_in_reason in reason


def test_future_timestamp_rejected_but_small_clock_drift_allowed():
    now = datetime.now(timezone.utc)
    _, reason = validate(TOPIC, payload(ts=(now + timedelta(hours=1)).isoformat()))
    assert reason and "future" in reason
    msg, reason = validate(TOPIC, payload(ts=(now + timedelta(minutes=2)).isoformat()))
    assert reason is None, "a device clock 2 minutes fast is normal drift, not an error"


def test_old_timestamp_is_valid_offline_replay():
    old = datetime.now(timezone.utc) - timedelta(days=2)
    msg, reason = validate(TOPIC, payload(ts=old.isoformat()))
    assert reason is None


def test_broken_json_is_rejected():
    msg, reason = validate(TOPIC, b'{"device_id": "c3-health-01", "metrics": {temperature: 21')
    assert msg is None and reason


def test_device_must_match_topic():
    msg, reason = validate("site-1/devices/someone-else/telemetry", payload())
    assert msg is None and "does not match" in reason


def test_parse_topic():
    assert parse_topic(TOPIC) == ("site-1", "c3-health-01")
    with pytest.raises(ValueError):
        parse_topic("site-1/devices/c3-health-01/status")
