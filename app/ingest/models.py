"""CONTRACT.md as code. If a message passes Telemetry.model_validate_json(), it's storable.

Pydantic v2 does the boring checks (types, required fields, ranges) from the type hints.
The custom rules (time window, min <= current, extra metrics) are validators below.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from pydantic import (AwareDatetime, BaseModel, ConfigDict, Field, ValidationError,
                      field_validator, model_validator)

OLDEST_ALLOWED = datetime(2024, 1, 1, tzinfo=timezone.utc)  # rejects "1970" readings from unsynced clocks
MAX_FUTURE = timedelta(minutes=5)


class Metrics(BaseModel):
    # strict=True: "temperature": "25" (a string) is rejected instead of silently converted.
    # extra="allow": unknown metric keys are kept (and checked below) instead of dropped.
    model_config = ConfigDict(strict=True, extra="allow")

    temperature: float = Field(ge=-40, le=125)
    humidity: Optional[float] = Field(default=None, ge=0, le=100)
    battery_v: Optional[float] = Field(default=None, ge=0, le=15)
    rssi: Optional[float] = Field(default=None, ge=-120, le=0)
    free_heap: Optional[int] = Field(default=None, ge=0)
    min_free_heap: Optional[int] = Field(default=None, ge=0)
    uptime_s: Optional[int] = Field(default=None, ge=0)
    wifi_reconnects: Optional[int] = Field(default=None, ge=0)

    @model_validator(mode="after")
    def check_rules(self) -> "Metrics":
        if self.free_heap is not None and self.min_free_heap is not None \
                and self.min_free_heap > self.free_heap:
            raise ValueError("min_free_heap must be <= free_heap")
        # Our decision for unknown keys: accept them, but only if they're numbers.
        # (bool is a subclass of int in Python, so exclude it explicitly.)
        for key, value in (self.model_extra or {}).items():
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"extra metric {key!r} must be a number")
        return self


class Telemetry(BaseModel):
    msg_id: str = Field(min_length=1, max_length=64)
    device_id: str = Field(min_length=1, max_length=64)
    seq: int = Field(ge=1)
    ts: AwareDatetime                      # must include a timezone, e.g. +00:00
    boot_id: Optional[int] = Field(default=None, ge=1)
    reset_reason: Optional[str] = Field(default=None, max_length=32)
    metrics: Metrics

    @field_validator("ts")
    @classmethod
    def ts_in_window(cls, v: datetime) -> datetime:
        now = datetime.now(timezone.utc)
        if v > now + MAX_FUTURE:
            raise ValueError("ts is in the future")
        if v < OLDEST_ALLOWED:
            raise ValueError("ts is too old (device clock not synced?)")
        return v


def parse_topic(topic: str) -> tuple[str, str]:
    """'site-1/devices/dev-001/telemetry' -> ('site-1', 'dev-001')"""
    parts = topic.split("/")
    if len(parts) != 4 or parts[1] != "devices" or parts[3] != "telemetry":
        raise ValueError(f"unexpected topic {topic!r}")
    return parts[0], parts[2]


def validate(topic: str, payload: bytes) -> tuple[Optional[Telemetry], Optional[str]]:
    """Returns (message, None) if valid, or (None, reason) if it must be dead-lettered."""
    try:
        _, topic_device = parse_topic(topic)
        msg = Telemetry.model_validate_json(payload)   # also catches broken JSON
    except ValidationError as e:
        err = e.errors()[0]
        where = ".".join(str(p) for p in err["loc"]) or "payload"
        return None, f"{where}: {err['msg']}"
    except ValueError as e:
        return None, str(e)
    if msg.device_id != topic_device:
        # A device must only publish as itself (later enforced by broker ACLs too).
        return None, f"device_id {msg.device_id!r} does not match topic device {topic_device!r}"
    return msg, None
