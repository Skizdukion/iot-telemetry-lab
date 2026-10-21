"""All settings come from environment variables (12-factor style), with dev defaults.
Same code runs on your laptop and in Docker; only the env changes."""
import os

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "+/devices/+/telemetry")  # + = exactly one topic level

# A FIXED client id + persistent session: while the worker is down, the broker keeps
# queueing QoS 1 messages for this id and delivers them when we reconnect.
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "ingest-worker-1")
MQTT_CLEAN_SESSION = os.getenv("MQTT_CLEAN_SESSION", "false").lower() == "true"

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://iot:iot@localhost:5432/iot")

BATCH_MAX_SIZE = int(os.getenv("BATCH_MAX_SIZE", "200"))      # flush at 200 messages...
BATCH_MAX_WAIT = float(os.getenv("BATCH_MAX_WAIT", "1.0"))    # ...or 1 s after the first one
QUEUE_MAX_SIZE = int(os.getenv("QUEUE_MAX_SIZE", "10000"))    # bounded = backpressure, not OOM
