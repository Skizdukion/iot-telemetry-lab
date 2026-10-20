"""
Python warm-up katas: the kind of thing you get in a 20-30 min live-coding round
for a telemetry/backend role. Each one maps to a real problem in the main lab.

Rules:
  * Standard library only. No numpy/pandas.
  * Aim for 15-25 minutes each. Talk out loud (or write comments) about edge cases FIRST.
  * Run:  pytest katas/ -q        (or one:  pytest katas/ -q -k token)
  * Write the code yourself. If you use AI, log it in AI_LOG.md (see README).
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Iterable, Iterator


# 1 ---------------------------------------------------------------------------
def moving_average(values: Iterable[float], window: int) -> Iterator[float]:
    """Yield, after each input value, the mean of the last `window` values seen
    (or of all values so far, if fewer than `window` have arrived).

    Must be O(1) per item. Do NOT re-sum the window each time.
    Must work on an infinite generator (be lazy).
    Raise ValueError if window < 1.

    >>> list(moving_average([1, 2, 3, 4], 2))
    [1.0, 1.5, 2.5, 3.5]
    """
    raise NotImplementedError


# 2 ---------------------------------------------------------------------------
class TokenBucket:
    """Rate limiter: `rate` tokens are added per second, up to `capacity`.
    The bucket starts full. allow(cost) takes `cost` tokens if available and
    returns True, otherwise returns False and takes nothing.

    `clock` is injectable so tests can control time (default time.monotonic).
    Use case: limit each device to N messages/second at the ingest layer.
    """

    def __init__(self, rate: float, capacity: float, clock: Callable[[], float] = time.monotonic):
        raise NotImplementedError

    def allow(self, cost: float = 1.0) -> bool:
        raise NotImplementedError


# 3 ---------------------------------------------------------------------------
def merge_sorted(*streams: Iterable[Any], key: Callable[[Any], Any] | None = None) -> Iterator[Any]:
    """Lazily merge any number of individually-sorted iterables into one sorted stream.

    Must work when the streams are infinite generators (so you cannot list() them).
    Hint: heapq. Be careful with ties when the items themselves are not comparable.
    Use case: merging per-device streams into one time-ordered stream.
    (Yes, heapq.merge exists. Implement it yourself.)
    """
    raise NotImplementedError


# 4 ---------------------------------------------------------------------------
class TTLDeduper:
    """Remember msg_ids for `ttl` seconds.

    is_duplicate(msg_id):
      - returns False the first time an id is seen (and remembers it)
      - returns True if the same id is seen again within `ttl` seconds of the first sighting
      - after `ttl` has passed, the id is forgotten (seeing it again returns False)

    Memory must stay bounded: expired ids must actually be evicted, not just ignored.
    __len__ returns how many ids are currently remembered (after evicting expired ones).
    Use case: drop QoS-1 redeliveries in the ingest worker before hitting the DB.
    """

    def __init__(self, ttl: float, clock: Callable[[], float] = time.monotonic):
        raise NotImplementedError

    def is_duplicate(self, msg_id: str) -> bool:
        raise NotImplementedError

    def __len__(self) -> int:
        raise NotImplementedError


# 5 ---------------------------------------------------------------------------
def find_gaps(timestamps: Iterable[float], expected_interval: float,
              tolerance: float = 1.5) -> list[tuple[float, float]]:
    """Given reading timestamps from ONE device (seconds; may be unsorted and contain
    duplicates), return the gaps: (last_ts_before_gap, first_ts_after_gap) for every
    pair of consecutive distinct timestamps more than expected_interval * tolerance apart.
    Return gaps sorted by start time. Empty or single-item input gives [].

    >>> find_gaps([0, 1, 2, 10, 11], expected_interval=1)
    [(2, 10)]
    """
    raise NotImplementedError


# 6 ---------------------------------------------------------------------------
async def collect_batch(queue: asyncio.Queue, max_size: int, max_wait: float) -> list:
    """Wait (indefinitely) for the first item. Then keep collecting until either
    `max_size` items are in the batch, or `max_wait` seconds have passed since
    the FIRST item arrived. Return the batch.

    Use case: the ingest worker batches DB inserts ("500 rows or 1 second, whichever first").
    Hint: asyncio.wait_for / loop.time().
    """
    raise NotImplementedError


# 7 ---------------------------------------------------------------------------
def retry(fn: Callable[[], Any], attempts: int, base_delay: float, max_delay: float,
          retry_on: tuple[type[BaseException], ...] = (Exception,),
          sleep: Callable[[float], None] = time.sleep) -> Any:
    """Call fn() up to `attempts` times. On an exception that is an instance of
    `retry_on`, sleep and try again. The delay before retry i (i = 0, 1, 2, ...) is
    min(max_delay, base_delay * 2**i). Return fn's result on success.
    If all attempts fail, re-raise the LAST exception.
    Exceptions not in `retry_on` propagate immediately with no retry.
    Do not sleep after the final failed attempt.
    """
    raise NotImplementedError
