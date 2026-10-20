"""Tests for katas.py. Plain pytest, no plugins (async tests use asyncio.run)."""
import asyncio
import itertools
import time

import pytest

from katas import (TokenBucket, TTLDeduper, collect_batch, find_gaps, merge_sorted,
                   moving_average, retry)


class FakeClock:
    def __init__(self, t: float = 0.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float):
        self.t += dt


# 1 moving_average --------------------------------------------------------------
def test_moving_average_basic():
    assert list(moving_average([1, 2, 3, 4], 2)) == [1.0, 1.5, 2.5, 3.5]


def test_moving_average_window_larger_than_input():
    assert list(moving_average([2, 4], 10)) == [2.0, 3.0]


def test_moving_average_window_one():
    assert list(moving_average([5, -1, 3], 1)) == [5.0, -1.0, 3.0]


def test_moving_average_is_lazy_on_infinite_stream():
    out = list(itertools.islice(moving_average(itertools.count(), 3), 5))
    assert out == pytest.approx([0.0, 0.5, 1.0, 2.0, 3.0])


def test_moving_average_invalid_window():
    with pytest.raises(ValueError):
        list(moving_average([1, 2], 0))


# 2 TokenBucket -------------------------------------------------------------------
def test_token_bucket_starts_full_then_empties():
    clk = FakeClock()
    tb = TokenBucket(rate=1, capacity=3, clock=clk)
    assert [tb.allow() for _ in range(4)] == [True, True, True, False]


def test_token_bucket_refills_over_time():
    clk = FakeClock()
    tb = TokenBucket(rate=2, capacity=2, clock=clk)
    assert tb.allow() and tb.allow()
    assert not tb.allow()
    clk.advance(0.5)  # +1 token
    assert tb.allow()
    assert not tb.allow()


def test_token_bucket_never_exceeds_capacity():
    clk = FakeClock()
    tb = TokenBucket(rate=10, capacity=2, clock=clk)
    clk.advance(100)
    assert [tb.allow() for _ in range(3)] == [True, True, False]


def test_token_bucket_failed_allow_takes_nothing():
    clk = FakeClock()
    tb = TokenBucket(rate=1, capacity=3, clock=clk)
    assert not tb.allow(cost=5)
    assert tb.allow(cost=3)


# 3 merge_sorted --------------------------------------------------------------------
def test_merge_sorted_basic():
    assert list(merge_sorted([1, 4, 7], [2, 5], [0, 3, 6, 8])) == list(range(9))


def test_merge_sorted_empty_streams():
    assert list(merge_sorted([], [1], [])) == [1]
    assert list(merge_sorted()) == []


def test_merge_sorted_infinite():
    evens = itertools.count(0, 2)
    odds = itertools.count(1, 2)
    assert list(itertools.islice(merge_sorted(evens, odds), 6)) == [0, 1, 2, 3, 4, 5]


def test_merge_sorted_key_with_uncomparable_items():
    a = [{"ts": 1, "d": "a"}, {"ts": 3, "d": "a"}]
    b = [{"ts": 1, "d": "b"}, {"ts": 2, "d": "b"}]  # tie on ts=1, dicts are not comparable
    out = list(merge_sorted(a, b, key=lambda r: r["ts"]))
    assert [r["ts"] for r in out] == [1, 1, 2, 3]


# 4 TTLDeduper ------------------------------------------------------------------------
def test_deduper_detects_duplicate_within_ttl():
    clk = FakeClock()
    d = TTLDeduper(ttl=10, clock=clk)
    assert d.is_duplicate("a") is False
    clk.advance(5)
    assert d.is_duplicate("a") is True


def test_deduper_forgets_after_ttl():
    clk = FakeClock()
    d = TTLDeduper(ttl=10, clock=clk)
    d.is_duplicate("a")
    clk.advance(11)
    assert d.is_duplicate("a") is False


def test_deduper_evicts_expired_ids():
    clk = FakeClock()
    d = TTLDeduper(ttl=10, clock=clk)
    for i in range(1000):
        d.is_duplicate(f"id-{i}")
    assert len(d) == 1000
    clk.advance(11)
    d.is_duplicate("fresh")
    assert len(d) == 1


# 5 find_gaps --------------------------------------------------------------------------
def test_find_gaps_basic():
    assert find_gaps([0, 1, 2, 10, 11], expected_interval=1) == [(2, 10)]


def test_find_gaps_unsorted_with_duplicates():
    ts = [11, 2, 0, 10, 1, 1, 2, 30, 31]
    assert find_gaps(ts, expected_interval=1) == [(2, 10), (11, 30)]


def test_find_gaps_tolerance():
    assert find_gaps([0, 1.4, 2.8], expected_interval=1, tolerance=1.5) == []
    assert find_gaps([0, 1.6], expected_interval=1, tolerance=1.5) == [(0, 1.6)]


def test_find_gaps_trivial():
    assert find_gaps([], 1) == []
    assert find_gaps([5], 1) == []


# 6 collect_batch -----------------------------------------------------------------------
def test_collect_batch_returns_when_full():
    async def go():
        q = asyncio.Queue()
        for i in range(10):
            q.put_nowait(i)
        t0 = time.monotonic()
        batch = await collect_batch(q, max_size=4, max_wait=5)
        return batch, time.monotonic() - t0, q.qsize()

    batch, elapsed, left = asyncio.run(go())
    assert batch == [0, 1, 2, 3]
    assert elapsed < 0.5
    assert left == 6


def test_collect_batch_returns_after_max_wait():
    async def go():
        q = asyncio.Queue()
        q.put_nowait("a")
        q.put_nowait("b")
        t0 = time.monotonic()
        batch = await collect_batch(q, max_size=100, max_wait=0.2)
        return batch, time.monotonic() - t0

    batch, elapsed = asyncio.run(go())
    assert batch == ["a", "b"]
    assert 0.15 <= elapsed < 0.6


def test_collect_batch_waits_for_first_item_and_includes_late_items():
    async def go():
        q = asyncio.Queue()

        async def producer():
            await asyncio.sleep(0.3)  # before the first item: no timer running yet
            q.put_nowait(1)
            await asyncio.sleep(0.1)  # inside the window: must be included
            q.put_nowait(2)
            await asyncio.sleep(0.5)  # after the window: must NOT be included
            q.put_nowait(3)

        task = asyncio.create_task(producer())
        batch = await collect_batch(q, max_size=100, max_wait=0.3)
        await task
        return batch

    assert asyncio.run(go()) == [1, 2]


# 7 retry --------------------------------------------------------------------------------
def test_retry_succeeds_after_failures_with_backoff():
    calls, sleeps = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 4:
            raise ConnectionError("nope")
        return "ok"

    assert retry(flaky, attempts=5, base_delay=0.1, max_delay=0.3, sleep=sleeps.append) == "ok"
    assert sleeps == pytest.approx([0.1, 0.2, 0.3])


def test_retry_reraises_last_exception_and_no_sleep_after_final():
    sleeps, n = [], [0]

    def always_fails():
        n[0] += 1
        raise TimeoutError(f"fail {n[0]}")

    with pytest.raises(TimeoutError, match="fail 3"):
        retry(always_fails, attempts=3, base_delay=1, max_delay=10, sleep=sleeps.append)
    assert sleeps == [1, 2]


def test_retry_does_not_retry_other_exceptions():
    sleeps, n = [], [0]

    def bad():
        n[0] += 1
        raise ValueError("bug, not transient")

    with pytest.raises(ValueError):
        retry(bad, attempts=5, base_delay=1, max_delay=1, retry_on=(ConnectionError,), sleep=sleeps.append)
    assert n[0] == 1 and sleeps == []
