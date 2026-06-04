"""Tests for FixedWindowCounter — a per-origin fixed-window rate limiter.

For each origin, `tokens` requests are allowed per fixed, epoch-aligned window
of `window_ms`; the count resets to full at each window boundary. acquire(origin)
spends a token if one remains in that origin's current window, otherwise sleeps
until the next boundary and spends one there. Origins have independent budgets.

Deterministic via an injected fake ms clock: now() reads a virtual time, and
sleep(ms) just advances it (never really waits). All units are milliseconds.

Contract:
  FixedWindowCounter(tokens, window_ms, *, now=_epoch_ms, sleep=_sleep_ms)
  async acquire(origin)
"""

import asyncio

import pytest

from pub_crawler.fixed_window_counter import FixedWindowCounter

ORIGIN = "https://mastodon.example"
ORIGIN_A = "https://a.example"
ORIGIN_B = "https://b.example"


class FakeClock:
    """Virtual ms clock: now() reads it, sleep(ms) advances it (no real wait)."""

    def __init__(self, start_ms=0):
        self.t = start_ms

    def now(self):
        return self.t

    async def sleep(self, ms):
        assert ms >= 0
        self.t += ms


def counter(tokens, window_ms, clock):
    return FixedWindowCounter(tokens, window_ms, now=clock.now, sleep=clock.sleep)


async def test_allows_up_to_tokens_within_one_window():
    clock = FakeClock(0)
    fwc = counter(3, 1000, clock)

    for _ in range(3):
        await fwc.acquire(ORIGIN)

    # All three fit in the current window -> nobody slept.
    assert clock.t == 0


async def test_blocks_until_the_aligned_window_boundary():
    # Start 500ms into the [0, 1000) window — exhausting it should sleep to the
    # ALIGNED boundary at 1000, not to 500 + 1000 (which a relative window does).
    clock = FakeClock(500)
    fwc = counter(2, 1000, clock)

    await fwc.acquire(ORIGIN)
    await fwc.acquire(ORIGIN)
    assert clock.t == 500  # window's 2 tokens spent, no sleep yet

    await fwc.acquire(ORIGIN)  # exhausted -> wait to the epoch-aligned boundary
    assert clock.t == 1000


async def test_refills_each_new_window():
    clock = FakeClock(0)
    fwc = counter(2, 1000, clock)

    await fwc.acquire(ORIGIN)
    await fwc.acquire(ORIGIN)  # window 0 spent

    clock.t = 1000  # time passes into window 1

    await fwc.acquire(ORIGIN)
    await fwc.acquire(ORIGIN)  # fresh 2 tokens -> immediate
    assert clock.t == 1000  # no sleeping needed


async def test_origins_have_independent_budgets():
    clock = FakeClock(0)
    fwc = counter(2, 1000, clock)

    # Exhaust origin A's window.
    await fwc.acquire(ORIGIN_A)
    await fwc.acquire(ORIGIN_A)

    # Origin B has its own full budget -> immediate, no sleep.
    await fwc.acquire(ORIGIN_B)
    await fwc.acquire(ORIGIN_B)
    assert clock.t == 0


async def test_concurrent_acquirers_respect_the_limit():
    clock = FakeClock(0)
    fwc = counter(2, 1000, clock)
    completed_at = []

    async def worker():
        await fwc.acquire(ORIGIN)
        completed_at.append(clock.now())

    await asyncio.gather(*(worker() for _ in range(3)))

    # Two get tokens in window 0 (t=0); the third must wait for window 1 (t=1000).
    assert sorted(completed_at) == [0, 0, 1000]
