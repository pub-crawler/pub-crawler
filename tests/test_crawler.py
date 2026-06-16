"""Unit tests for pub_crawler.crawler -- the worker loop and the Crawler pool.

worker(name, dispatcher): the per-task drain loop.
  get() a job, dispatch() it, and
    - on success         -> done(job)            (release the lease, NOT a failure)
    - dispatch() raises  -> fail(job), continue  (record it, SKIP done, keep looping)
    - get() is None      -> return               (stop sentinel from a stopped dispatcher)
  A failed job is never marked done, a succeeded job is never failed, and one
  job's failure never stops the worker from draining the next. A raise from the
  pop/release path (get()/done()) is logged and swallowed with exponential
  backoff rather than killing the worker; only after too many consecutive such
  failures does the worker give up. CancelledError (a BaseException) is never
  swallowed, so finish()/abort() can still cancel workers.

reap(dispatcher): one recovery pass -- re-enqueue every job dispatcher.expired()
  reports as abandoned (enqueue() releases the stale lease and re-queues it).
  Returns nothing.

reap_worker(dispatcher, sleep): the looping driver -- reap(), then sleep one
  reaping window (MAX_INFLIGHT), forever. The injected sleep is the test lever:
  it records the requested delay and steps the loop deterministically (no real
  waiting). The loop survives a failing pass and stops cleanly on cancel.

Crawler(dispatcher, max_workers):
  start():  spawn max_workers tasks, each running the worker loop.
  finish(): run to completion -- await dispatcher.join() until the queue drains,
            then wind the (now-idle) workers down.
  abort():  stop gracefully -- trip the dispatcher's circuit breaker
            (dispatcher.stop()) so get() stops handing out work, let in-flight
            jobs finish, and leave any un-started jobs on the queue.
  The distinction the tests pin: finish() waits for the queue to EMPTY (so nothing
  in flight is cut off), while abort() stops NOW and leaves the backlog behind.

Fakes stand in for the real dispatcher. For the worker loop: ScriptedDispatcher
hands out a finite job list then returns the None stop sentinel (and can mark
chosen jobs' dispatch() as raising, via failing=); ThrowingDispatcher raises from
get() or done() on scripted calls, to prove the loop's guard swallows those and
keeps draining; ParkingDispatcher blocks forever in get() so a cancel has
something to interrupt. For the Crawler, the richer FakeDispatcher mirrors
get()/join()/stop(); stop() is assumed SYNCHRONOUS (sets a flag) -- flag if it
turns out async. Every await that could hang on an unmet contract is bounded by
wait_for.
"""

import asyncio

import pytest

from pub_crawler.crawler import Crawler, reap, reap_worker, worker
from pub_crawler.dispatcher import MAX_INFLIGHT


def job(n):
    return {"job_type": "actor", "tag": n}


async def _until(predicate, timeout=1.0):
    """Yield to the loop until predicate() holds, bounded so a stuck test fails."""

    async def spin():
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(spin(), timeout=timeout)


# ---------------------------------------------------------------------------
# worker(): the per-task drain loop
# ---------------------------------------------------------------------------


async def _instant_sleep(ms):
    """No-op sleep injected into worker() so its backoff never actually waits."""


class ScriptedDispatcher:
    """Hands out a finite job list, then returns the None stop sentinel forever.
    Jobs whose tag is in failing have their dispatch() raise -- exercising the
    inline fail-and-continue path, which never reaches the loop's outer guard."""

    def __init__(self, jobs, failing=()):
        self._jobs = list(jobs)
        self._failing = set(failing)  # tags whose dispatch() raises
        self.dispatched = []
        self.done_jobs = []
        self.failed_jobs = []

    async def get(self):
        return self._jobs.pop(0) if self._jobs else None

    async def dispatch(self, job):
        self.dispatched.append(job)
        if job["tag"] in self._failing:
            raise RuntimeError(f"dispatch blew up on {job['tag']}")

    async def done(self, job):
        self.done_jobs.append(job)

    async def fail(self, job):
        self.failed_jobs.append(job)


async def test_worker_marks_a_succeeded_job_done_and_never_fails_it():
    dis = ScriptedDispatcher([job("ok")])

    await asyncio.wait_for(worker("w-0", dis), timeout=1.0)

    assert dis.dispatched == [job("ok")]
    assert dis.done_jobs == [job("ok")]
    assert dis.failed_jobs == []


async def test_worker_fails_a_raised_job_and_never_marks_it_done():
    dis = ScriptedDispatcher([job("bad")], failing=["bad"])

    await asyncio.wait_for(worker("w-0", dis), timeout=1.0)

    assert dis.dispatched == [job("bad")]
    assert dis.failed_jobs == [job("bad")]
    assert dis.done_jobs == []  # the whole point: no done() on the failure path


async def test_worker_keeps_draining_after_a_failure():
    # A failure must not break the loop: the bad job is failed (not done), and
    # the worker goes on to dispatch and complete the next job.
    dis = ScriptedDispatcher([job("bad"), job("good")], failing=["bad"])

    await asyncio.wait_for(worker("w-0", dis), timeout=1.0)

    assert dis.dispatched == [job("bad"), job("good")]
    assert dis.failed_jobs == [job("bad")]
    assert dis.done_jobs == [job("good")]


async def test_worker_exits_when_get_returns_none():
    # None from get() is the stop sentinel: the worker drains real jobs, then
    # returns the moment get() yields None -- it must NOT dispatch the sentinel.
    # (A worker that ignored None would loop on it forever; wait_for turns that
    # hang into a failure.)
    dis = ScriptedDispatcher([job("a"), job("b")])

    await asyncio.wait_for(worker("w-0", dis), timeout=1.0)

    assert dis.dispatched == [job("a"), job("b")]
    assert dis.done_jobs == [job("a"), job("b")]
    assert dis.failed_jobs == []
    assert None not in dis.dispatched


class ThrowingDispatcher:
    """get() and/or done() raise on scripted calls, to prove that a raise from
    either one does not kill the worker. get() walks a script -- ("raise",) to
    blow up, ("job", j) to hand out a job, ("stop",) to return the None sentinel
    -- so the loop still ends deterministically on None even though no exception
    ends it. done() raises for any job whose tag is in done_failing.

    These pin the post-fix contract: only the None sentinel stops a worker; an
    exception from get() or done() is logged and swallowed, and the worker goes
    on to drain the next job. (Whether a job whose done() raised is later handed
    to fail() is left to the implementation -- not asserted here.)"""

    def __init__(self, script, done_failing=()):
        self._script = list(script)
        self._done_failing = set(done_failing)
        self.get_calls = 0
        self.dispatched = []
        self.done_jobs = []
        self.failed_jobs = []

    async def get(self):
        self.get_calls += 1
        if not self._script:
            return None
        action = self._script.pop(0)
        if action[0] == "raise":
            raise RuntimeError("get() blew up (e.g. bad job_type on the pop path)")
        if action[0] == "stop":
            return None
        return action[1]

    async def dispatch(self, job):
        self.dispatched.append(job)

    async def done(self, job):
        if job["tag"] in self._done_failing:
            raise RuntimeError(f"done() blew up releasing {job['tag']}")
        self.done_jobs.append(job)

    async def fail(self, job):
        self.failed_jobs.append(job)


async def test_worker_survives_a_raise_from_get():
    # A raise from get() -- e.g. an unknown job_type or a missing key on the pop
    # path -- must be logged and swallowed, NOT propagated out of the worker. The
    # worker tries get() again and drains the job queued behind the raise; the
    # job's arrival in dispatched is the proof the loop kept going.
    dis = ThrowingDispatcher([("raise",), ("job", job("a")), ("stop",)])

    await asyncio.wait_for(worker("w-0", dis, sleep=_instant_sleep), timeout=1.0)

    assert dis.dispatched == [job("a")]
    assert dis.done_jobs == [job("a")]


async def test_worker_survives_a_raise_from_done():
    # done() raising (releasing the lease blew up) must not break the loop: the
    # worker keeps draining and dispatches the next job. "a" never lands in
    # done_jobs because its done() raised before recording.
    dis = ThrowingDispatcher(
        [("job", job("a")), ("job", job("b")), ("stop",)],
        done_failing=["a"],
    )

    await asyncio.wait_for(worker("w-0", dis, sleep=_instant_sleep), timeout=1.0)

    assert dis.dispatched == [job("a"), job("b")]
    assert dis.done_jobs == [job("b")]


class ParkingDispatcher:
    """get() parks on an unset event forever, so the worker sits live inside its
    loop with something for a cancel to interrupt. entered_get fires once the
    worker has reached (and is now blocked in) get()."""

    def __init__(self):
        self._parked = asyncio.Event()  # never set -> get() blocks forever
        self.entered_get = asyncio.Event()

    async def get(self):
        self.entered_get.set()
        await self._parked.wait()


async def test_worker_cancellation_is_not_swallowed_by_the_guard():
    # The loop-body guard must catch Exception only -- never CancelledError,
    # which is a BaseException. Cancelling a parked worker has to unwind it:
    # Crawler.finish()/abort() wind workers down with task.cancel(), so a guard
    # broadened to except BaseException (or a bare except) would hang them.
    dis = ParkingDispatcher()
    task = asyncio.create_task(worker("w-0", dis))
    await _until(dis.entered_get.is_set)  # worker is now parked inside get()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# reap(): one recovery pass over abandoned (expired) leases
# ---------------------------------------------------------------------------


class ReapDispatcher:
    """Records what reap() re-enqueues; expired() reports the abandoned leases."""

    def __init__(self, expired_jobs=()):
        self._expired = list(expired_jobs)
        self.enqueued = []

    async def expired(self):
        return list(self._expired)

    async def enqueue(self, job):
        self.enqueued.append(job)


async def test_reap_reenqueues_every_expired_job():
    a, b, c = job("a"), job("b"), job("c")
    dis = ReapDispatcher([a, b, c])

    result = await reap(dis)

    # Every abandoned job is handed back to enqueue() (which releases its stale
    # lease and re-queues it), and reap() itself returns nothing.
    assert dis.enqueued == [a, b, c]
    assert result is None


async def test_reap_with_nothing_expired_is_a_noop():
    dis = ReapDispatcher([])

    result = await reap(dis)

    assert dis.enqueued == []
    assert result is None


# ---------------------------------------------------------------------------
# reap_worker(): the looping driver -- reap, then sleep a window, repeat
# ---------------------------------------------------------------------------


class StopLoop(Exception):
    """Raised by the injected sleep to end the otherwise-infinite driver loop."""


class CountingSleep:
    """Records each requested delay; raises StopLoop on the Nth call so the
    driver runs a known number of iterations without ever waiting."""

    def __init__(self, stop_after):
        self.durations = []
        self._stop_after = stop_after

    async def __call__(self, ms):
        self.durations.append(ms)
        if len(self.durations) >= self._stop_after:
            raise StopLoop


class FlakyReapDispatcher:
    """expired() raises on its first call, then recovers a job on later calls --
    to prove a failed pass doesn't kill the loop."""

    def __init__(self):
        self.expired_calls = 0
        self.enqueued = []

    async def expired(self):
        self.expired_calls += 1
        if self.expired_calls == 1:
            raise RuntimeError("redis hiccup during expired()")
        return [job("recovered")]

    async def enqueue(self, job):
        self.enqueued.append(job)


async def test_reap_worker_reaps_then_sleeps_each_iteration():
    # Each iteration reaps (re-enqueuing the expired job) and then sleeps one
    # window; the injected sleep stops the loop after the third pass.
    a = job("a")
    dis = ReapDispatcher([a])
    sleep = CountingSleep(stop_after=3)

    with pytest.raises(StopLoop):
        await reap_worker(dis, sleep)

    assert dis.enqueued == [a, a, a]  # reaped once per iteration
    assert sleep.durations == [MAX_INFLIGHT, MAX_INFLIGHT, MAX_INFLIGHT]


async def test_reap_worker_survives_a_failing_pass():
    # A pass that raises is logged and swallowed; the loop keeps going.
    dis = FlakyReapDispatcher()
    sleep = CountingSleep(stop_after=2)

    with pytest.raises(StopLoop):
        await reap_worker(dis, sleep)

    assert dis.expired_calls == 2  # ran a second pass after the first raised
    assert dis.enqueued == [job("recovered")]  # second pass recovered the job


async def test_reap_worker_stops_cleanly_on_cancel():
    # Parked in sleep, a cancel unwinds the driver (sleep is outside the loop's
    # try, so CancelledError propagates) rather than being swallowed.
    dis = ReapDispatcher([job("a")])
    parked = asyncio.Event()  # never set -> sleep blocks forever

    async def blocking_sleep(ms):
        await parked.wait()

    task = asyncio.create_task(reap_worker(dis, blocking_sleep))
    await _until(lambda: dis.enqueued == [job("a")])  # one pass done, now parked

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# Crawler: start()/finish()/abort() over a worker pool
# ---------------------------------------------------------------------------


class FakeDispatcher:
    def __init__(self, jobs=()):
        self._jobs = list(jobs)
        self._in_flight = 0  # handed out by get(), not yet done()/fail()ed
        self._stopped = False
        self._wake = asyncio.Event()
        if self._jobs:
            self._wake.set()
        self.dispatched = []
        self.done_jobs = []
        self.failed_jobs = []
        self.dispatch_gate = None  # if set, dispatch() blocks on it
        self.dispatch_started = asyncio.Event()

    def stop(self):  # the circuit breaker
        self._stopped = True
        self._wake.set()  # wake any worker parked in get()

    async def get(self):
        while True:
            if (
                self._stopped
            ):  # checked at the TOP: a stopped dispatcher hands out nothing
                return None
            if self._jobs:
                self._in_flight += 1
                return self._jobs.pop(0)
            self._wake.clear()
            await self._wake.wait()  # idle: park (cancellable) until a job or stop

    async def dispatch(self, job):
        self.dispatched.append(job)
        self.dispatch_started.set()
        if self.dispatch_gate is not None:
            await self.dispatch_gate.wait()

    async def done(self, job):
        self.done_jobs.append(job)
        self._in_flight -= 1

    async def fail(self, job):
        self.failed_jobs.append(job)
        self._in_flight -= 1

    async def join(self):
        while self._jobs or self._in_flight > 0:
            await asyncio.sleep(0)


async def test_start_runs_max_workers_concurrently():
    # With dispatch gated, exactly max_workers jobs can be in flight at once;
    # the rest wait because every worker is busy.
    gate = asyncio.Event()
    dis = FakeDispatcher([job(i) for i in range(5)])
    dis.dispatch_gate = gate
    crawler = Crawler(dis, max_workers=3)

    await crawler.start()
    await _until(lambda: len(dis.dispatched) == 3)
    await asyncio.sleep(0)  # give a 4th no chance to sneak in
    assert len(dis.dispatched) == 3  # capped at max_workers

    gate.set()  # release so the pool can drain
    await asyncio.wait_for(crawler.finish(), timeout=1.0)


async def test_finish_drains_the_queue_then_returns():
    dis = FakeDispatcher([job(i) for i in range(5)])
    crawler = Crawler(dis, max_workers=3)

    await crawler.start()
    await asyncio.wait_for(crawler.finish(), timeout=1.0)

    # finish() returns only once every queued job has been processed.
    assert sorted(j["tag"] for j in dis.done_jobs) == [0, 1, 2, 3, 4]
    assert dis.failed_jobs == []
    assert dis._jobs == []


async def test_abort_stops_gracefully_and_leaves_queued_jobs():
    # Gate dispatch so two jobs are mid-flight when we abort.
    gate = asyncio.Event()
    dis = FakeDispatcher([job(i) for i in range(5)])
    dis.dispatch_gate = gate
    crawler = Crawler(dis, max_workers=2)

    await crawler.start()
    await _until(lambda: len(dis.dispatched) == 2)  # both workers in flight

    aborting = asyncio.create_task(crawler.abort())
    await asyncio.sleep(0)
    assert not aborting.done()  # abort() waits for the in-flight jobs to finish

    gate.set()  # let the in-flight jobs complete
    await asyncio.wait_for(aborting, timeout=1.0)

    # The two in-flight jobs finished (not abandoned)...
    assert len(dis.done_jobs) == 2
    # ...and the remaining three were left on the queue, never handed out.
    assert len(dis._jobs) == 3


async def test_abort_before_start_is_safe():
    # No workers running -> abort() is a clean no-op that still trips the breaker.
    dis = FakeDispatcher()
    crawler = Crawler(dis, max_workers=3)

    await asyncio.wait_for(crawler.abort(), timeout=1.0)

    assert dis._stopped
