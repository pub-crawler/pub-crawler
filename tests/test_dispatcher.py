"""Tests for Dispatcher — the job_type -> handler registry used both directions.

  - set_handler(job_type, handler): register.
  - enqueue(job): ask the handler that HANDLES this job type for its
    next_available, and put (next_available, count, job) on the priority queue.
  - get(): pop the soonest job, unwrap, hand it back (re-checking readiness).
  - dispatch(job): hand the job to that handler's handle().

Built before the handlers (which take the dispatcher and register via
set_handler), so the construction cycle dissolves.

Assumptions to flag if the shape differs:
  - Dispatcher(queue) takes a PriorityQueue; jobs ride as
    (next_available, count, job) tuples; get() unwraps back to the job.
  - next_available is the priority key only — NOT stamped onto the job.
  - dispatch on an unknown job_type raises.
"""

import asyncio

import pytest
from fakeredis import FakeAsyncRedis, FakeServer

from pub_crawler.dispatcher import Dispatcher, MAX_INFLIGHT


def fake_redis():
    # Fresh, isolated in-memory async Redis (its own server) per call.
    return FakeAsyncRedis(server=FakeServer())


class FakeHandler:
    def __init__(self, na=0):
        self.na = na
        self.na_calls = []
        self.handled = []

    def next_available(self, job):
        self.na_calls.append(job)
        return job.get("na", self.na)  # job can carry its own na for ordering tests

    async def handle(self, job):
        self.handled.append(job)


class Clock:
    """A controllable clock for lease/expiry tests; advance by setting .t."""

    def __init__(self, t=0):
        self.t = t

    def __call__(self):
        return self.t


def actor_job():
    return {"job_type": "actor", "url": "https://x.example/users/a", "depth": 1}


# ---------------------------------------------------------------------------
# dispatch: route to the handler for the job_type
# ---------------------------------------------------------------------------


async def test_dispatch_routes_to_the_handler_for_the_job_type():
    ah, wfh = FakeHandler(), FakeHandler()
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", ah)
    dis.set_handler("webfinger", wfh)

    job = actor_job()
    await dis.dispatch(job)

    assert ah.handled == [job]
    assert wfh.handled == []


async def test_dispatch_unknown_job_type_raises():
    dis = Dispatcher(fake_redis())
    with pytest.raises(Exception):
        await dis.dispatch({"job_type": "mystery"})


# ---------------------------------------------------------------------------
# enqueue: stamp next_available (from the HANDLING handler) + queue
# ---------------------------------------------------------------------------


async def test_enqueue_consults_the_handler_and_queues_the_job():
    ah = FakeHandler(na=4242)
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", ah)

    job = actor_job()
    await dis.enqueue(job)

    # The handler that handles this type is asked when it can next be handled,
    assert ah.na_calls == [job]
    # and the job round-trips back out through the priority queue via get().
    assert await dis.get() == job


async def test_enqueue_uses_the_handler_for_the_jobs_own_type():
    ah = FakeHandler(na=100)
    ch = FakeHandler(na=500)
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", ah)
    dis.set_handler("collection", ch)

    job = {"job_type": "collection", "url": "https://x.example/c"}
    await dis.enqueue(job)

    # Only the handler for THIS job's type is consulted, and the job round-trips.
    assert ch.na_calls == [job]
    assert ah.na_calls == []
    assert await dis.get() == job


# ---------------------------------------------------------------------------
# Priority: get() returns jobs in next_available order, FIFO on ties
# ---------------------------------------------------------------------------


async def test_get_returns_jobs_in_next_available_order():
    h = FakeHandler()
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", h)

    for na in (300, 100, 200):
        await dis.enqueue({"job_type": "actor", "na": na})

    order = [(await dis.get())["na"] for _ in range(3)]
    assert order == [100, 200, 300]  # soonest first, regardless of insertion order


async def test_get_breaks_next_available_ties_by_insertion_order():
    h = FakeHandler(na=100)  # every job gets the same next_available
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", h)

    await dis.enqueue({"job_type": "actor", "tag": "first"})
    await dis.enqueue({"job_type": "actor", "tag": "second"})

    # Equal priority -> FIFO. Also proves the job dicts are never compared:
    # a missing tiebreaker would raise TypeError here.
    assert (await dis.get())["tag"] == "first"
    assert (await dis.get())["tag"] == "second"


# ---------------------------------------------------------------------------
# join(): await until the queue is fully drained (termination)
# ---------------------------------------------------------------------------


async def test_join_returns_once_the_queue_is_drained():
    h = FakeHandler()
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", h)

    await dis.enqueue(actor_job())
    await dis.enqueue(actor_job())

    # A worker drains the queue: get -> dispatch -> done (per-job task_done).
    async def drain():
        for _ in range(2):
            job = await dis.get()
            await dis.dispatch(job)
            await dis.done(job)

    worker = asyncio.create_task(drain())

    # join() must block until both jobs are done, then return (timeout guards a hang).
    await asyncio.wait_for(dis.join(), timeout=1.0)
    await worker

    assert len(h.handled) == 2


# ---------------------------------------------------------------------------
# In-flight tracking: get() leases a job, done()/re-enqueue release it.
#   inflight() -> the jobs taken by get() but not yet released (async; reads
#   Redis so it survives a crash). Order isn't promised; membership + count are.
# ---------------------------------------------------------------------------


async def test_inflight_is_empty_before_anything_is_taken():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())

    assert await dis.inflight() == []


async def test_get_puts_the_job_in_flight():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    await dis.enqueue(actor_job())

    job = await dis.get()

    assert await dis.inflight() == [job]


async def test_done_takes_the_job_out_of_flight():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    await dis.enqueue(actor_job())
    job = await dis.get()
    assert await dis.inflight() == [job]

    await dis.done(job)

    assert await dis.inflight() == []


async def test_inflight_lists_every_job_currently_in_flight():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    a = {"job_type": "actor", "tag": "a"}
    b = {"job_type": "actor", "tag": "b"}
    await dis.enqueue(a)
    await dis.enqueue(b)

    ja = await dis.get()
    jb = await dis.get()

    flight = await dis.inflight()
    assert len(flight) == 2
    assert ja in flight
    assert jb in flight


async def test_re_enqueuing_an_in_flight_job_releases_it():
    # A handler that defers its own job (e.g. retry-after) re-enqueues it: that
    # takes it back out of flight and returns it to the queue.
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    await dis.enqueue(actor_job())
    job = await dis.get()
    assert await dis.inflight() == [job]

    await dis.enqueue(job)

    # No longer in flight...
    assert await dis.inflight() == []
    # ...but back on the queue, ready to be taken again.
    assert await dis.get() == job


async def test_join_blocks_until_the_inflight_list_empties():
    # The queue can be empty while a job is still being worked: join() must wait
    # for the in-flight job to finish, not merely for the queue to drain.
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    await dis.enqueue(actor_job())
    job = await dis.get()  # queue now empty, but the job is in flight
    assert await dis.inflight() == [job]

    async def finish():
        await asyncio.sleep(0.05)
        await dis.done(job)

    worker = asyncio.create_task(finish())
    # Returns only after done() empties the in-flight list (timeout guards a hang).
    await asyncio.wait_for(dis.join(), timeout=1.0)
    await worker

    assert await dis.inflight() == []


# ---------------------------------------------------------------------------
# expired(): in-flight jobs whose lease deadline (set at get(), MAX_INFLIGHT
# out) is now in the past — the read-only surface a reaper walks. Read-only:
# it observes, it does NOT release; re-enqueue is what recovers the job.
# ---------------------------------------------------------------------------


async def test_expired_lists_jobs_past_their_deadline():
    clock = Clock(0)
    dis = Dispatcher(fake_redis(), now=clock)
    dis.set_handler("actor", FakeHandler())
    await dis.enqueue(actor_job())
    job = await dis.get()  # leased at t=0, deadline = MAX_INFLIGHT

    # Still within the lease -> not expired.
    clock.t = MAX_INFLIGHT - 1
    assert await dis.expired() == []

    # Past the lease -> expired.
    clock.t = MAX_INFLIGHT + 1
    assert await dis.expired() == [job]


async def test_expired_excludes_jobs_still_within_their_lease():
    clock = Clock(0)
    dis = Dispatcher(fake_redis(), now=clock)
    dis.set_handler("actor", FakeHandler())

    old = {"job_type": "actor", "tag": "old"}
    await dis.enqueue(old)
    old_job = await dis.get()  # deadline = MAX_INFLIGHT

    # Lease a second job much later, so its deadline is further out.
    clock.t = MAX_INFLIGHT - 100
    fresh = {"job_type": "actor", "tag": "fresh"}
    await dis.enqueue(fresh)
    fresh_job = await dis.get()  # deadline = 2*MAX_INFLIGHT - 100

    # Step just past the OLD deadline but well short of the fresh one.
    clock.t = MAX_INFLIGHT + 1
    expired = await dis.expired()

    assert old_job in expired
    assert fresh_job not in expired  # per-job deadline, not all-or-nothing


async def test_expired_excludes_a_completed_job():
    clock = Clock(0)
    dis = Dispatcher(fake_redis(), now=clock)
    dis.set_handler("actor", FakeHandler())
    await dis.enqueue(actor_job())
    job = await dis.get()
    await dis.done(job)  # finished -> out of flight before its lease lapses

    clock.t = MAX_INFLIGHT + 1  # well past when the lease would have expired
    assert await dis.expired() == []


async def test_re_enqueuing_an_expired_job_recovers_it():
    # The reaper pattern end to end: expired() finds the abandoned job, and
    # enqueue() releases it from flight and puts it back on the queue.
    clock = Clock(0)
    dis = Dispatcher(fake_redis(), now=clock)
    dis.set_handler("actor", FakeHandler())
    await dis.enqueue(actor_job())
    job = await dis.get()

    clock.t = MAX_INFLIGHT + 1
    [expired_job] = await dis.expired()
    await dis.enqueue(expired_job)

    # Released from flight (so no longer expired)...
    assert await dis.inflight() == []
    assert await dis.expired() == []
    # ...and waiting on the queue again.
    assert await dis.get() == job
