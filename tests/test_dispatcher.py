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

from pub_crawler.dispatcher import Dispatcher


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


def actor_job():
    return {"job_type": "actor", "url": "https://x.example/users/a", "depth": 1}


# ---------------------------------------------------------------------------
# dispatch: route to the handler for the job_type
# ---------------------------------------------------------------------------


async def test_dispatch_routes_to_the_handler_for_the_job_type():
    ah, wfh = FakeHandler(), FakeHandler()
    dis = Dispatcher(asyncio.Queue())
    dis.set_handler("actor", ah)
    dis.set_handler("webfinger", wfh)

    job = actor_job()
    await dis.dispatch(job)

    assert ah.handled == [job]
    assert wfh.handled == []


async def test_dispatch_unknown_job_type_raises():
    dis = Dispatcher(asyncio.Queue())
    with pytest.raises(Exception):
        await dis.dispatch({"job_type": "mystery"})


# ---------------------------------------------------------------------------
# enqueue: stamp next_available (from the HANDLING handler) + queue
# ---------------------------------------------------------------------------


async def test_enqueue_consults_the_handler_and_queues_the_job():
    ah = FakeHandler(na=4242)
    dis = Dispatcher(asyncio.PriorityQueue())
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
    dis = Dispatcher(asyncio.PriorityQueue())
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
    dis = Dispatcher(asyncio.PriorityQueue())
    dis.set_handler("actor", h)

    for na in (300, 100, 200):
        await dis.enqueue({"job_type": "actor", "na": na})

    order = [(await dis.get())["na"] for _ in range(3)]
    assert order == [100, 200, 300]  # soonest first, regardless of insertion order


async def test_get_breaks_next_available_ties_by_insertion_order():
    h = FakeHandler(na=100)  # every job gets the same next_available
    dis = Dispatcher(asyncio.PriorityQueue())
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
    dis = Dispatcher(asyncio.PriorityQueue())
    dis.set_handler("actor", h)

    await dis.enqueue(actor_job())
    await dis.enqueue(actor_job())

    # A worker drains the queue: get -> dispatch -> done (per-job task_done).
    async def drain():
        for _ in range(2):
            job = await dis.get()
            await dis.dispatch(job)
            dis.done(job)

    worker = asyncio.create_task(drain())

    # join() must block until both jobs are done, then return (timeout guards a hang).
    await asyncio.wait_for(dis.join(), timeout=1.0)
    await worker

    assert len(h.handled) == 2
