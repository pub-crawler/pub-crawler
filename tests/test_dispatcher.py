"""Tests for Dispatcher — the job_type -> handler registry used both directions.

  - set_handler(job_type, handler): register.
  - enqueue(job): ask the handler that HANDLES this job type for its
    next_available, stamp it onto the job, and put the job on the queue.
  - dispatch(job): hand the job to that handler's handle().

Built before the handlers (which take the dispatcher and register via
set_handler), so the construction cycle dissolves.

Assumptions to flag if the shape differs:
  - Dispatcher(queue) takes the queue; the stamped job dicts land on it
    (FIFO for now; priority ordering is a later refinement).
  - enqueue stamps under job["next_available"].
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
        return self.na

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


async def test_enqueue_stamps_next_available_and_queues_the_job():
    ah = FakeHandler(na=4242)
    queue = asyncio.Queue()
    dis = Dispatcher(queue)
    dis.set_handler("actor", ah)

    job = actor_job()
    await dis.enqueue(job)

    # Asked the handler that handles this type when it can next be handled...
    assert ah.na_calls == [job]
    # ...stamped that onto the job, and queued it.
    queued = queue.get_nowait()
    assert queued["next_available"] == 4242
    assert queued["job_type"] == "actor"


async def test_enqueue_uses_the_handler_for_the_jobs_own_type():
    ah = FakeHandler(na=100)
    ch = FakeHandler(na=500)
    queue = asyncio.Queue()
    dis = Dispatcher(queue)
    dis.set_handler("actor", ah)
    dis.set_handler("collection", ch)

    await dis.enqueue({"job_type": "collection", "url": "https://x.example/c"})

    # Only the collection handler is consulted, and its answer is what's stamped.
    assert ch.na_calls and not ah.na_calls
    assert queue.get_nowait()["next_available"] == 500
