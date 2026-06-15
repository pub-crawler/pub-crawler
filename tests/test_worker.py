"""Unit tests for crawler.worker — the per-task drain loop.

The loop's contract, independent of the real dispatcher/handlers:
  get() a job, dispatch() it, and
    - on success  -> done(job)            (release the lease, NOT a failure)
    - on exception -> fail(job), continue  (record it, SKIP done, keep looping)
A failed job must never be marked done, a succeeded job must never be failed,
and one job's failure must not stop the worker from draining the next.

worker() loops forever on get(); a fake dispatcher scripts a finite run and then
raises StopWorker from get() (which is outside the try, so it breaks the loop)
to end the test deterministically.
"""

import asyncio

import pytest

from pub_crawler.crawler import worker


class StopWorker(Exception):
    """Sentinel raised by the fake get() to end the otherwise-infinite loop."""


class FakeDispatcher:
    def __init__(self, jobs, failing=()):
        self._jobs = list(jobs)
        self._failing = set(failing)  # tags whose dispatch() raises
        self.dispatched = []
        self.done_jobs = []
        self.failed_jobs = []

    async def get(self):
        if not self._jobs:
            raise StopWorker
        return self._jobs.pop(0)

    async def dispatch(self, job):
        self.dispatched.append(job)
        if job["tag"] in self._failing:
            raise RuntimeError(f"dispatch blew up on {job['tag']}")

    async def done(self, job):
        self.done_jobs.append(job)

    async def fail(self, job):
        self.failed_jobs.append(job)


def job(tag):
    return {"job_type": "actor", "tag": tag}


async def test_worker_marks_a_succeeded_job_done_and_never_fails_it():
    dis = FakeDispatcher([job("ok")])

    with pytest.raises(StopWorker):
        await worker("w-0", dis)

    assert dis.dispatched == [job("ok")]
    assert dis.done_jobs == [job("ok")]
    assert dis.failed_jobs == []


async def test_worker_fails_a_raised_job_and_never_marks_it_done():
    dis = FakeDispatcher([job("bad")], failing=["bad"])

    with pytest.raises(StopWorker):
        await worker("w-0", dis)

    assert dis.dispatched == [job("bad")]
    assert dis.failed_jobs == [job("bad")]
    assert dis.done_jobs == []  # the whole point: no done() on the failure path


async def test_worker_keeps_draining_after_a_failure():
    # A failure must not break the loop: the bad job is failed (not done), and
    # the worker goes on to dispatch and complete the next job.
    dis = FakeDispatcher([job("bad"), job("good")], failing=["bad"])

    with pytest.raises(StopWorker):
        await worker("w-0", dis)

    assert dis.dispatched == [job("bad"), job("good")]
    assert dis.failed_jobs == [job("bad")]
    assert dis.done_jobs == [job("good")]


class NoneAfterJobs:
    """get() hands out the scripted jobs, then returns None forever -- the stop
    sentinel a stopped dispatcher yields."""

    def __init__(self, jobs=()):
        self._jobs = list(jobs)
        self.dispatched = []
        self.done_jobs = []
        self.failed_jobs = []

    async def get(self):
        return self._jobs.pop(0) if self._jobs else None

    async def dispatch(self, job):
        self.dispatched.append(job)

    async def done(self, job):
        self.done_jobs.append(job)

    async def fail(self, job):
        self.failed_jobs.append(job)


async def test_worker_exits_when_get_returns_none():
    # None from get() is the stop sentinel: the worker drains real jobs, then
    # returns the moment get() yields None -- it must NOT dispatch the sentinel.
    # (A worker that ignored None would loop on it forever; wait_for turns that
    # hang into a failure.)
    dis = NoneAfterJobs([job("a"), job("b")])

    await asyncio.wait_for(worker("w-0", dis), timeout=1.0)

    assert dis.dispatched == [job("a"), job("b")]
    assert dis.done_jobs == [job("a"), job("b")]
    assert dis.failed_jobs == []
    assert None not in dis.dispatched
