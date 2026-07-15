"""Tests for Dispatcher — a pure priority queue plus the job_type -> handler
registry used at dispatch time.

  - set_handler(job_type, handler): register (needed for dispatch(), NOT enqueue).
  - enqueue(job): put the job on the queue at a constant score (0); ordering
    is entirely the member prefix: depth -> job type -> enqueue time (FIFO).
  - get(): pop the tip + lease it, full stop. No availability check, no
    re-queue — a throttled job is handed out and the WORKER waits in the
    client's acquire(). The dispatcher is out of the time business.
  - dispatch(job): hand the job to its type's handler.handle().

Availability/throttling is the clients' concern (FixedWindowCounter.acquire);
handler.next_available is never consulted by the dispatcher.

Assumptions to flag if the shape differs:
  - dispatch on an unknown job_type raises.
  - stop()/lease/seen/failed semantics are unchanged from the previous contract.
"""

import asyncio

import pytest
from fakeredis import FakeAsyncRedis, FakeServer

from pub_crawler.dispatcher import Dispatcher, MAX_INFLIGHT, QUEUE


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
    return {"job_type": "actor", "actor_id": "https://x.example/users/a", "depth": 1}


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
# enqueue: queue the job, nothing else. Availability is NOT the dispatcher's
# business — no handler is consulted, no time is stamped into the score. All
# throttling lives in the clients' acquire(), after get() hands the job out.
# ---------------------------------------------------------------------------


async def test_enqueue_consults_no_handler():
    ah = FakeHandler(na=4242)
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", ah)

    job = actor_job()
    await dis.enqueue(job)

    # The handler is never asked about availability,
    assert ah.na_calls == []
    # and the job round-trips back out through the priority queue via get().
    assert await dis.get() == job


async def test_enqueue_works_with_no_handlers_registered():
    # enqueue no longer needs the handler registry at all — a bare dispatcher
    # (e.g. a seeding or recovery script) can queue jobs for a crawler to drain.
    dis = Dispatcher(fake_redis())

    job = actor_job()
    await dis.enqueue(job)

    assert await dis.get() == job


# ---------------------------------------------------------------------------
# Priority: get() returns jobs in depth -> job-type -> FIFO order. Availability
# never reorders anything — a job's throttle state is invisible to the queue.
# ---------------------------------------------------------------------------


async def test_get_ignores_availability_and_stays_fifo_within_a_class():
    # Jobs whose handler would report wildly different next_available values
    # (carried in `na`, which the old contract sorted by) come back in pure
    # insertion order: availability plays no role in queue position.
    h = FakeHandler()
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", h)

    for na in (300, 100, 200):
        await dis.enqueue(
            {"job_type": "actor", "actor_id": f"https://x.example/users/{na}", "na": na}
        )

    order = [(await dis.get())["na"] for _ in range(3)]
    assert order == [300, 100, 200]  # insertion order, NOT availability order


async def test_get_breaks_same_class_ties_by_insertion_order():
    h = FakeHandler(na=100)
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", h)

    await dis.enqueue(
        {
            "job_type": "actor",
            "actor_id": "https://x.example/users/first",
            "tag": "first",
        }
    )
    await dis.enqueue(
        {
            "job_type": "actor",
            "actor_id": "https://x.example/users/second",
            "tag": "second",
        }
    )

    # Equal priority -> FIFO. Also proves the job dicts are never compared:
    # a missing tiebreaker would raise TypeError here.
    assert (await dis.get())["tag"] == "first"
    assert (await dis.get())["tag"] == "second"


async def test_get_ties_stay_fifo_across_a_digit_width_boundary():
    # The FIFO tiebreaker must order numerically, not lexicographically. Enqueue
    # enough same-class jobs that the insertion counter crosses a power-of-10
    # boundary (0..10): lexicographically "10" < "2", so a string-compared
    # tiebreaker would float the 11th job ahead of the 3rd. FIFO must hold.
    h = FakeHandler(na=100)
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", h)

    for i in range(11):
        await dis.enqueue(
            {"job_type": "actor", "actor_id": f"https://x.example/users/{i}", "tag": i}
        )

    order = [(await dis.get())["tag"] for _ in range(11)]
    assert order == list(range(11))


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
    a = {"job_type": "actor", "actor_id": "https://x.example/users/a", "tag": "a"}
    b = {"job_type": "actor", "actor_id": "https://x.example/users/b", "tag": "b"}
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

    old = {"job_type": "actor", "actor_id": "https://x.example/users/old", "tag": "old"}
    await dis.enqueue(old)
    old_job = await dis.get()  # deadline = MAX_INFLIGHT

    # Lease a second job much later, so its deadline is further out.
    clock.t = MAX_INFLIGHT - 100
    fresh = {
        "job_type": "actor",
        "actor_id": "https://x.example/users/fresh",
        "tag": "fresh",
    }
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


# ---------------------------------------------------------------------------
# fail()/failed(): record a job that could not be processed onto a simple list.
#   fail(job) appends; failed() is an ASYNC ITERATOR over the recorded jobs.
#   The store may be unordered (or ordered by insertion) — order isn't promised;
#   membership + count are. Jobs round-trip back out as equal dicts.
# ---------------------------------------------------------------------------


async def collect_failed(dis):
    return [job async for job in dis.failed()]


async def test_failed_is_empty_before_anything_fails():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())

    assert await collect_failed(dis) == []


async def test_fail_records_the_job():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    job = actor_job()

    await dis.fail(job)

    # The job round-trips back out of failed() as an equal dict.
    assert await collect_failed(dis) == [job]


async def test_failed_lists_every_failed_job():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    a = {"job_type": "actor", "actor_id": "https://x.example/users/a", "tag": "a"}
    b = {"job_type": "actor", "actor_id": "https://x.example/users/b", "tag": "b"}

    await dis.fail(a)
    await dis.fail(b)

    failed = await collect_failed(dis)
    assert len(failed) == 2
    assert a in failed
    assert b in failed


async def test_failed_can_be_iterated_more_than_once():
    # failed() is an inspection surface a reporter walks; reading it must not
    # consume the record, so a second pass sees the same jobs.
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    await dis.fail(actor_job())

    first = await collect_failed(dis)
    second = await collect_failed(dis)

    assert first == second
    assert len(second) == 1


async def test_failing_a_job_does_not_put_it_in_flight():
    # Recording a failure is terminal bookkeeping: the job lands on the failed
    # list, not in the in-flight set.
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())

    await dis.fail(actor_job())

    assert await dis.inflight() == []
    assert await collect_failed(dis) == [actor_job()]


async def test_failing_an_in_flight_job_releases_its_lease():
    # The real path: get() leases the job, then it fails. fail() is terminal, so
    # like done() it takes the job OUT of flight -- otherwise the lease lingers,
    # the job still counts toward join(), and once it expires the reaper would
    # re-enqueue a job we've already recorded as failed.
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    await dis.enqueue(actor_job())
    job = await dis.get()  # leased -> in flight
    assert await dis.inflight() == [job]

    await dis.fail(job)

    # Released from flight (so a reaper won't resurrect it)...
    assert await dis.inflight() == []
    # ...and recorded on the failed list.
    assert await collect_failed(dis) == [job]


# ---------------------------------------------------------------------------
# unfail(): remove one job from the failed record, the bookkeeping half of
# recovery (bin/recover_failed.py) -- a recovered job is re-enqueued AND taken
# off the failed list, so a second recovery run doesn't double-enqueue it.
# Matches by job value (same round-trip encoding fail() uses), not identity.
# Removing is all it does: seen is untouched, and unfailing a job that isn't
# on the list is a quiet no-op.
# ---------------------------------------------------------------------------


async def test_unfail_removes_the_job_from_failed():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    job = actor_job()
    await dis.fail(job)

    await dis.unfail(job)

    assert await collect_failed(dis) == []


async def test_unfail_leaves_other_failed_jobs_alone():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    a = {"job_type": "actor", "actor_id": "https://x.example/users/a", "depth": 1}
    b = {"job_type": "actor", "actor_id": "https://x.example/users/b", "depth": 1}
    await dis.fail(a)
    await dis.fail(b)

    await dis.unfail(a)

    assert await collect_failed(dis) == [b]


async def test_unfail_of_a_job_that_never_failed_is_a_noop():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    a = actor_job()
    await dis.fail(a)
    b = {"job_type": "actor", "actor_id": "https://x.example/users/other", "depth": 1}

    await dis.unfail(b)  # never failed; must not raise

    assert await collect_failed(dis) == [a]


async def test_unfail_matches_by_job_value_not_identity():
    # Recovery re-reads jobs out of failed() as fresh dicts; unfail must match
    # the stored member from an equal dict, not the original object.
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    await dis.fail(actor_job())

    await dis.unfail(actor_job())

    assert await collect_failed(dis) == []


async def test_unfail_does_not_unmark_seen():
    # Recovery unfails and re-enqueues in some order; whichever comes first,
    # the job's seen-mark (set by its original enqueue) must survive unfail.
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    job = actor_job()
    await dis.enqueue(job)
    leased = await dis.get()
    await dis.fail(leased)

    await dis.unfail(leased)

    assert await dis.seen(job)


# ---------------------------------------------------------------------------
# stop(): the circuit breaker. stop() sets a flag checked at the TOP of get();
# a stopped dispatcher hands out no more work -- get() returns None instead, and
# WITHOUT popping anything (queued jobs stay put for the next run). The breaker
# gates handing work OUT only: enqueue() still works (e.g. re-enqueuing in-flight
# jobs during shutdown). No reset -- build a fresh dispatcher to resume.
#
# Assumed shape (flag if it differs): stop() is SYNCHRONOUS -- it just sets a
# flag, so it is called without await. Every get() here is wrapped in wait_for
# so a breaker that blocks (e.g. checked after the pop instead of before) fails
# the test instead of hanging it.
# ---------------------------------------------------------------------------


async def test_get_returns_none_when_stopped():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    await dis.enqueue(actor_job())

    dis.stop()

    assert await asyncio.wait_for(dis.get(), timeout=1.0) is None


async def test_get_returns_none_when_stopped_even_on_an_empty_queue():
    # The flag is checked BEFORE the blocking pop, so a stopped dispatcher with
    # an empty queue returns None at once rather than blocking in bzpopmin.
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())

    dis.stop()

    assert await asyncio.wait_for(dis.get(), timeout=1.0) is None


async def test_stop_leaves_queued_jobs_in_place():
    # stop() refuses to hand the job out; it must not pop it. A fresh, un-stopped
    # dispatcher on the same Redis still finds it waiting.
    r = fake_redis()
    stopped = Dispatcher(r)
    stopped.set_handler("actor", FakeHandler())
    await stopped.enqueue(actor_job())
    stopped.stop()
    assert await asyncio.wait_for(stopped.get(), timeout=1.0) is None

    fresh = Dispatcher(r)
    fresh.set_handler("actor", FakeHandler())
    assert await asyncio.wait_for(fresh.get(), timeout=1.0) == actor_job()


async def test_enqueue_still_works_after_stop():
    # The breaker gates get() only -- enqueue onto a stopped dispatcher succeeds.
    r = fake_redis()
    dis = Dispatcher(r)
    dis.set_handler("actor", FakeHandler())

    dis.stop()
    await dis.enqueue(actor_job())

    fresh = Dispatcher(r)
    fresh.set_handler("actor", FakeHandler())
    assert await asyncio.wait_for(fresh.get(), timeout=1.0) == actor_job()


async def test_stop_is_idempotent():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    await dis.enqueue(actor_job())

    dis.stop()
    dis.stop()

    assert await asyncio.wait_for(dis.get(), timeout=1.0) is None


# ---------------------------------------------------------------------------
# seen()/enqueue de-dup: enqueue() records job_id(job) in a Redis set; seen(job)
# tests membership, so handlers can skip re-queuing duplicates. NOTHING un-sees
# -- queued, in flight, failed, or done, the id stays in the set. Identity is by
# job_id, so the SAME resource reached again (e.g. a deeper crawl) reads as
# already seen. (These use job_id-shaped actor jobs -- a real actor_id field --
# since enqueue/seen run job_id() on them, unlike the opaque actor_job() above
# that only the queue mechanics need.)
# ---------------------------------------------------------------------------


def actor_seed(actor_id="https://x.example/users/a", depth=1):
    return {"job_type": "actor", "actor_id": actor_id, "depth": depth}


async def test_a_job_is_not_seen_before_it_is_enqueued():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())

    assert not await dis.seen(actor_seed())


async def test_enqueue_marks_the_job_seen():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    job = actor_seed()

    await dis.enqueue(job)

    assert await dis.seen(job)


async def test_seen_is_keyed_by_job_id_not_dict_identity():
    # The same actor reached at a different depth is the SAME job: enqueuing it
    # once marks every depth-variant seen.
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())

    await dis.enqueue(actor_seed(depth=1))

    assert await dis.seen(actor_seed(depth=2))


async def test_seen_distinguishes_different_jobs():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())

    await dis.enqueue(actor_seed(actor_id="https://x.example/users/a"))

    assert not await dis.seen(actor_seed(actor_id="https://x.example/users/b"))


async def test_a_job_stays_seen_after_it_is_done():
    # Nothing un-sees: completing a job leaves its id in the set, so it is never
    # re-queued.
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    job = actor_seed()
    await dis.enqueue(job)
    got = await dis.get()
    await dis.done(got)

    assert await dis.seen(job)


async def test_a_job_stays_seen_after_it_fails():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    job = actor_seed()
    await dis.enqueue(job)
    got = await dis.get()
    await dis.fail(got)

    assert await dis.seen(job)


async def test_re_enqueuing_a_seen_job_keeps_it_seen():
    # reap()/recovery just enqueue again; that must be safe and idempotent on the
    # seen set.
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())
    job = actor_seed()
    await dis.enqueue(job)

    await dis.enqueue(job)

    assert await dis.seen(job)


async def test_seen_raises_on_an_unidentifiable_job():
    # A job whose job_id is None (here: actor with no actor_id) can't be tracked;
    # seen() refuses it loudly rather than querying a junk member.
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())

    with pytest.raises(Exception):
        await dis.seen({"job_type": "actor", "depth": 1})


async def test_enqueue_raises_on_an_unidentifiable_job():
    # Same guard on the write side: an unidentifiable job is rejected before it
    # can reach the queue or the seen set.
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", FakeHandler())

    with pytest.raises(Exception):
        await dis.enqueue({"job_type": "actor", "depth": 1})


# ---------------------------------------------------------------------------
# Queue ordering with depth + job type: get() must hand back jobs ordered by
#   depth (shallowest) -> job type (webfinger < actor < collection < page)
#   -> FIFO (insertion order).
# Availability is NOT a sort key at any rank. Black-box: these assert get()
# ORDER only, independent of how the keys are encoded (member prefix vs score).
#
# Assumptions to flag if the contract differs:
#   - depth outranks job type. The mixed case (different depth AND type at
#     once) is intentionally NOT pinned here pending the depth-vs-type
#     precedence call -- say the word and I'll add it.
#   - webfinger jobs carry no `depth`; assumed to sort as depth 0 (so type
#     precedence lands them first among the shallowest).
#   - a missing `depth` defaults gracefully (uniform no-depth jobs still order
#     FIFO -- which is why the basic queue tests keep passing).
# ---------------------------------------------------------------------------


_ID_FIELD = {
    "actor": "actor_id",
    "collection": "collection_id",
    "page": "page_id",
    "webfinger": "webfinger",
}


def oj(job_type, tag, *, depth=None):
    """A uniquely-identified ordering job with an optional depth and a `tag`
    to assert ordering by."""
    job = {
        "job_type": job_type,
        _ID_FIELD[job_type]: f"https://x.example/{job_type}/{tag}",
        "tag": tag,
    }
    if depth is not None:
        job["depth"] = depth
    return job


def ordering_dispatcher():
    dis = Dispatcher(fake_redis())
    for t in ("webfinger", "actor", "collection", "page"):
        dis.set_handler(t, FakeHandler())
    return dis


async def drain_tags(dis, n):
    return [(await dis.get())["tag"] for _ in range(n)]


async def test_depth_dominates_insertion_order():
    # The inversion of the old contract: a shallow job enqueued LATER beats a
    # deep job enqueued earlier. Depth is the top-ranked key, full stop.
    dis = ordering_dispatcher()
    await dis.enqueue(oj("actor", "early-d3", depth=3))
    await dis.enqueue(oj("actor", "late-d0", depth=0))

    assert await drain_tags(dis, 2) == ["late-d0", "early-d3"]


async def test_shallower_depth_first():
    dis = ordering_dispatcher()
    await dis.enqueue(oj("actor", "d2", depth=2))
    await dis.enqueue(oj("actor", "d0", depth=0))

    assert await drain_tags(dis, 2) == ["d0", "d2"]


async def test_same_depth_orders_by_job_type():
    # webfinger < actor < collection < page (here: actor/collection/page at one depth)
    dis = ordering_dispatcher()
    await dis.enqueue(oj("page", "pg", depth=1))
    await dis.enqueue(oj("collection", "co", depth=1))
    await dis.enqueue(oj("actor", "ac", depth=1))

    assert await drain_tags(dis, 3) == ["ac", "co", "pg"]


async def test_webfinger_sorts_ahead_of_a_same_depth_actor():
    # webfinger has the highest type precedence (and no depth -> assumed depth 0).
    dis = ordering_dispatcher()
    await dis.enqueue(oj("actor", "ac", depth=0))
    await dis.enqueue(oj("webfinger", "wf"))

    assert await drain_tags(dis, 2) == ["wf", "ac"]


async def test_full_tie_falls_back_to_fifo():
    # same depth and type -> insertion order preserved.
    dis = ordering_dispatcher()
    await dis.enqueue(oj("actor", "first", depth=1))
    await dis.enqueue(oj("actor", "second", depth=1))

    assert await drain_tags(dis, 2) == ["first", "second"]


async def test_depth_orders_numerically_not_lexicographically():
    # the padding trap: depth 10 must NOT sort before depth 2.
    dis = ordering_dispatcher()
    await dis.enqueue(oj("actor", "d10", depth=10))
    await dis.enqueue(oj("actor", "d2", depth=2))

    assert await drain_tags(dis, 2) == ["d2", "d10"]


# ---------------------------------------------------------------------------
# Throttle-free dispatch: the dispatcher never asks anyone about availability.
# get() is pop-the-tip + lease, full stop -- a throttled job is handed out and
# the worker waits in the client's acquire(), not in the queue. These pin the
# contract by poisoning next_available: any call to it fails the test.
# ---------------------------------------------------------------------------


class PoisonedHandler:
    """A handler whose next_available must never be called. handle() works,
    so dispatch-path tests can still use it."""

    def __init__(self):
        self.handled = []

    def next_available(self, job):
        raise AssertionError("next_available must not be consulted")

    async def handle(self, job):
        self.handled.append(job)


async def test_get_never_consults_next_available():
    dis = Dispatcher(fake_redis())
    dis.set_handler("actor", PoisonedHandler())

    await dis.enqueue(oj("actor", "throttled", depth=1))

    # The tip comes straight out, throttle state unseen and unasked.
    got = await asyncio.wait_for(dis.get(), timeout=1.0)
    assert got["tag"] == "throttled"


async def test_enqueued_jobs_all_score_zero():
    # White-box on purpose: the constant-zero score is what makes the member
    # prefix (depth|type|ts) the total queue order, and what the one-shot
    # re-score fixup for the live queue relies on. Pin it.
    dis = ordering_dispatcher()
    await dis.enqueue(oj("actor", "a", depth=1))
    await dis.enqueue(oj("page", "p", depth=3))

    members = await dis.redis.zrange(QUEUE, 0, -1, withscores=True)
    assert len(members) == 2
    assert all(score == 0 for _, score in members)
