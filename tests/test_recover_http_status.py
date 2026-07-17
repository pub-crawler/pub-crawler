"""Tests for recover_http_status — re-enqueue work lost to recoverable HTTP
status deaths, which leave a status property on the node but NO failed-set
entry (the handlers record 4xx/5xx and end the chain silently).

bin/recover_http_status.py exports
  recover_http_status(dispatcher, G, seed_actor_ids, recoverable_status_codes)
with recoverable_status_codes defaulting to the "tidy" transient set
{408, 429, 500, 502, 503, 504, 520, 524} (Cloudflare's abandonment-signal
codes 521/522/523/525/526/530 are opt-in via the parameter).

Per node in G.all_nodes(), three independent stages:
  - http_status recoverable (actor never fetched):
      label in seed_actor_ids       -> actor job at depth 0
      elif node has own depth prop  -> actor job at that depth (post-fix
                                       failures stamp depth; it's exact)
      elif first_neighbor has depth -> actor job at neighbor depth + 1
      else                          -> skip (logged, not counted)
  - {direction}_http_status recoverable -> collection job from the node's
      collection-URL prop + depth prop
  - {direction}_last_page_http_status recoverable -> page job from the
      node's {direction}_last_page prop + depth prop
Guards: any missing expected prop skips that branch, never raises.

Enqueues are UNCONDITIONAL — these jobs are all long-seen; recovery must not
gate on the seen set. Returns the number of jobs enqueued (skips excluded).

Uses the real Dispatcher over fakeredis (enqueue is inherently unconditional
there, and the queue can be read back) and FakeGraph from support.
"""

import json

import pytest
from fakeredis import FakeAsyncRedis, FakeServer

from recover_http_status import recover_http_status
from pub_crawler.dispatcher import Dispatcher, QUEUE
from support import FakeGraph

SEED = "https://seed.example/users/root"
ACTOR = "https://a.example/users/alice"
NEIGHBOR = "https://b.example/users/bob"
OTHER = "https://c.example/users/carol"


def fake_redis():
    return FakeAsyncRedis(server=FakeServer())


def dispatcher():
    return Dispatcher(fake_redis())


async def queued_jobs(d):
    """Jobs on the queue ZSET, parsed from `depth|type|ts|job` members."""
    members = await d.redis.zrange(QUEUE, 0, -1)
    return [json.loads(m.decode().split("|", 3)[3]) for m in members]


async def failed_actor(g, label, status=429, **props):
    """A node whose actor fetch died on an HTTP status: no depth unless given."""
    await g.ensure_node(label)
    await g.set_node_properties(label, {"http_status": status, **props})


async def fetched_actor(g, label, depth=1, **props):
    """A successfully fetched node: depth + whatever stage props the test needs."""
    await g.ensure_node(label)
    await g.set_node_properties(label, {"http_status": 200, "depth": depth, **props})


# ---------------------------------------------------------------------------
# actor stage: depth resolution order — seed 0, own depth, neighbor + 1
# ---------------------------------------------------------------------------


async def test_recovers_a_failed_seed_actor_at_depth_zero():
    d = dispatcher()
    g = FakeGraph()
    await failed_actor(g, SEED, 429)

    count = await recover_http_status(d, g, {SEED})

    assert count == 1
    assert await queued_jobs(d) == [{"job_type": "actor", "actor_id": SEED, "depth": 0}]


async def test_recovers_a_failed_actor_at_its_own_stamped_depth():
    # Post-fix failures stamp depth on the node; that exact value wins over
    # first_neighbor reconstruction even when a neighbor is available.
    d = dispatcher()
    g = FakeGraph()
    await fetched_actor(g, NEIGHBOR, depth=0)
    await failed_actor(g, ACTOR, 502, depth=3)
    await g.ensure_edge(NEIGHBOR, ACTOR)

    count = await recover_http_status(d, g, set())

    assert count == 1
    assert await queued_jobs(d) == [
        {"job_type": "actor", "actor_id": ACTOR, "depth": 3}
    ]


async def test_recovers_a_failed_actor_at_first_neighbor_depth_plus_one():
    # Historical failures have no depth prop: reconstruct from the FIRST
    # neighbor (the discoverer), not any later or shallower one.
    d = dispatcher()
    g = FakeGraph()
    await fetched_actor(g, NEIGHBOR, depth=2)
    await fetched_actor(g, OTHER, depth=0)
    await failed_actor(g, ACTOR, 429)
    await g.ensure_edge(NEIGHBOR, ACTOR)  # earliest edge: the discoverer
    await g.ensure_edge(OTHER, ACTOR)  # later, shallower sighting — ignored

    count = await recover_http_status(d, g, set())

    assert count == 1
    assert await queued_jobs(d) == [
        {"job_type": "actor", "actor_id": ACTOR, "depth": 3}
    ]


async def test_skips_a_failed_actor_with_no_neighbor_and_no_depth():
    d = dispatcher()
    g = FakeGraph()
    await failed_actor(g, ACTOR, 429)  # edgeless, undated: nowhere to hang it

    count = await recover_http_status(d, g, set())

    assert count == 0
    assert await queued_jobs(d) == []


async def test_skips_a_failed_actor_whose_neighbor_has_no_depth():
    d = dispatcher()
    g = FakeGraph()
    await g.ensure_node(NEIGHBOR)  # bare node, never fetched, no depth
    await failed_actor(g, ACTOR, 429)
    await g.ensure_edge(NEIGHBOR, ACTOR)

    count = await recover_http_status(d, g, set())

    assert count == 0
    assert await queued_jobs(d) == []


# ---------------------------------------------------------------------------
# status filtering: tidy defaults, terminal codes ignored, custom set
# ---------------------------------------------------------------------------


async def test_default_codes_are_the_tidy_transient_set():
    d = dispatcher()
    g = FakeGraph()
    tidy = [408, 429, 500, 502, 503, 504, 520, 524]
    seeds = set()
    for i, status in enumerate(tidy):
        label = f"https://x.example/users/u{i}"
        seeds.add(label)
        await failed_actor(g, label, status)

    count = await recover_http_status(d, g, seeds)

    assert count == len(tidy)


@pytest.mark.parametrize("status", [200, 403, 404, 410, 530])
async def test_non_recoverable_statuses_enqueue_nothing(status):
    d = dispatcher()
    g = FakeGraph()
    await failed_actor(g, SEED, status)

    count = await recover_http_status(d, g, {SEED})

    assert count == 0
    assert await queued_jobs(d) == []


async def test_custom_code_set_overrides_the_default():
    d = dispatcher()
    g = FakeGraph()
    await failed_actor(g, SEED, 530)  # not in the tidy default
    await failed_actor(g, ACTOR, 429)  # in the default, NOT in the override

    count = await recover_http_status(d, g, {SEED, ACTOR}, {530})

    assert count == 1
    assert await queued_jobs(d) == [{"job_type": "actor", "actor_id": SEED, "depth": 0}]


# ---------------------------------------------------------------------------
# collection and page stages
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("direction", ["followers", "following"])
async def test_recovers_a_failed_collection(direction):
    d = dispatcher()
    g = FakeGraph()
    url = f"{ACTOR}/{direction}"
    await fetched_actor(
        g, ACTOR, depth=2, **{direction: url, f"{direction}_http_status": 503}
    )

    count = await recover_http_status(d, g, set())

    assert count == 1
    assert await queued_jobs(d) == [
        {
            "job_type": "collection",
            "collection_id": url,
            "owner_id": ACTOR,
            "direction": direction,
            "depth": 2,
        }
    ]


@pytest.mark.parametrize("direction", ["followers", "following"])
async def test_recovers_a_failed_page_from_the_last_page_pointer(direction):
    d = dispatcher()
    g = FakeGraph()
    page = f"{ACTOR}/{direction}?page=44"
    await fetched_actor(
        g,
        ACTOR,
        depth=1,
        **{
            f"{direction}_last_page": page,
            f"{direction}_last_page_http_status": 429,
        },
    )

    count = await recover_http_status(d, g, set())

    assert count == 1
    assert await queued_jobs(d) == [
        {
            "job_type": "page",
            "page_id": page,
            "owner_id": ACTOR,
            "direction": direction,
            "depth": 1,
        }
    ]


async def test_multiple_recoverable_stages_on_one_node_all_fire():
    # Directions live independent lives: followers died at the collection,
    # following died mid-pagination — both jobs go out, both count.
    d = dispatcher()
    g = FakeGraph()
    await fetched_actor(
        g,
        ACTOR,
        depth=1,
        followers=f"{ACTOR}/followers",
        followers_http_status=502,
        following_last_page=f"{ACTOR}/following?page=7",
        following_last_page_http_status=429,
    )

    count = await recover_http_status(d, g, set())

    assert count == 2
    jobs = await queued_jobs(d)
    assert {
        "job_type": "collection",
        "collection_id": f"{ACTOR}/followers",
        "owner_id": ACTOR,
        "direction": "followers",
        "depth": 1,
    } in jobs
    assert {
        "job_type": "page",
        "page_id": f"{ACTOR}/following?page=7",
        "owner_id": ACTOR,
        "direction": "following",
        "depth": 1,
    } in jobs


async def test_missing_expected_props_skip_the_branch_not_the_run():
    # A recoverable collection status with no collection URL recorded (and a
    # recoverable page status with no last_page) must not raise or enqueue —
    # and must not stop OTHER nodes from being processed.
    d = dispatcher()
    g = FakeGraph()
    await fetched_actor(g, ACTOR, depth=1, followers_http_status=502)
    await fetched_actor(g, OTHER, depth=1, following_last_page_http_status=429)
    await failed_actor(g, SEED, 429)

    count = await recover_http_status(d, g, {SEED})

    assert count == 1
    assert await queued_jobs(d) == [{"job_type": "actor", "actor_id": SEED, "depth": 0}]


# ---------------------------------------------------------------------------
# unconditional enqueue + quiet no-op
# ---------------------------------------------------------------------------


async def test_enqueues_even_though_the_job_was_seen_before():
    # These jobs all ran once, so they're in SEEN; recovery must not gate on it.
    d = dispatcher()
    g = FakeGraph()
    job = {"job_type": "actor", "actor_id": SEED, "depth": 0}
    await d.enqueue(job)  # marks seen
    assert await d.get() == job  # drain the queue (leased, then abandoned)
    assert await d.seen(job)
    await failed_actor(g, SEED, 429)

    count = await recover_http_status(d, g, {SEED})

    assert count == 1
    assert await queued_jobs(d) == [job]


async def test_a_healthy_graph_enqueues_nothing():
    d = dispatcher()
    g = FakeGraph()
    await fetched_actor(g, ACTOR, depth=1, followers=f"{ACTOR}/followers")
    await fetched_actor(g, NEIGHBOR, depth=2)

    count = await recover_http_status(d, g, set())

    assert count == 0
    assert await queued_jobs(d) == []


async def test_seed_membership_outranks_a_stamped_depth():
    # A seed can carry a non-zero stamped depth (first discovered via a page,
    # failed post-fix, webfinger job then SEEN-blocked). Seed status wins:
    # re-enqueue at depth 0, not the stamp.
    d = dispatcher()
    g = FakeGraph()
    await failed_actor(g, SEED, 429, depth=2)

    count = await recover_http_status(d, g, {SEED})

    assert count == 1
    assert await queued_jobs(d) == [{"job_type": "actor", "actor_id": SEED, "depth": 0}]


async def test_one_job_per_direction_collection_outranks_page():
    # Deliberate: when a direction has BOTH a recoverable collection status and
    # a recoverable last-page status, only the collection job goes out — one
    # chain per direction, no parallel restarts. (A later run reaches the page
    # branch once the collection retry has overwritten its status.)
    d = dispatcher()
    g = FakeGraph()
    await fetched_actor(
        g,
        ACTOR,
        depth=1,
        followers=f"{ACTOR}/followers",
        followers_http_status=503,
        followers_last_page=f"{ACTOR}/followers?page=9",
        followers_last_page_http_status=429,
    )

    count = await recover_http_status(d, g, set())

    assert count == 1
    assert await queued_jobs(d) == [
        {
            "job_type": "collection",
            "collection_id": f"{ACTOR}/followers",
            "owner_id": ACTOR,
            "direction": "followers",
            "depth": 1,
        }
    ]
