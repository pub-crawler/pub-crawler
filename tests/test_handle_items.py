"""Tests for handle_items — the shared collection-member ingestion step.

    async def handle_items(graph, dispatcher, items, owner_id, direction, depth)

Used by PageHandler and CollectionHandler. For each member item it:
  - extracts the id (a dict's "id", or a bare string; falsy ids skipped),
  - ensures the member node,
  - adds the directional follow edge with a `from_{direction}` flag
    (followers: member -> owner ; following: owner -> member),
  - enqueues an actor job at depth+1 for members not already fetched
    (i.e. with no `last_fetch_date`).

It does NOT write owner node properties (the handlers own that) and assumes the
owner node already exists (the caller ensures it). These tests assert observable
outcomes — graph state + enqueued jobs — not which graph methods are called, so
they hold for both the per-item and the bulk-method implementations.

Pure DI: FakeGraph + FakeDispatcher, no HTTP.
"""

from pub_crawler.handle_items import handle_items
from support import FakeDispatcher, FakeGraph

OWNER = "https://example.com/owner"
MEMBER_A = "https://a.example/users/a"
MEMBER_B = "https://b.example/users/b"
DEPTH = 1


def actor_job(actor_id, depth):
    return {"job_type": "actor", "actor_id": actor_id, "depth": depth}


def actor_jobs(dis):
    return [j for j in dis.enqueued if j["job_type"] == "actor"]


async def run(graph, dis, items, *, direction="followers", depth=DEPTH):
    # The caller ensures the owner node first; mirror that precondition here.
    await graph.ensure_node(OWNER)
    await handle_items(graph, dis, items, OWNER, direction, depth)


async def test_followers_wires_member_to_owner_with_flag():
    graph = FakeGraph()
    dis = FakeDispatcher()

    await run(graph, dis, [MEMBER_A], direction="followers")

    assert await graph.has_node(MEMBER_A)  # member ensured
    assert await graph.has_edge(MEMBER_A, OWNER)  # member -> owner
    assert await graph.get_edge_property(MEMBER_A, OWNER, "from_followers") is True


async def test_following_orients_edge_from_owner_with_flag():
    graph = FakeGraph()
    dis = FakeDispatcher()

    await run(graph, dis, [MEMBER_A], direction="following")

    assert await graph.has_edge(OWNER, MEMBER_A)  # owner -> member (mirror)
    assert not await graph.has_edge(MEMBER_A, OWNER)
    assert await graph.get_edge_property(OWNER, MEMBER_A, "from_following") is True


async def test_enqueues_actor_jobs_at_depth_plus_one():
    graph = FakeGraph()
    dis = FakeDispatcher()

    await run(graph, dis, [MEMBER_A, MEMBER_B], depth=DEPTH)

    # One actor job per member at depth+1. Enqueue order is NOT guaranteed
    # (the unfetched set is unordered), so assert membership, not sequence.
    jobs = actor_jobs(dis)
    assert len(jobs) == 2
    assert actor_job(MEMBER_A, DEPTH + 1) in jobs
    assert actor_job(MEMBER_B, DEPTH + 1) in jobs


async def test_already_fetched_member_is_edged_but_not_re_enqueued():
    graph = FakeGraph()
    dis = FakeDispatcher()
    await graph.ensure_node(MEMBER_A)
    await graph.set_node_property(MEMBER_A, "last_fetch_date", "2026-06-01T00:00:00")

    await run(graph, dis, [MEMBER_A, MEMBER_B])

    # A is already fetched -> no actor job; B is new -> enqueued.
    assert actor_jobs(dis) == [actor_job(MEMBER_B, DEPTH + 1)]
    # ...but A's edge still lands (it's valid regardless of fetch state).
    assert await graph.has_edge(MEMBER_A, OWNER)


async def test_dict_items_use_their_id():
    graph = FakeGraph()
    dis = FakeDispatcher()

    await run(graph, dis, [{"id": MEMBER_A, "type": "Person"}])

    assert await graph.has_edge(MEMBER_A, OWNER)
    assert actor_jobs(dis) == [actor_job(MEMBER_A, DEPTH + 1)]


async def test_skips_falsy_ids():
    graph = FakeGraph()
    dis = FakeDispatcher()

    await run(graph, dis, ["", MEMBER_A, None])

    # The valid member is wired up and enqueued...
    assert await graph.has_edge(MEMBER_A, OWNER)
    assert actor_jobs(dis) == [actor_job(MEMBER_A, DEPTH + 1)]
    # ...and the falsy ids produced no node, edge, or actor job.
    assert not await graph.has_node("")
    assert not await graph.has_node(None)


async def test_empty_items_is_a_noop():
    graph = FakeGraph()
    dis = FakeDispatcher()

    await run(graph, dis, [])

    assert dis.enqueued == []
    assert [e async for e in graph.all_edges()] == []


# ---------------------------------------------------------------------------
# depth on discovery: a newly-discovered member node is stamped with its
# first-discovery depth (depth+1, the same depth as the actor job it gets).
# Set-if-absent — a node that already carries a depth (re-discovered deeper, or
# already fetched) keeps its existing depth; first-discovery is never clobbered.
# ---------------------------------------------------------------------------


async def test_new_member_gets_first_discovery_depth():
    graph = FakeGraph()
    dis = FakeDispatcher()

    await run(graph, dis, [MEMBER_A], depth=1)

    assert await graph.get_node_property(MEMBER_A, "depth") == 2  # depth + 1


async def test_new_member_depth_matches_its_actor_job():
    # the stamped depth equals the depth its enqueued actor job carries
    graph = FakeGraph()
    dis = FakeDispatcher()

    await run(graph, dis, [MEMBER_A], depth=3)

    assert await graph.get_node_property(MEMBER_A, "depth") == 4
    assert actor_jobs(dis) == [actor_job(MEMBER_A, 4)]


async def test_existing_depth_is_not_overwritten():
    # a member re-discovered deeper keeps its shallower first-discovery depth
    graph = FakeGraph()
    dis = FakeDispatcher()
    await graph.ensure_node(MEMBER_A)
    await graph.set_node_property(MEMBER_A, "depth", 1)

    await run(graph, dis, [MEMBER_A], depth=5)  # this path would be depth 6

    assert await graph.get_node_property(MEMBER_A, "depth") == 1  # unchanged


async def test_fetched_member_depth_untouched():
    # an already-fetched node (last_fetch_date + its own depth) is not
    # re-stamped: no job, and no depth clobber
    graph = FakeGraph()
    dis = FakeDispatcher()
    await graph.ensure_node(MEMBER_A)
    await graph.set_node_properties(
        MEMBER_A,
        {
            "depth": 0,
            "last_fetch_date": "2026-07-01T00:00:00Z",
        },
    )

    await run(graph, dis, [MEMBER_A], depth=5)

    assert await graph.get_node_property(MEMBER_A, "depth") == 0
    assert actor_jobs(dis) == []  # already fetched -> no job
