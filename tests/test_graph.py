"""Contract tests for the Graph interface — every implementation must satisfy them.

The same assertions run against each backend through the parametrized `graph`
fixture: FakeGraph always, and DatabaseGraph against a real Postgres when
TEST_DATABASE_URL is set (the `db`-marked params, deselected by default). That's
what keeps FakeGraph provably equivalent to the real one — the handler tests
trust the fake only because these hold for both.

The interface is async (DatabaseGraph does async DB I/O):
  await ensure_node(label) / has_node(label)->bool / delete_node(label)
  await ensure_edge(f, t)   / has_edge(f, t)->bool / delete_edge(f, t)
  await set_node_property(label, name, value)
  await get_node_property(label, name)        -> value (None if absent)
  await get_node_properties(label)            -> {name: value}
  (edge equivalents)
  async for node_id, label, props in all_nodes()      # int id (for GML)
  async for from_id, to_id, props in all_edges()      # int endpoint ids only

Assumptions flagged for confirmation:
  - all_nodes() yields (id, label, props); all_edges() yields (from_id, to_id,
    props) — integer node ids, because that's what GML references. all_edges
    carries ONLY ids (no labels); map them back via all_nodes if you need labels.
    props is bundled so the export is one pass, not N+1.
  - all_nodes()/all_edges() run a server-side cursor and do NOT open their own
    transaction — the caller iterates them inside a transaction it owns (one txn
    around both = a consistent snapshot). The db fixture's per-test rollback txn
    supplies that here; FakeGraph ignores it.
  - get_*_property returns None for an absent property.
  - delete_node's behaviour when edges still reference it (cascade vs. error) is
    NOT pinned here — that's the FK ON DELETE policy, your call.
"""

import asyncio

import pytest

from support import FakeGraph

A = "https://a.example/users/a"
B = "https://b.example/users/b"
C = "https://c.example/users/c"


@pytest.fixture(params=["fake", pytest.param("db", marks=pytest.mark.db)])
async def graph(request):
    """The contract subject, one per backend.

    'fake' -> an in-memory FakeGraph. 'db' -> a DatabaseGraph over a real Postgres
    connection *pool*; each table is truncated before the test (rollback isolation
    can't span a pool's many connections), so the same assertions run against real
    SQL from a clean slate. The 'db' param skips until both TEST_DATABASE_URL and
    DatabaseGraph exist.
    """
    if request.param == "fake":
        yield FakeGraph()
        return

    dsn = request.getfixturevalue("pg_dsn")  # skips if unset / asyncpg missing
    import asyncpg

    try:
        from pub_crawler.database import database_setup
        from pub_crawler.database_graph import DatabaseGraph
    except ImportError as exc:
        pytest.skip(f"DatabaseGraph not implemented yet ({exc})")

    # max_size > 1 so concurrent ops land on distinct connections (the whole point
    # of the pool, and what the concurrency test exercises).
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
    try:
        async with pool.acquire() as conn:
            await database_setup(conn)  # idempotent (migration ledger)
            # Pool isolation = truncate, not rollback. RESTART IDENTITY keeps node
            # ids deterministic across tests; CASCADE clears the FK-linked rows.
            # (The migrations ledger is intentionally left intact.)
            await conn.execute(
                "TRUNCATE node, edge, node_property, edge_property "
                "RESTART IDENTITY CASCADE"
            )
        yield DatabaseGraph(pool)
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# nodes: ensure / has / delete
# ---------------------------------------------------------------------------


async def test_ensure_node_then_has_node(graph):
    assert not await graph.has_node(A)
    await graph.ensure_node(A)
    assert await graph.has_node(A)


async def test_ensure_node_is_idempotent(graph):
    await graph.ensure_node(A)
    await graph.ensure_node(A)  # no error, still one node
    assert await graph.has_node(A)


async def test_delete_node(graph):
    await graph.ensure_node(A)
    await graph.delete_node(A)
    assert not await graph.has_node(A)


async def test_delete_node_after_its_id_was_looked_up(graph):
    # Looking the node up first lets a backend that caches label->id populate that
    # cache; delete_node must then invalidate it, or has_node would keep reporting
    # a stale, deleted id as present. (FakeGraph has no such cache, so it passes
    # trivially -- the point is to hold the caching DatabaseGraph to the contract.)
    await graph.ensure_node(A)
    assert await graph.has_node(A)  # seeds any label->id cache
    await graph.delete_node(A)
    assert not await graph.has_node(A)  # must not return a stale cached id


# ---------------------------------------------------------------------------
# edges: ensure / has / delete (directed)
# ---------------------------------------------------------------------------


async def test_ensure_edge_then_has_edge(graph):
    await graph.ensure_node(A)
    await graph.ensure_node(B)
    assert not await graph.has_edge(A, B)
    await graph.ensure_edge(A, B)
    assert await graph.has_edge(A, B)
    assert not await graph.has_edge(B, A)  # directed


async def test_ensure_edge_is_idempotent(graph):
    await graph.ensure_node(A)
    await graph.ensure_node(B)
    await graph.ensure_edge(A, B)
    await graph.ensure_edge(A, B)
    assert await graph.has_edge(A, B)


async def test_delete_edge(graph):
    await graph.ensure_node(A)
    await graph.ensure_node(B)
    await graph.ensure_edge(A, B)
    await graph.delete_edge(A, B)
    assert not await graph.has_edge(A, B)


# ---------------------------------------------------------------------------
# node properties (jsonb: types survive the round-trip)
# ---------------------------------------------------------------------------


async def test_set_and_get_node_property_keeps_type(graph):
    await graph.ensure_node(A)
    await graph.set_node_property(A, "followers_count", 42)
    await graph.set_node_property(A, "preferredUsername", "alice")
    assert await graph.get_node_property(A, "followers_count") == 42  # int, not "42"
    assert await graph.get_node_property(A, "preferredUsername") == "alice"


async def test_get_node_property_absent_is_none(graph):
    await graph.ensure_node(A)
    assert await graph.get_node_property(A, "missing") is None


async def test_set_node_property_overwrites(graph):
    await graph.ensure_node(A)
    await graph.set_node_property(A, "followers_count", 1)
    await graph.set_node_property(A, "followers_count", 99)
    assert await graph.get_node_property(A, "followers_count") == 99


async def test_get_node_properties_returns_all(graph):
    await graph.ensure_node(A)
    await graph.set_node_property(A, "followers_count", 5)
    await graph.set_node_property(A, "type", "Person")
    assert await graph.get_node_properties(A) == {
        "followers_count": 5,
        "type": "Person",
    }


# ---------------------------------------------------------------------------
# edge properties
# ---------------------------------------------------------------------------


async def test_set_and_get_edge_property(graph):
    await graph.ensure_node(A)
    await graph.ensure_node(B)
    await graph.ensure_edge(A, B)
    await graph.set_edge_property(A, B, "from_followers", True)
    assert await graph.get_edge_property(A, B, "from_followers") is True
    assert await graph.get_edge_properties(A, B) == {"from_followers": True}


# ---------------------------------------------------------------------------
# iteration (for the snapshot/GML export)
# ---------------------------------------------------------------------------


async def test_all_nodes_yields_id_label_and_props(graph):
    await graph.ensure_node(A)
    await graph.set_node_property(A, "followers_count", 5)
    await graph.ensure_node(B)  # no props

    by_label = {
        label: (node_id, props) async for node_id, label, props in graph.all_nodes()
    }
    assert by_label.keys() == {A, B}
    assert by_label[A][1] == {"followers_count": 5}
    assert by_label[B][1] == {}
    # ids are integers (GML node ids) and distinct per node
    assert isinstance(by_label[A][0], int)
    assert by_label[A][0] != by_label[B][0]


async def test_all_edges_yields_endpoint_ids_and_props(graph):
    for n in (A, B, C):
        await graph.ensure_node(n)
    await graph.ensure_edge(A, B)
    await graph.ensure_edge(A, C)  # no props
    await graph.set_edge_property(A, B, "direction", "followers")

    # all_edges yields integer endpoint ids (what GML's source/target reference);
    # map id -> label via all_nodes to interpret them, exactly as snapshot.py will.
    id_to_label = {node_id: label async for node_id, label, _ in graph.all_nodes()}

    seen = {}
    async for from_id, to_id, props in graph.all_edges():
        seen[(id_to_label[from_id], id_to_label[to_id])] = props

    assert seen == {(A, B): {"direction": "followers"}, (A, C): {}}


# ---------------------------------------------------------------------------
# concurrency — the crawler's workers all share ONE Graph
# ---------------------------------------------------------------------------


async def test_concurrent_operations_share_the_connection_safely(graph):
    """The crawler runs ~25 workers against a single shared Graph. For
    DatabaseGraph that's one asyncpg connection, which is NOT concurrency-safe —
    overlapping ops raise "another operation is in progress" unless the graph
    serializes access. Trivial for FakeGraph; the real assertion is the db param.
    """
    labels = [f"https://example.test/users/u{i}" for i in range(20)]

    await asyncio.gather(*(graph.ensure_node(label) for label in labels))
    await asyncio.gather(
        *(graph.set_node_property(label, "n", i) for i, label in enumerate(labels))
    )

    for i, label in enumerate(labels):
        assert await graph.get_node_property(label, "n") == i


# ---------------------------------------------------------------------------
# bulk variants — one round-trip for many nodes/edges/props. Same observable
# behaviour as the singular methods; the edge bulk-ops require the nodes (and,
# for the edge-property setters, the edges) to already exist.
# ---------------------------------------------------------------------------


async def test_ensure_nodes_creates_all_and_is_idempotent(graph):
    await graph.ensure_nodes([])  # empty is a no-op, not an error
    await graph.ensure_nodes([A, B, C])
    await graph.ensure_nodes([A, B, C])  # idempotent
    assert await graph.has_node(A)
    assert await graph.has_node(B)
    assert await graph.has_node(C)


async def test_ensure_from_edges_fans_out_directed(graph):
    await graph.ensure_nodes([A, B, C])  # edges require pre-existing nodes
    await graph.ensure_from_edges(A, [B, C])
    assert await graph.has_edge(A, B)
    assert await graph.has_edge(A, C)
    assert not await graph.has_edge(B, A)  # directed


async def test_ensure_to_edges_fans_in_directed(graph):
    await graph.ensure_nodes([A, B, C])
    await graph.ensure_to_edges([A, B], C)
    assert await graph.has_edge(A, C)
    assert await graph.has_edge(B, C)
    assert not await graph.has_edge(C, A)  # directed


async def test_set_nodes_property_sets_same_on_every_node(graph):
    await graph.ensure_nodes([A, B])
    await graph.set_nodes_property([A, B], "depth", 3)
    assert await graph.get_node_property(A, "depth") == 3
    assert await graph.get_node_property(B, "depth") == 3


async def test_set_node_properties_sets_many_on_one_node(graph):
    await graph.ensure_node(A)
    await graph.set_node_properties(A, {"type": "Person", "depth": 1})
    assert await graph.get_node_property(A, "type") == "Person"
    assert await graph.get_node_property(A, "depth") == 1


async def test_set_edge_properties_sets_many_on_one_edge(graph):
    await graph.ensure_nodes([A, B])
    await graph.ensure_edge(A, B)
    await graph.set_edge_properties(A, B, {"from_following": True, "weight": 2})
    assert await graph.get_edge_property(A, B, "from_following") is True
    assert await graph.get_edge_property(A, B, "weight") == 2


async def test_set_from_edges_property_sets_same_on_fan_out(graph):
    await graph.ensure_nodes([A, B, C])
    await graph.ensure_from_edges(A, [B, C])
    await graph.set_from_edges_property(A, [B, C], "from_following", True)
    assert await graph.get_edge_property(A, B, "from_following") is True
    assert await graph.get_edge_property(A, C, "from_following") is True


async def test_set_to_edges_property_sets_same_on_fan_in(graph):
    await graph.ensure_nodes([A, B, C])
    await graph.ensure_to_edges([A, B], C)
    await graph.set_to_edges_property([A, B], C, "from_followers", True)
    assert await graph.get_edge_property(A, C, "from_followers") is True
    assert await graph.get_edge_property(B, C, "from_followers") is True


async def test_get_nodes_property_keyed_by_label_omits_absent(graph):
    # The last_fetch_date enqueue-gate: only labels that HAVE the property come
    # back, so the new ones are the set difference.
    await graph.ensure_nodes([A, B, C])
    await graph.set_node_property(A, "last_fetch_date", "2026-01-01")
    await graph.set_node_property(B, "last_fetch_date", "2026-01-02")
    # C has no last_fetch_date

    result = await graph.get_nodes_property([A, B, C], "last_fetch_date")

    assert result == {A: "2026-01-01", B: "2026-01-02"}  # C omitted
    assert set([A, B, C]) - result.keys() == {C}  # the "which are new" use case


async def test_get_nodes_property_empty_input_is_empty_dict(graph):
    assert await graph.get_nodes_property([], "anything") == {}


async def test_get_from_edges_property_keyed_by_to_label_omits_absent(graph):
    await graph.ensure_nodes([A, B, C])
    await graph.ensure_from_edges(A, [B, C])
    await graph.set_edge_property(A, B, "from_following", True)
    # A->C has no from_following

    result = await graph.get_from_edges_property(A, [B, C], "from_following")

    assert result == {B: True}  # keyed by to_label, C omitted


async def test_get_to_edges_property_keyed_by_from_label_omits_absent(graph):
    await graph.ensure_nodes([A, B, C])
    await graph.ensure_to_edges([A, B], C)
    await graph.set_edge_property(A, C, "from_followers", True)
    # B->C has no from_followers

    result = await graph.get_to_edges_property([A, B], C, "from_followers")

    assert result == {A: True}  # keyed by from_label, B omitted
