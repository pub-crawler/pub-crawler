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

import pytest

from support import FakeGraph

A = "https://a.example/users/a"
B = "https://b.example/users/b"
C = "https://c.example/users/c"


@pytest.fixture(params=["fake", pytest.param("db", marks=pytest.mark.db)])
async def graph(request):
    """The contract subject, one per backend.

    'fake' -> an in-memory FakeGraph. 'db' -> a DatabaseGraph on a real Postgres
    connection, wrapped in a transaction that's rolled back after the test, so
    the same 14 assertions run against real SQL without leaving anything behind.
    The 'db' param skips until both TEST_DATABASE_URL and DatabaseGraph exist.
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

    conn = await asyncpg.connect(dsn)
    try:
        await database_setup(conn)  # idempotent (migration ledger), commits schema
        tr = conn.transaction()
        await tr.start()
        try:
            yield DatabaseGraph(conn)
        finally:
            await tr.rollback()
    finally:
        await conn.close()


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
