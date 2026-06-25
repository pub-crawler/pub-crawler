"""Tests for snapshot — write a Graph out as two Parquet files (nodes, edges).

snapshot iterates a Graph (all_nodes -> (id, label, props), all_edges ->
(from_id, to_id, props)) and writes two Parquet files:

  nodes: one row per node, columns
    id, label, hostname, preferred_username, name, published, type,
    followers_count, following_count
  edges: one row per edge, columns `from` and `to` (integer node ids)

The node columns beyond id/label are read from each node's `props` by the
column's own (snake_case) name; a node missing that property gets null in the
column. Only those listed columns are emitted — any other property is ignored.
The edge file carries no properties, just the two endpoint ids.

The signature is snapshot(G, node_filename, edge_filename) — nodes first. The
transaction is the caller's (main()), so snapshot needs only the graph + the two
paths; a FakeGraph drives it with no DB or connection.

Output is verified by reading it back with pyarrow (how the real snapshot is
consumed). Rows are matched by id/label rather than position — snapshot makes no
ordering promise.
"""

from datetime import datetime, timezone

import pyarrow.parquet as pq
import pytest

from snapshot import snapshot
from support import FakeGraph

EVAN = "https://cosocial.example/users/evan"
ALICE = "https://social.example/users/alice"

NODE_COLUMNS = [
    "id",
    "label",
    "hostname",
    "preferred_username",
    "name",
    "published",
    "type",
    "followers_count",
    "following_count",
]
EDGE_COLUMNS = ["from", "to"]


def _rows_by(table, key):
    """Read a parquet file back as {row[key]: row} dicts of Python scalars."""
    return {row[key]: row for row in pq.read_table(table).to_pylist()}


async def test_writes_node_and_edge_parquet_with_expected_columns(tmp_path):
    g = FakeGraph()
    await g.ensure_node(EVAN)
    await g.set_node_properties(
        EVAN,
        {
            "hostname": "cosocial.example",
            "preferred_username": "evan",
            "name": "Evan P",
            "published": "2018-01-01T00:00:00Z",
            "type": "Person",
            "followers_count": 5,
            "following_count": 7,
        },
    )
    await g.ensure_node(ALICE)  # a node with no properties
    await g.ensure_edge(EVAN, ALICE)
    nodes_out = tmp_path / "nodes.parquet"
    edges_out = tmp_path / "edges.parquet"

    await snapshot(g, str(nodes_out), str(edges_out))

    node_table = pq.read_table(str(nodes_out))
    edge_table = pq.read_table(str(edges_out))
    assert node_table.column_names == NODE_COLUMNS
    assert edge_table.column_names == EDGE_COLUMNS

    nodes = _rows_by(str(nodes_out), "label")
    assert set(nodes) == {EVAN, ALICE}
    evan = nodes[EVAN]
    assert evan["hostname"] == "cosocial.example"
    assert evan["preferred_username"] == "evan"
    assert evan["name"] == "Evan P"
    # the ISO `published` string is parsed into a seconds-resolution UTC timestamp;
    # pyarrow hands it back as an aware datetime (compared by instant, not tzinfo)
    assert evan["published"] == datetime(2018, 1, 1, tzinfo=timezone.utc)
    assert evan["type"] == "Person"
    assert evan["followers_count"] == 5  # integers stay integers
    assert evan["following_count"] == 7
    assert isinstance(evan["id"], int)

    # the directed edge is written as the two endpoint ids
    edges = pq.read_table(str(edges_out)).to_pylist()
    assert edges == [{"from": evan["id"], "to": nodes[ALICE]["id"]}]


async def test_missing_node_properties_are_null(tmp_path):
    # A bare node (or one missing some of the personal fields) gets null in every
    # column it has no property for; id and label are always present.
    g = FakeGraph()
    await g.ensure_node(ALICE)
    await g.set_node_property(ALICE, "type", "Service")  # only one field set
    nodes_out = tmp_path / "nodes.parquet"
    edges_out = tmp_path / "edges.parquet"

    await snapshot(g, str(nodes_out), str(edges_out))

    alice = _rows_by(str(nodes_out), "label")[ALICE]
    assert alice["label"] == ALICE
    assert isinstance(alice["id"], int)
    assert alice["type"] == "Service"
    for col in ("hostname", "preferred_username", "name", "published"):
        assert alice[col] is None
    for col in ("followers_count", "following_count"):
        assert alice[col] is None


async def test_ignores_properties_outside_the_column_set(tmp_path):
    # Nodes carry many properties (inbox, summary, icon, ...) that aren't snapshot
    # columns. Only the declared columns are emitted; extras never appear.
    g = FakeGraph()
    await g.ensure_node(EVAN)
    await g.set_node_properties(
        EVAN,
        {"name": "Evan P", "summary": "a bio", "inbox": "https://x.example/inbox"},
    )
    nodes_out = tmp_path / "nodes.parquet"
    edges_out = tmp_path / "edges.parquet"

    await snapshot(g, str(nodes_out), str(edges_out))

    table = pq.read_table(str(nodes_out))
    assert table.column_names == NODE_COLUMNS  # no `summary` / `inbox` columns
    assert _rows_by(str(nodes_out), "label")[EVAN]["name"] == "Evan P"


async def test_unicode_name_roundtrips(tmp_path):
    # Real display names carry emoji/CJK/accents (non-ASCII bit the first real
    # crawl). Parquet stores UTF-8 natively, so the value must come back intact —
    # no escaping, no mangling.
    name = "Ian 🇨🇦 韓 café ⁂"
    g = FakeGraph()
    await g.ensure_node(EVAN)
    await g.set_node_property(EVAN, "name", name)
    nodes_out = tmp_path / "nodes.parquet"
    edges_out = tmp_path / "edges.parquet"

    await snapshot(g, str(nodes_out), str(edges_out))

    assert _rows_by(str(nodes_out), "label")[EVAN]["name"] == name


async def test_empty_graph_writes_empty_typed_parquet(tmp_path):
    # No nodes/edges still produces two readable files carrying the full schema
    # (downstream consumers depend on a stable column set), just zero rows.
    nodes_out = tmp_path / "nodes.parquet"
    edges_out = tmp_path / "edges.parquet"

    await snapshot(FakeGraph(), str(nodes_out), str(edges_out))

    node_table = pq.read_table(str(nodes_out))
    edge_table = pq.read_table(str(edges_out))
    assert node_table.column_names == NODE_COLUMNS
    assert edge_table.column_names == EDGE_COLUMNS
    assert node_table.num_rows == 0
    assert edge_table.num_rows == 0


class RacyGraph(FakeGraph):
    """A FakeGraph whose edge pass yields endpoints the node pass never did.

    snapshot writes every node row first, then every edge row. With the crawler
    running, nodes are inserted between those two passes, so all_edges can surface
    an edge whose source/target id has no node row. add_dangling_edge injects
    exactly that: an edge to/from an id all_nodes never yields."""

    def __init__(self):
        super().__init__()
        self._dangling = []  # (from_id, to_id, props) with no node row

    def add_dangling_edge(self, from_id, to_id, props=None):
        self._dangling.append((from_id, to_id, props or {}))

    async def all_edges(self):
        async for edge in super().all_edges():
            yield edge
        for edge in self._dangling:
            yield edge


async def test_skips_edges_whose_endpoint_has_no_node_row(tmp_path):
    # The race: the crawler adds nodes (ids never emitted in the node pass) and
    # edges to/from them during snapshot's edge pass. Parquet won't reject such an
    # edge (unlike GML), so the edge file would carry `from`/`to` ids that point at
    # no node row -- dangling references. snapshot must drop them, keeping only
    # edges whose both endpoints got a node row.
    #
    # The dangling ids sit just ABOVE the highest emitted id (ids auto-increment
    # and nodes are never deleted, so a later node always gets a higher id). The
    # surviving edge points AT the node holding that highest id (ALICE), so an
    # off-by-one in the cutoff (>= vs >) would wrongly drop it and fail here.
    g = RacyGraph()
    await g.ensure_node(EVAN)
    await g.ensure_node(ALICE)  # ALICE gets the highest id of the declared nodes
    await g.ensure_edge(EVAN, ALICE)  # both endpoints declared -> survives
    max_id = g._ids[ALICE]
    g.add_dangling_edge(g._ids[EVAN], max_id + 1)  # target added after node pass
    g.add_dangling_edge(max_id + 2, g._ids[ALICE])  # source added after node pass
    nodes_out = tmp_path / "nodes.parquet"
    edges_out = tmp_path / "edges.parquet"

    await snapshot(g, str(nodes_out), str(edges_out))

    edges = pq.read_table(str(edges_out)).to_pylist()
    assert edges == [{"from": g._ids[EVAN], "to": g._ids[ALICE]}]  # only the survivor


@pytest.mark.parametrize(
    "bad_published",
    [
        "-0001-11-30T00:00:00Z",  # the "year zero" value that actually crashed prod
        "0000-00-00T00:00:00Z",  # another null-date sentinel some software emits
        "not a date",  # outright garbage
    ],
)
async def test_malformed_published_does_not_sink_the_snapshot(tmp_path, bad_published):
    # A single actor with an unparseable `published` must not crash the node pass —
    # in production one such row (`-0001-11-30T00:00:00Z`) failed every nightly run.
    # The bad value parses to null (same as a missing one), that node is still
    # written, and a well-formed node alongside it is unaffected.
    g = FakeGraph()
    await g.ensure_node(EVAN)
    await g.set_node_properties(EVAN, {"name": "Evan P", "published": bad_published})
    await g.ensure_node(ALICE)
    await g.set_node_property(ALICE, "published", "2020-06-01T00:00:00Z")
    nodes_out = tmp_path / "nodes.parquet"
    edges_out = tmp_path / "edges.parquet"

    await snapshot(g, str(nodes_out), str(edges_out))  # must not raise

    nodes = _rows_by(str(nodes_out), "label")
    assert set(nodes) == {EVAN, ALICE}  # the bad row didn't drop the whole snapshot
    assert nodes[EVAN]["published"] is None  # unparseable -> null
    assert nodes[EVAN]["name"] == "Evan P"  # the rest of the bad node survives
    # the well-formed neighbour is untouched
    assert nodes[ALICE]["published"] == datetime(2020, 6, 1, tzinfo=timezone.utc)
