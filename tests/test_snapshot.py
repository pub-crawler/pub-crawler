"""Tests for snapshot — write a Graph out as GML.

snapshot iterates a Graph (all_nodes -> (id, label, props), all_edges ->
(from_id, to_id, props)) and writes a GML file: each node an integer `id` + a
`label` string + its scalar properties, each edge `source`/`target` by id + its
properties. The transaction is the caller's (main()), so snapshot needs only the
graph + a path — a FakeGraph drives it with no DB or connection.

The output is verified by reading it back with networkx (how the real snapshot is
consumed): an invalid file fails to parse rather than silently mis-asserting, and
networkx keys nodes by their `label`, resolving edge source/target ids to those
labels.
"""

import networkx as nx

from snapshot import snapshot
from support import FakeGraph

EVAN = "https://cosocial.ca/users/evan"
ALICE = "https://example.social/users/alice"


async def test_writes_nodes_edges_and_props_readable_by_networkx(tmp_path):
    g = FakeGraph()
    await g.ensure_node(EVAN)
    await g.set_node_property(EVAN, "preferredUsername", "evan")
    await g.set_node_property(EVAN, "followers_count", 5)
    await g.ensure_node(ALICE)  # a node with no properties
    await g.ensure_edge(EVAN, ALICE)
    await g.set_edge_property(EVAN, ALICE, "direction", "followers")
    out = tmp_path / "graph.gml"

    await snapshot(g, str(out))

    read = nx.read_gml(str(out))
    assert read.is_directed()  # "directed 1"
    assert set(read.nodes) == {EVAN, ALICE}
    # scalar props round-trip with their types preserved
    assert read.nodes[EVAN]["preferredUsername"] == "evan"
    assert read.nodes[EVAN]["followers_count"] == 5
    assert dict(read.nodes[ALICE]) == {}  # no props -> empty node block
    # the edge is directed and carries its property
    assert read.has_edge(EVAN, ALICE)
    assert not read.has_edge(ALICE, EVAN)
    assert read.edges[EVAN, ALICE]["direction"] == "followers"


async def test_exports_booleans_as_0_1(tmp_path):
    # GML has no boolean; the convention is 0/1. A plain `type(v) == int` check
    # would silently DROP these (bool isn't int by ==), so this guards that path —
    # and it's the shape the edge-provenance flags will take.
    g = FakeGraph()
    await g.ensure_node(EVAN)
    await g.ensure_node(ALICE)
    await g.ensure_edge(EVAN, ALICE)
    await g.set_edge_property(EVAN, ALICE, "from_followers", True)
    await g.set_edge_property(EVAN, ALICE, "from_following", False)
    out = tmp_path / "graph.gml"

    await snapshot(g, str(out))

    read = nx.read_gml(str(out))
    assert read.edges[EVAN, ALICE]["from_followers"] == 1
    assert read.edges[EVAN, ALICE]["from_following"] == 0


async def test_escapes_quotes_and_ampersands_in_strings(tmp_path):
    # Display names can contain " and & — both break a raw GML string (the " ends
    # it, the & is the escape introducer). networkx round-trips the numeric refs
    # &#34; / &#38;, so a correctly-escaped value reads back intact.
    g = FakeGraph()
    await g.ensure_node(EVAN)
    await g.set_node_property(EVAN, "name", 'Ann "Banksy" & Co')
    out = tmp_path / "graph.gml"

    await snapshot(g, str(out))

    read = nx.read_gml(str(out))  # raises on an unescaped "
    assert read.nodes[EVAN]["name"] == 'Ann "Banksy" & Co'


async def test_escapes_non_ascii_characters(tmp_path):
    # Real fediverse display names are full of emoji/CJK/accents, and nx.read_gml
    # reads GML as ASCII — so non-ASCII must be escaped to numeric refs (&#N;) or
    # the file won't even decode. (This is what bit the first real crawl.)
    g = FakeGraph()
    await g.ensure_node(EVAN)
    await g.set_node_property(EVAN, "name", "Ian 🇨🇦 韓 café ⁂")
    out = tmp_path / "graph.gml"

    await snapshot(g, str(out))

    # File is pure ASCII...
    out.read_text(encoding="ascii")  # raises if any raw non-ASCII slipped through
    # ...and the characters round-trip.
    read = nx.read_gml(str(out))
    assert read.nodes[EVAN]["name"] == "Ian 🇨🇦 韓 café ⁂"


async def test_empty_graph_is_valid_gml(tmp_path):
    out = tmp_path / "graph.gml"

    await snapshot(FakeGraph(), str(out))

    read = nx.read_gml(str(out))
    assert read.number_of_nodes() == 0
    assert read.number_of_edges() == 0
