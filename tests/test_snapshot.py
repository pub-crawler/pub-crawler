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


async def test_escapes_control_chars_and_non_ascii(tmp_path):
    # Real display names carry emoji/CJK/accents (non-ASCII is what bit the first
    # real crawl), and bios/names can carry embedded newlines/tabs. GML is read as
    # ASCII and is line-oriented, so everything outside printable ASCII — incl. \n
    # and \t — must be escaped to numeric refs (&#N;) or the file won't parse.
    name = "Ian 🇨🇦 韓 café ⁂\nsecond line\ttabbed"
    g = FakeGraph()
    await g.ensure_node(EVAN)
    await g.set_node_property(EVAN, "name", name)
    out = tmp_path / "graph.gml"

    await snapshot(g, str(out))

    out.read_text(
        encoding="ascii"
    )  # pure ASCII: raises if a raw non-ASCII byte slipped through
    read = nx.read_gml(str(out))  # a raw \n would split the string and break the parse
    assert read.nodes[EVAN]["name"] == name


async def test_empty_graph_is_valid_gml(tmp_path):
    out = tmp_path / "graph.gml"

    await snapshot(FakeGraph(), str(out))

    read = nx.read_gml(str(out))
    assert read.number_of_nodes() == 0
    assert read.number_of_edges() == 0
