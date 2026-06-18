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

EVAN = "https://cosocial.example/users/evan"
ALICE = "https://social.example/users/alice"


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


class RacyGraph(FakeGraph):
    """A FakeGraph whose edge pass yields endpoints the node pass never did.

    snapshot writes every `node` stanza first, then every `edge` stanza. With the
    crawler running, nodes are inserted between those two passes, so all_edges can
    surface an edge whose source/target id has no `node` stanza. add_dangling_edge
    injects exactly that: an edge to/from an id all_nodes never yields."""

    def __init__(self):
        super().__init__()
        self._dangling = []  # (from_id, to_id, props) with no node stanza

    def add_dangling_edge(self, from_id, to_id, props=None):
        self._dangling.append((from_id, to_id, props or {}))

    async def all_edges(self):
        async for edge in super().all_edges():
            yield edge
        for edge in self._dangling:
            yield edge


async def test_skips_edges_whose_endpoint_has_no_node_stanza(tmp_path):
    # The race: the crawler adds nodes (ids never emitted in the node pass) and
    # edges to/from them during snapshot's edge pass. An edge referencing an
    # undeclared node id makes nx.read_gml reject the WHOLE file, so those edges
    # must be skipped -- keeping only edges whose both endpoints got a stanza.
    #
    # The dangling ids sit just ABOVE the highest emitted id (ids auto-increment
    # and nodes are never deleted, so a later node always gets a higher id). The
    # surviving edge points AT the node holding that highest id (ALICE), so an
    # off-by-one in the cutoff (>= vs >) would wrongly drop it and fail here.
    g = RacyGraph()
    await g.ensure_node(EVAN)
    await g.ensure_node(ALICE)  # ALICE gets the highest id of the declared nodes
    await g.ensure_edge(EVAN, ALICE)  # both endpoints declared -> survives
    await g.set_edge_property(EVAN, ALICE, "direction", "followers")
    max_id = g._ids[ALICE]
    g.add_dangling_edge(g._ids[EVAN], max_id + 1)  # target added after node pass
    g.add_dangling_edge(max_id + 2, g._ids[ALICE])  # source added after node pass
    out = tmp_path / "graph.gml"

    await snapshot(g, str(out))

    read = nx.read_gml(str(out))  # RAISES if a dangling edge slipped through
    assert set(read.nodes) == {EVAN, ALICE}
    assert read.number_of_edges() == 1  # only the fully-declared edge remains
    assert read.edges[EVAN, ALICE]["direction"] == "followers"
