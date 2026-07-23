"""Contract tests for the Graph host methods — every implementation must satisfy.

Hosts are the second entity family on the Graph (for the host survey): a
`host` table mirroring `node` (id, hostname UNIQUE, created_at) and a
`host_property` table mirroring `node_property` (JSONB values). The API is
nine methods, each an exact mirror of its node counterpart, with `hostname`
in place of `label` and no edges:

  await ensure_host(hostname) / ensure_hosts(hostnames)
  await has_host(hostname) -> bool / delete_host(hostname)
  await set_host_property(hostname, name, value)
  await set_hosts_property(hostnames, name, value)     # same value on many
  await set_host_properties(hostname, properties)      # many values on one
  await get_host_property(hostname, name)  -> value (None if absent)
  await get_hosts_property(hostnames, name) -> {hostname: value}, absent omitted
  await get_host_properties(hostname)      -> {name: value}
  async for host_id, hostname, props in all_hosts()    # int id, streaming

Like the node contract in test_graph.py, the same assertions run against each
backend through the parametrized `graph` fixture: FakeGraph always, and
DatabaseGraph against a real Postgres when TEST_DATABASE_URL is set (the
`db`-marked params, deselected by default).

Assumptions flagged for confirmation:
  - Hostnames arrive already normalized (lowercase, A-label) — the survey
    driver's job. The graph stores what it's given; no normalization pinned.
  - Hosts and nodes are separate namespaces: ensuring a host neither creates
    a node nor appears in all_nodes(), and vice versa.
  - get_hosts_property omits hostnames lacking the property — that absence is
    the survey's "never surveyed" gate (mirrors get_nodes_property).
  - all_hosts() streams like all_nodes(): server-side cursor, own transaction,
    not a consistent snapshot alongside other iterators.
  - delete_host cascades to the host's properties (FK ON DELETE CASCADE).
"""

import asyncio

import pytest

from support import FakeGraph

H1 = "a.example"
H2 = "b.example"
H3 = "c.example"


@pytest.fixture(params=["fake", pytest.param("db", marks=pytest.mark.db)])
async def graph(request):
    """The contract subject, one per backend — same shape as test_graph.py's."""
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

    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
    try:
        async with pool.acquire() as conn:
            await database_setup(conn)  # applies the host migrations via the ledger
            # Both families: this suite asserts exact all_nodes() contents too,
            # and rows from a prior test_graph.py db run would otherwise leak in.
            await conn.execute(
                "TRUNCATE node, edge, node_property, edge_property, "
                "host, host_property RESTART IDENTITY CASCADE"
            )
        yield DatabaseGraph(pool)
    finally:
        await pool.close()


async def hostnames_in(graph):
    """The set of hostnames the graph currently holds, via all_hosts (so the
    ensure tests also pin iteration, independently of has_host)."""
    return {hostname async for _, hostname, _ in graph.all_hosts()}


# ---------------------------------------------------------------------------
# ensure: single, bulk, idempotent
# ---------------------------------------------------------------------------


async def test_ensure_host_then_all_hosts_contains_it(graph):
    assert await hostnames_in(graph) == set()
    await graph.ensure_host(H1)
    assert await hostnames_in(graph) == {H1}


async def test_ensure_host_is_idempotent(graph):
    await graph.ensure_host(H1)
    await graph.ensure_host(H1)  # no error, still one host

    assert [h async for _, h, _ in graph.all_hosts()] == [H1]


async def test_ensure_hosts_creates_all_and_is_idempotent(graph):
    await graph.ensure_hosts([])  # empty is a no-op, not an error
    await graph.ensure_hosts([H1, H2, H3])
    await graph.ensure_hosts([H1, H2, H3])  # idempotent

    assert await hostnames_in(graph) == {H1, H2, H3}


async def test_re_ensuring_does_not_burn_ids(graph):
    # Unlike node, host ids should stay (mostly) consecutive: re-ensuring an
    # existing host must not consume identity values (the insert filters
    # existing rows before the default is evaluated, rather than relying on
    # ON CONFLICT alone). Concurrent-race gaps are tolerated; systematic
    # re-ensure burn is not.
    await graph.ensure_host(H1)
    await graph.ensure_host(H1)  # re-ensure, single
    await graph.ensure_hosts([H1])  # re-ensure, bulk
    await graph.ensure_host(H2)

    ids = {h: host_id async for host_id, h, _ in graph.all_hosts()}

    assert ids[H2] == ids[H1] + 1


# ---------------------------------------------------------------------------
# has / delete
# ---------------------------------------------------------------------------


async def test_has_host_after_ensure(graph):
    assert not await graph.has_host(H1)
    await graph.ensure_host(H1)
    assert await graph.has_host(H1)


async def test_delete_host(graph):
    await graph.ensure_host(H1)
    await graph.delete_host(H1)
    assert not await graph.has_host(H1)


async def test_delete_host_after_its_id_was_looked_up(graph):
    # Mirrors test_graph.py's cache pin: if a backend caches hostname->id,
    # looking the host up first seeds that cache and delete_host must
    # invalidate it, or has_host keeps reporting a stale, deleted id.
    await graph.ensure_host(H1)
    assert await graph.has_host(H1)  # seeds any hostname->id cache
    await graph.delete_host(H1)
    assert not await graph.has_host(H1)  # must not return a stale cached id


# ---------------------------------------------------------------------------
# host properties (jsonb: types survive the round-trip)
# ---------------------------------------------------------------------------


async def test_set_and_get_host_property_keeps_type(graph):
    await graph.ensure_host(H1)
    await graph.set_host_property(H1, "users_total", 42)
    await graph.set_host_property(H1, "software_name", "mastodon")
    assert await graph.get_host_property(H1, "users_total") == 42  # int, not "42"
    assert await graph.get_host_property(H1, "software_name") == "mastodon"


async def test_get_host_property_absent_is_none(graph):
    await graph.ensure_host(H1)
    assert await graph.get_host_property(H1, "missing") is None


async def test_set_host_property_overwrites(graph):
    await graph.ensure_host(H1)
    await graph.set_host_property(H1, "stage", "connect")
    await graph.set_host_property(H1, "stage", "complete")
    assert await graph.get_host_property(H1, "stage") == "complete"


async def test_get_host_properties_returns_all(graph):
    await graph.ensure_host(H1)
    await graph.set_host_property(H1, "stage", "complete")
    await graph.set_host_property(H1, "users_total", 5)
    assert await graph.get_host_properties(H1) == {
        "stage": "complete",
        "users_total": 5,
    }


async def test_get_host_properties_of_a_bare_host_is_empty(graph):
    await graph.ensure_host(H1)
    assert await graph.get_host_properties(H1) == {}


# ---------------------------------------------------------------------------
# bulk property variants — same observable behaviour as the singular methods
# ---------------------------------------------------------------------------


async def test_set_host_properties_sets_many_on_one_host(graph):
    # The survey's save: one host's whole result dict in one call.
    await graph.ensure_host(H1)
    await graph.set_host_properties(
        H1,
        {
            "stage": "complete",
            "last_fetch_date": "2026-07-23T12:00:00+00:00",
            "software_name": "mastodon",
            "users_total": 890000,
            "open_registrations": True,
        },
    )

    props = await graph.get_host_properties(H1)

    assert props["stage"] == "complete"
    assert props["users_total"] == 890000  # int survives
    assert props["open_registrations"] is True  # bool survives
    assert props["last_fetch_date"] == "2026-07-23T12:00:00+00:00"


async def test_set_hosts_property_sets_same_on_every_host(graph):
    await graph.ensure_hosts([H1, H2])
    await graph.set_hosts_property([H1, H2], "stage", "dns")
    assert await graph.get_host_property(H1, "stage") == "dns"
    assert await graph.get_host_property(H2, "stage") == "dns"


async def test_get_hosts_property_keyed_by_hostname_omits_absent(graph):
    # The never-surveyed gate: only hosts that HAVE last_fetch_date come back,
    # so the due set includes the difference.
    await graph.ensure_hosts([H1, H2, H3])
    await graph.set_host_property(H1, "last_fetch_date", "2026-07-01T00:00:00+00:00")
    await graph.set_host_property(H2, "last_fetch_date", "2026-07-22T00:00:00+00:00")
    # H3 has never been surveyed

    result = await graph.get_hosts_property([H1, H2, H3], "last_fetch_date")

    assert result == {
        H1: "2026-07-01T00:00:00+00:00",
        H2: "2026-07-22T00:00:00+00:00",
    }
    assert {H1, H2, H3} - result.keys() == {H3}  # the "never surveyed" use case


async def test_get_hosts_property_empty_input_is_empty_dict(graph):
    assert await graph.get_hosts_property([], "anything") == {}


# ---------------------------------------------------------------------------
# iteration (the scan and the host snapshot export)
# ---------------------------------------------------------------------------


async def test_all_hosts_yields_id_hostname_and_props(graph):
    await graph.ensure_host(H1)
    await graph.set_host_property(H1, "stage", "complete")
    await graph.ensure_host(H2)  # no props

    by_hostname = {
        hostname: (host_id, props)
        async for host_id, hostname, props in graph.all_hosts()
    }

    assert by_hostname.keys() == {H1, H2}
    assert by_hostname[H1][1] == {"stage": "complete"}
    assert by_hostname[H2][1] == {}
    # ids are integers and distinct per host
    assert isinstance(by_hostname[H1][0], int)
    assert by_hostname[H1][0] != by_hostname[H2][0]


# ---------------------------------------------------------------------------
# hosts and nodes are separate namespaces
# ---------------------------------------------------------------------------


async def test_hosts_do_not_leak_into_nodes_or_vice_versa(graph):
    await graph.ensure_host(H1)
    await graph.ensure_node("https://a.example/users/a")

    assert await hostnames_in(graph) == {H1}
    node_labels = {label async for _, label, _ in graph.all_nodes()}
    assert node_labels == {"https://a.example/users/a"}
    assert not await graph.has_node(H1)  # the host is not a node


# ---------------------------------------------------------------------------
# concurrency — the survey's ~50 workers all share ONE Graph
# ---------------------------------------------------------------------------


async def test_concurrent_operations_share_the_connection_safely(graph):
    """Mirrors the node-side concurrency contract: overlapping ensure/set ops
    from gathered tasks must serialize safely on the shared pool."""
    hosts = [f"h{i}.example" for i in range(20)]

    await asyncio.gather(*(graph.ensure_host(h) for h in hosts))
    await asyncio.gather(
        *(graph.set_host_property(h, "n", i) for i, h in enumerate(hosts))
    )

    for i, h in enumerate(hosts):
        assert await graph.get_host_property(h, "n") == i
