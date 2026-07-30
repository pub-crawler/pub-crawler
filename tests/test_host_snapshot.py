"""Tests for host_snapshot — write the HostSurvey out as one Parquet file.

snapshot_hosts(survey, filename) iterates the HostSurvey container's
all_hosts() -> (id, hostname, props) and writes one row per host with
columns:

  id, hostname, last_fetch_date, failure, error_detail, nodeinfo_version,
  software_name, software_version, users_total, users_active_month,
  users_active_halfyear, local_posts, local_comments

Columns beyond id/hostname are read from each host's props by the column's
own name; a host missing a property gets null (a never-surveyed host is all
nulls after id/hostname). Only the listed columns are emitted — any other
property is ignored. last_fetch_date (ISO-8601 string in the store) is
parsed into a seconds-resolution UTC timestamp column; an unparseable value
becomes null without sinking the run (the `published` lesson from
snapshot.py). The count columns are int64, clamped snapshot.py-style (out of
[0, 2^63-1] -> null) — wider than the node snapshot's int32 since no GML
consumer constrains them. Everything else is a plain string column.

Like snapshot.py: the writer batches record_batches, makes no row-ordering
promise, and an empty graph still yields a readable zero-row file with the
full schema. Output is verified by reading it back with pyarrow.

Assumed contract (adjust the tests if the shape differs).
"""

from datetime import datetime, timezone

import pyarrow.parquet as pq

from host_snapshot import snapshot_hosts
from support import FakeHostSurvey

HOST_COLUMNS = [
    "id",
    "hostname",
    "last_fetch_date",
    "failure",
    "error_detail",
    "nodeinfo_version",
    "software_name",
    "software_version",
    "users_total",
    "users_active_month",
    "users_active_halfyear",
    "local_posts",
    "local_comments",
]

SURVEYED = {
    "last_fetch_date": "2026-07-30T12:00:00+00:00",
    "nodeinfo_version": "2.0",
    "software_name": "mastodon",
    "software_version": "4.3.2",
    "users_total": 890000,
    "users_active_month": 230000,
    "users_active_halfyear": 510000,
    "local_posts": 120000000,
}

FAILED = {
    "last_fetch_date": "2026-07-29T00:00:00+00:00",
    "failure": "dns_error",
    "error_detail": "ConnectError('[Errno 8] nodename nor servname provided')",
}


def rows_by_hostname(filename):
    return {row["hostname"]: row for row in pq.read_table(filename).to_pylist()}


async def survey_with(**hosts):
    survey = FakeHostSurvey()
    for hostname, props in hosts.items():
        await survey.ensure_host(hostname)
        if props:
            await survey.set_host_properties(hostname, props)
    return survey


async def test_writes_host_parquet_with_expected_columns(tmp_path):
    g = await survey_with(
        **{
            "live.example": dict(SURVEYED),
            "dead.example": dict(FAILED),
            "bare.example": None,  # seeded but never surveyed
        }
    )
    out = tmp_path / "hosts.parquet"

    await snapshot_hosts(g, str(out))

    table = pq.read_table(str(out))
    assert table.column_names == HOST_COLUMNS

    rows = rows_by_hostname(str(out))
    assert set(rows) == {"live.example", "dead.example", "bare.example"}

    live = rows["live.example"]
    assert isinstance(live["id"], int)
    assert live["software_name"] == "mastodon"
    assert live["software_version"] == "4.3.2"
    assert live["users_total"] == 890000  # integers stay integers
    assert live["local_posts"] == 120000000
    assert live["failure"] is None  # surveyed clean: no failure
    assert live["local_comments"] is None  # property absent -> null

    dead = rows["dead.example"]
    assert dead["failure"] == "dns_error"
    assert dead["error_detail"] == FAILED["error_detail"]
    assert dead["software_name"] is None

    bare = rows["bare.example"]
    assert isinstance(bare["id"], int)
    for column in HOST_COLUMNS[2:]:
        assert bare[column] is None
    assert len({rows[h]["id"] for h in rows}) == 3  # ids distinct


async def test_last_fetch_date_becomes_a_utc_timestamp(tmp_path):
    g = await survey_with(**{"live.example": dict(SURVEYED)})
    out = tmp_path / "hosts.parquet"

    await snapshot_hosts(g, str(out))

    live = rows_by_hostname(str(out))["live.example"]
    assert live["last_fetch_date"] == datetime(2026, 7, 30, 12, tzinfo=timezone.utc)


async def test_malformed_last_fetch_date_does_not_sink_the_snapshot(tmp_path):
    # The `published` lesson: one corrupt timestamp must not crash the pass.
    # The bad value nulls out, the row survives, neighbours are unaffected.
    g = await survey_with(
        **{
            "corrupt.example": {"last_fetch_date": "not a date", "failure": "error"},
            "fine.example": dict(SURVEYED),
        }
    )
    out = tmp_path / "hosts.parquet"

    await snapshot_hosts(g, str(out))  # must not raise

    rows = rows_by_hostname(str(out))
    assert set(rows) == {"corrupt.example", "fine.example"}
    assert rows["corrupt.example"]["last_fetch_date"] is None
    assert rows["corrupt.example"]["failure"] == "error"  # rest of the row survives
    assert rows["fine.example"]["last_fetch_date"] == datetime(
        2026, 7, 30, 12, tzinfo=timezone.utc
    )


async def test_ignores_properties_outside_the_column_set(tmp_path):
    # Future survey versions may store more properties (peers_count, schema
    # validity, ...). Only the declared columns are emitted.
    g = await survey_with(
        **{"live.example": {**SURVEYED, "peers_count": 4200, "extra": "x"}}
    )
    out = tmp_path / "hosts.parquet"

    await snapshot_hosts(g, str(out))

    table = pq.read_table(str(out))
    assert table.column_names == HOST_COLUMNS  # no peers_count / extra columns
    assert rows_by_hostname(str(out))["live.example"]["software_name"] == "mastodon"


async def test_unicode_error_detail_roundtrips(tmp_path):
    # error_detail is a repr of whatever the network threw at us — it can carry
    # non-ASCII (IDN hostnames in messages, for one). Parquet is UTF-8 native.
    detail = "ConnectError('bawü.social: café ⁂ 韓')"
    g = await survey_with(
        **{"idn.example": {"failure": "connect_error", "error_detail": detail}}
    )
    out = tmp_path / "hosts.parquet"

    await snapshot_hosts(g, str(out))

    assert rows_by_hostname(str(out))["idn.example"]["error_detail"] == detail


async def test_empty_graph_writes_empty_typed_parquet(tmp_path):
    # Zero hosts still produces a readable file carrying the full schema.
    out = tmp_path / "hosts.parquet"

    await snapshot_hosts(FakeHostSurvey(), str(out))

    table = pq.read_table(str(out))
    assert table.column_names == HOST_COLUMNS
    assert table.num_rows == 0
