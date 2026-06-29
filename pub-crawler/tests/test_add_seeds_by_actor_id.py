"""Tests for add_seeds_by_actor_id — enqueue actor-id seed jobs onto the queue.

The actor-id counterpart of add_seeds: the input is a file of already-resolved
*actor ids* (one per line, blanks skipped), and for each it enqueues a
{job_type:'actor', actor_id:<id>, depth:0} job onto the dispatcher's Redis ZSET.
It does NOT process the jobs (no fetch), so seeding must make no HTTP calls.

Unlike add_seeds, it stands up an ActivityPubClient + ActorHandler so the
dispatcher has the "actor" handler registered — the AP client parses a PEM at
construction, so the tests pass a real throwaway key. The graph is unused during
seeding, so None is passed for it (as add_seeds hands its handler a None graph).

A FakeAsyncRedis stands in for Valkey; the queue is read back and members parsed
the same way Dispatcher.enqueue writes them ("depth|type|ts|job").
"""

import json

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fakeredis import FakeAsyncRedis, FakeServer

from add_seeds_by_actor_id import add_seeds_by_actor_id
from pub_crawler.dispatcher import QUEUE

# ActivityPubClient loads the PEM at construction, so use a real (throwaway) key.
_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
PEM = _KEY.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
).decode()

X = "https://a.example/users/x"
Y = "https://b.example/users/y"


def fake_redis():
    # Fresh, isolated in-memory async Redis (its own server) per call.
    return FakeAsyncRedis(server=FakeServer())


def no_http(request):
    # Seeding only enqueues; it must never fetch. Any request here is a bug.
    raise AssertionError(f"unexpected HTTP during seeding: {request.url}")


async def run(seeds_path, r):
    # graph (3rd arg) is unused during seeding -> None, matching add_seeds.
    await add_seeds_by_actor_id(
        str(seeds_path),
        r,
        None,
        private_key_pem_data=PEM,
        transport=httpx.MockTransport(no_http),
    )


async def queued_jobs(r):
    """The jobs currently on the queue ZSET, in score order, parsed back.
    Members are `depth|type|ts|job` — the job JSON is after the 3rd `|`."""
    jobs = []
    for member in await r.zrange(QUEUE, 0, -1):
        jobs.append(json.loads(member.decode().split("|", 3)[3]))
    return jobs


async def test_enqueues_an_actor_job_per_id(tmp_path):
    seeds = tmp_path / "ids.txt"
    seeds.write_text(f"{X}\n{Y}\n")
    r = fake_redis()

    await run(seeds, r)

    # FIFO by enqueue order; each id becomes a depth-0 actor job.
    assert await queued_jobs(r) == [
        {"job_type": "actor", "actor_id": X, "depth": 0},
        {"job_type": "actor", "actor_id": Y, "depth": 0},
    ]


async def test_skips_blank_and_whitespace_lines(tmp_path):
    seeds = tmp_path / "ids.txt"
    seeds.write_text(f"  {X}  \n\n\n   \n{Y}\n")
    r = fake_redis()

    await run(seeds, r)

    assert [j["actor_id"] for j in await queued_jobs(r)] == [X, Y]


async def test_follow_up_seeds_append_to_an_existing_queue(tmp_path):
    # The "initial or follow-up" use: a second run adds to what's already queued.
    first = tmp_path / "first.txt"
    first.write_text(f"{X}\n")
    second = tmp_path / "second.txt"
    second.write_text(f"{Y}\n")
    r = fake_redis()

    await run(first, r)
    await run(second, r)

    assert {j["actor_id"] for j in await queued_jobs(r)} == {X, Y}
