import logging
import json

import redis.asyncio
import asyncpg

from pub_crawler.database import database_setup
from pub_crawler.database_graph import DatabaseGraph
from pub_crawler.dispatcher import QUEUE, INFLIGHT, FAILED, SEEN
from pub_crawler.job_id import job_id


async def del_seen(r):
    await r.delete(SEEN)
    logging.info(f"Deleted seen set")


async def add_in_flight_to_seen(r):
    for key in await r.hkeys(INFLIGHT):
        job = None
        try:
            job = json.loads(key)
        except Exception as e:
            logging.warning(f"Could not parse in-flight job {key}: {e}; skipping")
            continue
        jid = job_id(job)
        if jid is None:
            logging.warning(f"Unidentifiable in-flight job: {key}; skipping")
            continue
        try:
            await r.sadd(SEEN, jid)
        except Exception as e:
            logging.warning(
                f"Exception adding in-flight job {jid} to seen: {e}; skipping"
            )
            continue
        logging.info(f"Added in-flight job {jid}")


async def add_failed_to_seen(r):
    async for key in r.sscan_iter(FAILED):
        job = None
        try:
            job = json.loads(key)
        except Exception as e:
            logging.warning(f"Could not parse failed job: {key} because {e}; skipping")
            continue
        jid = job_id(job)
        if jid is None:
            logging.warning(f"Unidentifiable failed job: {key}; skipping")
            continue
        try:
            await r.sadd(SEEN, jid)
        except Exception as e:
            logging.warning(f"Exception adding failed job {jid} to seen: {e}; skipping")
            continue
        logging.info(f"Added failed job {jid}")


async def add_graph_to_seen(r, G):
    async for id, label, props in G.all_nodes():
        if "last_fetch_date" not in props:
            logging.warning(f"Unfetched actor: {label}; skipping")
            continue
        job = {"job_type": "actor", "actor_id": label, "depth": props.get("depth")}
        jid = job_id(job)
        if jid is None:
            logging.warning(f"Unidentifiable successful job: {job}; skipping")
            continue
        try:
            await r.sadd(SEEN, jid)
        except Exception as e:
            logging.warning(
                f"Exception adding successful job {jid} to seen: {e}; skipping"
            )
            continue
        logging.info(f"Added successful job {jid}")


async def add_queue_to_seen_and_dedupe(r):
    async for member, score in r.zscan_iter(QUEUE):
        _, jobstr = member.decode().split("|", 1)
        job = None
        try:
            job = json.loads(jobstr)
        except Exception as e:
            logging.warning(f"Could not parse queue job {jobstr}: {e}; skipping")
            continue
        jid = job_id(job)
        if jid is None:
            logging.warning(f"Unidentifiable queue job: {job}; skipping")
            continue
        res = None
        try:
            res = await r.sadd(SEEN, jid)
        except Exception as e:
            logging.warning(f"Exception adding queue job {jid} to seen: {e}; skipping")
            continue
        if res == 0:
            logging.info(f"Duplicate job in queue {jid}; removing")
            try:
                await r.zrem(QUEUE, member)
            except Exception as e:
                logging.warning(
                    f"Exception removing duplicate job {jid} from queue: {e}; skipping"
                )
                continue
        else:
            logging.info(f"Added queue job {jid}")


async def fixup_seen(r, G):
    await del_seen(r)
    await add_in_flight_to_seen(r)
    await add_failed_to_seen(r)
    await add_graph_to_seen(r, G)
    await add_queue_to_seen_and_dedupe(r)


async def main(redis_url, database_url):
    r = redis.asyncio.Redis.from_url(redis_url)
    pool = await asyncpg.create_pool(database_url)
    async with pool.acquire() as conn:
        await database_setup(conn)
    G = DatabaseGraph(pool)

    await fixup_seen(r, G)


if __name__ == "__main__":
    import os
    import asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        print("Set REDIS_URL environment variable")
        exit(-1)

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("Set DATABASE_URL environment variable")
        exit(-1)

    asyncio.run(main(redis_url, database_url))
