import logging
import json

import redis.asyncio
import asyncpg

from pub_crawler.database import database_setup
from pub_crawler.database_graph import DatabaseGraph
from pub_crawler.dispatcher import QUEUE, INFLIGHT, FAILED, SEEN
from pub_crawler.job_id import job_id

LOG_INTERVAL = 10_000


async def del_seen(r):
    await r.delete(SEEN)


async def add_in_flight_to_seen(r):
    tried = 0
    succeeded = 0
    for key in await r.hkeys(INFLIGHT):
        tried += 1
        if tried % LOG_INTERVAL == 0:
            logging.info(f"{tried} in-flight jobs seen")
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
        logging.debug(f"Added in-flight job {jid}")
        succeeded += 1
    return tried, succeeded


async def add_failed_to_seen(r):
    tried = 0
    succeeded = 0
    async for key in r.sscan_iter(FAILED):
        tried += 1
        if tried % LOG_INTERVAL == 0:
            logging.info(f"{tried} failed jobs seen")
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
        logging.debug(f"Added failed job {jid}")
        succeeded += 1
    return tried, succeeded


async def add_graph_to_seen(r, G):
    tried = 0
    succeeded = 0
    async for _, label, props in G.all_nodes():
        tried += 1
        if tried % LOG_INTERVAL == 0:
            logging.info(f"{tried} successful jobs seen")
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
        logging.debug(f"Added successful job {jid}")
        succeeded += 1
    return tried, succeeded


async def add_queue_to_seen_and_dedupe(r):
    tried = 0
    succeeded = 0
    deduped = 0
    async for member, _ in r.zscan_iter(QUEUE):
        tried += 1
        if tried % LOG_INTERVAL == 0:
            logging.info(f"{tried} queue jobs seen")
        job = None
        try:
            _, jobstr = member.decode().split("|", 1)
            job = json.loads(jobstr)
        except Exception as e:
            logging.warning(f"Could not parse queue job {member}: {e}; skipping")
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
            logging.debug(f"Duplicate job in queue {jid}; removing")
            try:
                await r.zrem(QUEUE, member)
            except Exception as e:
                logging.warning(
                    f"Exception removing duplicate job {jid} from queue: {e}; skipping"
                )
                continue
            deduped += 1
        else:
            logging.debug(f"Added queue job {jid}")
            succeeded += 1
    return tried, succeeded, deduped


async def fixup_seen(r, G):
    await del_seen(r)
    logging.info("Deleted seen")
    tried, succeeded = await add_in_flight_to_seen(r)
    logging.info(f"{succeeded}/{tried} in-flight jobs added")
    tried, succeeded = await add_failed_to_seen(r)
    logging.info(f"{succeeded}/{tried} failed jobs added")
    tried, succeeded = await add_graph_to_seen(r, G)
    logging.info(f"{succeeded}/{tried} successful jobs added")
    tried, succeeded, deduped = await add_queue_to_seen_and_dedupe(r)
    logging.info(f"{succeeded}/{tried} queue jobs added; {deduped} jobs deduped")


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
