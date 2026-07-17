import asyncio
import uvloop
import logging
from urllib.parse import urlparse
from pathlib import Path

import redis.asyncio
import asyncpg

from crawl import make_dispatcher, DEFAULT_MAX_WORKERS, DEFAULT_PRIVATE_KEY_PEM_FILENAME
from pub_crawler.database import database_setup
from pub_crawler.database_graph import DatabaseGraph

DEFAULT_SEED_ACTOR_IDS = set()
DEFAULT_CODES = {408, 429, 500, 502, 503, 504, 520, 524}


async def recover_http_status(
    dispatcher, G, seed_actor_ids=DEFAULT_SEED_ACTOR_IDS, codes=DEFAULT_CODES
):
    total = 0
    async for id, label, props in G.all_nodes():
        if props.get("http_status", None) in codes:
            logging.info(f"Actor {label} with status code {props["http_status"]}")
            total += await recover_actor(dispatcher, G, label, props, seed_actor_ids)
        else:
            for direction in ["followers", "following"]:
                if props.get(f"{direction}_http_status", None) in codes:
                    logging.info(
                        f"Collection {direction} of {label} with status code {props[f"{direction}_http_status"]}"
                    )
                    total += await recover_collection(
                        dispatcher, label, props, direction
                    )
                elif props.get(f"{direction}_last_page_http_status", None) in codes:
                    logging.info(
                        f"Page {direction} of {label} with status code {props[f"{direction}_last_page_http_status"]}"
                    )
                    total += await recover_page(dispatcher, label, props, direction)
    return total


async def recover_actor(dispatcher, G, actor_id, props, seed_actor_ids):
    depth = -1
    if actor_id in seed_actor_ids:
        depth = 0
    elif "depth" in props:
        depth = props["depth"]
    else:
        neighbor = await G.first_neighbor(actor_id)
        if neighbor is None:
            logging.warning(f"Skipping actor job of {actor_id}: no first neighbor")
            return 0
        neighbor_depth = await G.get_node_property(neighbor, "depth")
        if neighbor_depth is None:
            logging.warning(
                f"Skipping actor job of {actor_id}: no first neighbor depth"
            )
            return 0
        depth = neighbor_depth + 1
    job = {
        "job_type": "actor",
        "actor_id": actor_id,
        "depth": depth,
    }
    await dispatcher.enqueue(job)
    logging.info(f"Enqueued actor job for {actor_id}")
    return 1


async def recover_collection(dispatcher, actor_id, props, direction):

    collection_id = props.get(direction, None)

    if collection_id is None:
        logging.warning(
            f"Skipping collection job for {direction} of {actor_id}: no collection id"
        )
        return 0

    depth = props.get("depth", None)

    if depth is None:
        logging.warning(
            f"Skipping collection job for {direction} of {actor_id}: no depth"
        )
        return 0

    job = {
        "job_type": "collection",
        "collection_id": collection_id,
        "owner_id": actor_id,
        "direction": direction,
        "depth": depth,
    }

    await dispatcher.enqueue(job)

    logging.info(f"Enqueued collection job for {direction} of {actor_id}")

    return 1


async def recover_page(dispatcher, actor_id, props, direction):

    page_id = props.get(f"{direction}_last_page", None)

    if page_id is None:
        logging.warning(f"Skipping page job for {direction} of {actor_id}: no page id")
        return 0

    depth = props.get("depth", None)

    if depth is None:
        logging.warning(f"Skipping page job for {direction} of {actor_id}: no depth")
        return 0

    job = {
        "job_type": "page",
        "page_id": page_id,
        "owner_id": actor_id,
        "direction": direction,
        "depth": depth,
    }

    await dispatcher.enqueue(job)

    logging.info(f"Enqueued page job for {direction} of {actor_id}")

    return 1


async def get_seed_actor_ids(seed_file):
    actor_ids = set()
    with open(seed_file) as f:
        for line in f:
            line = line.strip()
            if not line or line == "webfinger,actor_id":
                continue
            wf, actor_id = line.split(",", 1)
            actor_ids.add(actor_id.strip())
    return actor_ids


async def main(redis_url, database_url, private_key_pem_filename, seed_file):

    private_key_pem_data = Path(private_key_pem_filename).read_text()

    r = redis.asyncio.Redis.from_url(redis_url)
    max_conns = max(DEFAULT_MAX_WORKERS // 2, 1)
    pool = await asyncpg.create_pool(
        database_url, max_size=max_conns, min_size=min(max_conns, 10)
    )
    async with pool.acquire() as conn:
        await database_setup(conn)

    try:
        actor_ids = await get_seed_actor_ids(seed_file)
        G = DatabaseGraph(pool)
        dispatcher = make_dispatcher(r, G, private_key_pem_data=private_key_pem_data)
        count = await recover_http_status(dispatcher, G, seed_actor_ids=actor_ids)
    finally:
        await r.aclose()
        await pool.close()
    return count


if __name__ == "__main__":
    import os
    import sys
    import argparse

    uvloop.install()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    for name in ("hpack", "h2", "httpcore", "httpx"):
        logging.getLogger(name).setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(
        description="Recover failed jobs by http status if possible."
    )

    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL connection URL (env: DATABASE_URL)",
    )
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("REDIS_URL"),
        help="Redis connection URL (env: REDIS_URL)",
    )
    parser.add_argument(
        "--private-key-pem",
        default=os.environ.get("PRIVATE_KEY_PEM", DEFAULT_PRIVATE_KEY_PEM_FILENAME),
        help="path to the PEM private key file "
        f"(env: PRIVATE_KEY_PEM, default: {DEFAULT_PRIVATE_KEY_PEM_FILENAME})",
    )
    parser.add_argument(
        "--seed-file",
        default=os.environ.get("SEED_FILE", None),
        help="CSV (webfinger, actor_id) of seeds (env: SEED_FILE)",
    )

    args = parser.parse_args()

    if not args.database_url:
        print("Set DATABASE_URL environment variable or pass --database-url")
        sys.exit(1)

    if not args.redis_url:
        print("Set REDIS_URL environment variable or pass --redis-url")
        sys.exit(1)

    if not args.seed_file:
        print("Set SEED_FILE environment variable or pass --seed-file")
        sys.exit(1)

    count = asyncio.run(
        main(args.redis_url, args.database_url, args.private_key_pem, args.seed_file)
    )
    print(f"{count} jobs updated")
