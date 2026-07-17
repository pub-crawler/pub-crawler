import logging

DEFAULT_SEED_ACTOR_IDS = frozenset()
DEFAULT_CODES = {408, 429, 500, 502, 503, 504, 520, 524}


async def recover_http_status(
    dispatcher, G, seed_actor_ids=DEFAULT_SEED_ACTOR_IDS, codes=DEFAULT_CODES
):
    total = 0
    async for id, label, props in G.all_nodes():
        if props.get("http_status", None) in codes:
            logging.info(f"Enqueuing actor job {label}")
            total += await recover_actor(dispatcher, G, label, props, seed_actor_ids)
        else:
            for direction in ["followers", "following"]:
                if props.get(f"{direction}_http_status", None) in codes:
                    logging.info(f"Enqueuing collection job for {direction} of{label}")
                    total += await recover_collection(
                        dispatcher, label, props, direction
                    )
                elif props.get(f"{direction}_last_page_http_status", None) in codes:
                    logging.info(f"Enqueuing collection job for {direction} of{label}")
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

    return 1
