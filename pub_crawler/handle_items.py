async def handle_items(graph, dispatcher, items, owner_id, direction, depth):
    ids = []
    for item in items:
        if isinstance(item, str) and item:
            ids.append(item)
        elif (
            isinstance(item, dict)
            and isinstance(item.get("id"), str)
            and item.get("id")
        ):
            ids.append(item["id"])

    await graph.ensure_nodes(ids)

    if direction == "followers":
        await graph.ensure_to_edges(ids, owner_id)
        await graph.set_to_edges_property(ids, owner_id, f"from_{direction}", True)
    elif direction == "following":
        await graph.ensure_from_edges(owner_id, ids)
        await graph.set_from_edges_property(owner_id, ids, f"from_{direction}", True)

    last_fetch_dates = await graph.get_nodes_property(ids, "last_fetch_date")

    not_fetched = ids - last_fetch_dates.keys()

    for id in not_fetched:
        job = {
            "job_type": "actor",
            "actor_id": id,
            "depth": depth + 1,
        }
        if not await dispatcher.seen(job):
            await dispatcher.enqueue(job)
