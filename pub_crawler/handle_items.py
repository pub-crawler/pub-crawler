async def handle_items(graph, dispatcher, items, owner_id, direction, depth):
    for item in items:
        if type(item) == dict:
            id = item["id"]
        else:
            id = item
        if not id:
            # log this
            continue
        await graph.ensure_node(id)
        if direction == "followers":
            await graph.ensure_edge(id, owner_id)
            await graph.set_edge_property(id, owner_id, f"from_{direction}", True)
        elif direction == "following":
            await graph.ensure_edge(owner_id, id)
            await graph.set_edge_property(owner_id, id, f"from_{direction}", True)
        last_fetch_date = await graph.get_node_property(id, "last_fetch_date")
        if not last_fetch_date:
            await dispatcher.enqueue(
                {
                    "job_type": "actor",
                    "actor_id": id,
                    "depth": depth + 1,
                }
            )
