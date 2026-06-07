from pub_crawler.handler import Handler


class CollectionHandler(Handler):

    def __init__(self, client, dispatcher, graph, max_depth):
        super().__init__(dispatcher)
        self.client = client
        self.graph = graph
        self.max_depth = max_depth

    async def handle(self, job):
        collection_id = job["collection_id"]
        owner_id = job["owner_id"]
        direction = job["direction"]
        depth = job["depth"]
        await self.graph.ensure_node(owner_id)
        json = await self.client.get(collection_id)
        await self._set_prop(owner_id, json, f"{direction}_count", "totalItems")
        if depth < self.max_depth:
            first = json.get("first", None)
            if first:
                await self.dispatcher.enqueue(
                    {
                        "job_type": "page",
                        "page_id": first,
                        "owner_id": owner_id,
                        "direction": direction,
                        "depth": depth,
                    }
                )
            else:
                items = json.get("items", json.get("orderedItems", None))
                if items:
                    for item in items:
                        if type(item) == dict:
                            id = item["id"]
                        else:
                            id = item
                        if not id:
                            # log this
                            continue
                        await self.graph.ensure_node(id)
                        if direction == "followers":
                            await self.graph.ensure_edge(id, owner_id)
                            await self.graph.set_edge_property(
                                id, owner_id, f"from_{direction}", True
                            )
                        elif direction == "following":
                            await self.graph.ensure_edge(owner_id, id)
                            await self.graph.set_edge_property(
                                owner_id, id, f"from_{direction}", True
                            )
                        last_fetch_date = await self.graph.get_node_property(
                            id, "last_fetch_date"
                        )
                        if not last_fetch_date:
                            await self.dispatcher.enqueue(
                                {
                                    "job_type": "actor",
                                    "actor_id": id,
                                    "depth": depth + 1,
                                }
                            )

    def next_available(self, job):
        return self.client.next_available(job["collection_id"])

    async def _set_prop(self, owner_id, json, prop, prop2):
        value = json.get(prop2, None)
        if value:
            await self.graph.set_node_property(owner_id, prop, value)
