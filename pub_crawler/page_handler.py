from pub_crawler.handler import Handler


class PageHandler(Handler):

    def __init__(self, client, dispatcher, graph):
        super().__init__(dispatcher)
        self.client = client
        self.graph = graph

    async def handle(self, job):
        page_id = job["page_id"]
        owner_id = job["owner_id"]
        direction = job["direction"]
        depth = job["depth"]
        await self.graph.ensure_node(owner_id)
        json = await self.client.get(page_id)
        next = json.get("next", None)
        if next:
            await self.dispatcher.enqueue(
                {
                    "job_type": "page",
                    "page_id": next,
                    "direction": direction,
                    "owner_id": owner_id,
                    "depth": depth,
                }
            )
        items = json.get("items", None)
        if not items:
            items = json.get("orderedItems", None)
        if not items:
            # log this
            return
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
            last_fetch_date = await self.graph.get_node_property(id, "last_fetch_date")
            if not last_fetch_date:
                await self.dispatcher.enqueue(
                    {"job_type": "actor", "actor_id": id, "depth": depth + 1}
                )

    def next_available(self, job):
        return self.client.next_available(job["page_id"])
