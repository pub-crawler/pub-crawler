from pub_crawler.handler import Handler
from pub_crawler.handle_items import handle_items
import httpx


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
        try:
            json = await self.client.get(collection_id)
        except httpx.HTTPStatusError as err:
            await self.graph.set_node_property(
                owner_id, f"{direction}_http_status", err.response.status_code
            )
            return
        await self.graph.set_node_property(owner_id, f"{direction}_http_status", 200)
        await self._set_prop(owner_id, json, f"{direction}_count", "totalItems")
        if depth < self.max_depth:
            first = json.get("first", None)
            if first:
                await self.graph.set_node_property(
                    owner_id, f"{direction}_members_shared", True
                )
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
                    await self.graph.set_node_property(
                        owner_id, f"{direction}_members_shared", True
                    )
                    await handle_items(
                        self.graph, self.dispatcher, items, owner_id, direction, depth
                    )
                else:
                    await self.graph.set_node_property(
                        owner_id, f"{direction}_members_shared", False
                    )

    def next_available(self, job):
        return self.client.next_available(job["collection_id"])

    async def _set_prop(self, owner_id, json, prop, prop2):
        if prop2 in json:
            await self.graph.set_node_property(owner_id, prop, json.get(prop2))
