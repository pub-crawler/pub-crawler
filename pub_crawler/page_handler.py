from pub_crawler.handler import Handler
from pub_crawler.handle_items import handle_items

import httpx


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
        await self.graph.set_node_property(owner_id, f"{direction}_last_page", page_id)
        await self.graph.set_node_property(
            owner_id, f"{direction}_pages_complete", False
        )
        try:
            json = await self.client.get(page_id)
        except httpx.HTTPStatusError as err:
            await self.graph.set_node_property(
                owner_id, f"{direction}_last_page_http_status", err.response.status_code
            )
            return
        await self.graph.set_node_property(
            owner_id, f"{direction}_last_page_http_status", 200
        )

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
        else:
            await self.graph.set_node_property(
                owner_id, f"{direction}_pages_complete", True
            )

        items = json.get("items", None)
        if not items:
            items = json.get("orderedItems", None)
        if not items:
            # log this
            return
        await handle_items(
            self.graph, self.dispatcher, items, owner_id, direction, depth
        )

    def next_available(self, job):
        return self.client.next_available(job["page_id"])
