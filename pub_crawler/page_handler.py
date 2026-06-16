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
        props = {}
        props[f"{direction}_last_page"] = page_id
        props[f"{direction}_pages_complete"] = False
        await self.graph.set_node_properties(owner_id, props)
        json = {}
        try:
            json = await self.client.get(page_id)
        except httpx.HTTPStatusError as err:
            props[f"{direction}_last_page_http_status"] = err.response.status_code

        if f"{direction}_last_page_http_status" not in props:
            props[f"{direction}_last_page_http_status"] = 200

            if "next" in json:
                next_id = None
                if isinstance(json.get("next"), dict) and isinstance(
                    json["next"].get("id"), str
                ):
                    next_id = json["next"].get("id")
                elif isinstance(json.get("next"), str) and json["next"]:
                    next_id = json["next"]
                if next_id:
                    job = {
                        "job_type": "page",
                        "page_id": next_id,
                        "direction": direction,
                        "owner_id": owner_id,
                        "depth": depth,
                    }
                    if not await self.dispatcher.seen(job):
                        await self.dispatcher.enqueue(job)
            else:
                props[f"{direction}_pages_complete"] = True

            items = json.get("items", json.get("orderedItems", None))

            if items:
                await handle_items(
                    self.graph, self.dispatcher, items, owner_id, direction, depth
                )

        await self.graph.set_node_properties(owner_id, props)

    def next_available(self, job):
        return self.client.next_available(job["page_id"])
