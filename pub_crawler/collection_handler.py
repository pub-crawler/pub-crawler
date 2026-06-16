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
        props = {}
        json = {}
        try:
            json = await self.client.get(collection_id)
        except httpx.HTTPStatusError as err:
            props[f"{direction}_http_status"] = err.response.status_code

        if f"{direction}_http_status" not in props:
            props[f"{direction}_http_status"] = 200
            if "totalItems" in json and isinstance(json["totalItems"], int):
                props[f"{direction}_count"] = json["totalItems"]

            if depth < self.max_depth:
                if "first" in json:
                    first_id = None
                    if isinstance(json.get("first"), dict) and isinstance(
                        json["first"].get("id"), str
                    ):
                        first_id = json["first"].get("id")
                    elif isinstance(json.get("first"), str) and json["first"]:
                        first_id = json["first"]
                    if first_id:
                        props[f"{direction}_members_shared"] = True
                        job = {
                            "job_type": "page",
                            "page_id": first_id,
                            "owner_id": owner_id,
                            "direction": direction,
                            "depth": depth,
                        }
                        if not await self.dispatcher.seen(job):
                            await self.dispatcher.enqueue(job)
                else:
                    items = json.get("items", json.get("orderedItems", None))
                    if items:
                        props[f"{direction}_members_shared"] = True
                        await handle_items(
                            self.graph,
                            self.dispatcher,
                            items,
                            owner_id,
                            direction,
                            depth,
                        )
                    else:
                        props[f"{direction}_members_shared"] = False

        await self.graph.set_node_properties(owner_id, props)

    def next_available(self, job):
        return self.client.next_available(job["collection_id"])
