from pub_crawler.handler import Handler
from datetime import datetime, timezone
from urllib.parse import urlparse
import httpx

class ActorHandler(Handler):

    def __init__(self, client, dispatcher, graph):
        super().__init__(dispatcher)
        self.client = client
        self.graph = graph

    async def handle(self, job):
        actor_id = job["actor_id"]
        depth = job["depth"]
        await self.graph.ensure_node(actor_id)
        last_fetch_date = await self.graph.get_node_property(
            actor_id, "last_fetch_date"
        )
        if last_fetch_date:
            return
        try:
            json, headers = await self.client.get_with_headers(actor_id)
        except httpx.HTTPStatusError as err:
            await self.graph.set_node_property(
                actor_id, "http_status", err.response.status_code
            )
            return
        await self.graph.set_node_property(
            actor_id, "http_status", 200
        )
        await self.graph.set_node_property(
            actor_id, "last_fetch_date", datetime.now(timezone.utc).isoformat()
        )
        await self.graph.set_node_property(actor_id, "depth", depth)
        await self.graph.set_node_property(
            actor_id, "hostname", urlparse(actor_id).hostname
        )
        await self._set_prop(actor_id, json, "preferredUsername")
        await self._set_prop(actor_id, json, "name")
        await self._set_prop(actor_id, json, "published")
        await self._set_prop(actor_id, json, "type")
        await self._set_prop(actor_id, json, "indexable")
        await self._set_prop(actor_id, json, "discoverable")
        await self._set_prop(actor_id, headers, "server")
        followers = json.get("followers", None)
        if followers:
            await self.graph.set_node_property(actor_id, "followers", followers)
            await self.dispatcher.enqueue(
                {
                    "job_type": "collection",
                    "collection_id": followers,
                    "owner_id": actor_id,
                    "direction": "followers",
                    "depth": depth,
                }
            )
        following = json.get("following", None)
        if following:
            await self.graph.set_node_property(actor_id, "following", following)
            await self.dispatcher.enqueue(
                {
                    "job_type": "collection",
                    "collection_id": following,
                    "owner_id": actor_id,
                    "direction": "following",
                    "depth": depth,
                }
            )

    def next_available(self, job):
        return self.client.next_available(job["actor_id"])

    async def _set_prop(self, actor_id, json, prop):
        if prop in json:
            await self.graph.set_node_property(actor_id, prop, json.get(prop))
