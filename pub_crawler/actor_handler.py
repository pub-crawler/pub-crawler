from pub_crawler.handler import Handler
from datetime import datetime, timezone


class ActorHandler(Handler):

    def __init__(self, client, dispatcher, graph):
        super().__init__(dispatcher)
        self.client = client
        self.graph = graph

    async def handle(self, job):
        actor_id = job["actor_id"]
        depth = job["depth"]
        if actor_id not in self.graph.nodes:
            self.graph.add_node(actor_id)
        node = self.graph.nodes[actor_id]
        if "last_fetch_date" in node:
            return
        json = await self.client.get(actor_id)
        node["last_fetch_date"] = datetime.now(timezone.utc).isoformat()
        self._set_prop(node, json, "preferredUsername")
        self._set_prop(node, json, "name")
        self._set_prop(node, json, "published")
        self._set_prop(node, json, "type")
        followers = json.get("followers", None)
        if followers:
            node["followers"] = followers
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
            node["following"] = following
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

    def _set_prop(self, node, json, prop):
        value = json.get(prop, None)
        if value:
            node[prop] = value
