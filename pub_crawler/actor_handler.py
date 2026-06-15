from pub_crawler.handler import Handler
from datetime import datetime, timezone
from urllib.parse import urlparse
import httpx
import json


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
            doc, headers = await self.client.get_with_headers(actor_id)
        except httpx.HTTPStatusError as err:
            await self.graph.set_node_property(
                actor_id, "http_status", err.response.status_code
            )
            return
        await self.graph.set_node_property(actor_id, "http_status", 200)
        await self.graph.set_node_property(
            actor_id, "last_fetch_date", datetime.now(timezone.utc).isoformat()
        )
        await self.graph.set_node_property(actor_id, "depth", depth)
        await self.graph.set_node_property(
            actor_id, "hostname", urlparse(actor_id).hostname
        )
        await self._set_prop(actor_id, doc, "preferredUsername", "preferred_username")
        await self._set_prop(actor_id, doc, "name")
        await self._set_prop(actor_id, doc, "summary")
        await self._set_prop(actor_id, doc, "published")
        await self._set_prop(actor_id, doc, "type")
        await self._set_prop(actor_id, doc, "indexable")
        await self._set_prop(actor_id, doc, "discoverable")
        await self._set_prop(actor_id, doc, "suspended")
        await self._set_prop(actor_id, doc, "memorial")
        await self._set_prop(actor_id, doc, "outbox")
        await self._set_prop(actor_id, doc, "movedTo", "moved_to")
        await self._set_prop(actor_id, doc, "url")
        await self._set_image_prop(actor_id, doc, "image")
        await self._set_image_prop(actor_id, doc, "icon")
        await self._set_also_known_as(actor_id, doc)
        await self._set_other_props(actor_id, doc)
        await self._set_prop(actor_id, headers, "server")
        followers = doc.get("followers", None)
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
        following = doc.get("following", None)
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

    async def _set_prop(self, actor_id, doc, as2_prop, gml_prop=None):
        if gml_prop is None:
            gml_prop = as2_prop
        if as2_prop in doc and self._is_scalar(doc.get(as2_prop)):
            await self.graph.set_node_property(actor_id, gml_prop, doc.get(as2_prop))

    async def _set_image_prop(self, actor_id, doc, prop):
        value = doc.get(prop)
        if (
            value
            and isinstance(value, dict)
            and value.get("type") == "Image"
            and isinstance(value.get("url"), str)
        ):
            await self.graph.set_node_property(actor_id, prop, value.get("url"))

    async def _set_other_props(self, actor_id, doc):
        other_props = []
        attachment = doc.get("attachment", None)
        if isinstance(attachment, dict):
            op = self._get_other_prop(attachment)
            if op is not None:
                other_props.append(op)
        elif isinstance(attachment, list):
            for att in attachment:
                op = self._get_other_prop(att)
                if op is not None:
                    other_props.append(op)
        if len(other_props) > 0:
            await self.graph.set_node_property(
                actor_id, "properties", json.dumps(other_props)
            )

    def _get_other_prop(self, obj):
        if (
            isinstance(obj, dict)
            and obj.get("type") == "PropertyValue"
            and "name" in obj
            and "value" in obj
        ):
            return [obj.get("name"), obj.get("value")]
        else:
            return None

    async def _set_also_known_as(self, actor_id, doc):
        akas = []
        aka = doc.get("alsoKnownAs", None)
        if isinstance(aka, str):
            akas = [aka]
        elif isinstance(aka, list):
            for uri in aka:
                if isinstance(uri, str):
                    akas.append(uri)
        if len(akas) > 0:
            await self.graph.set_node_property(
                actor_id, "also_known_as", json.dumps(akas)
            )

    def _is_scalar(self, value):
        return value is not None and not isinstance(value, (dict, list))
