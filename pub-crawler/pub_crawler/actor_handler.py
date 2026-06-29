from pub_crawler.handler import Handler
from datetime import datetime, timezone
from urllib.parse import urlparse
import httpx
import orjson


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
        props = {}
        headers = {}
        doc = {}
        if last_fetch_date:
            return
        try:
            doc, headers = await self.client.get_with_headers(actor_id)
        except httpx.HTTPStatusError as err:
            props["http_status"] = err.response.status_code

        if "http_status" not in props:
            props["http_status"] = 200
            props["last_fetch_date"] = datetime.now(timezone.utc).isoformat()
            props["depth"] = depth
            props["hostname"] = urlparse(actor_id).hostname
            if "server" in headers:
                props["server"] = headers["server"]
            if "preferredUsername" in doc and isinstance(doc["preferredUsername"], str):
                props["preferred_username"] = doc["preferredUsername"]
            if "published" in doc and isinstance(doc["published"], str):
                props["published"] = doc["published"]
            if "type" in doc and isinstance(doc["type"], str):
                props["type"] = doc["type"]
            if "indexable" in doc and isinstance(doc["indexable"], bool):
                props["indexable"] = doc["indexable"]
            if "discoverable" in doc and isinstance(doc["discoverable"], bool):
                props["discoverable"] = doc["discoverable"]
            if "suspended" in doc and isinstance(doc["suspended"], bool):
                props["suspended"] = doc["suspended"]
            if "memorial" in doc and isinstance(doc["memorial"], bool):
                props["memorial"] = doc["memorial"]
            if "movedTo" in doc and isinstance(doc["movedTo"], str):
                props["moved_to"] = doc["movedTo"]
            if "url" in doc and isinstance(doc["url"], str):
                props["url"] = doc["url"]
            await self._set_also_known_as(props, doc)
            if doc.get("discoverable") is True:
                if "name" in doc and isinstance(doc["name"], str):
                    props["name"] = doc["name"]
                if "summary" in doc and isinstance(doc["summary"], str):
                    props["summary"] = doc["summary"]
                await self._set_image_prop(props, doc, "image")
                await self._set_image_prop(props, doc, "icon")
                await self._set_other_props(props, doc)
            if doc.get("indexable") is True:
                if "outbox" in doc and isinstance(doc["outbox"], str):
                    props["outbox"] = doc["outbox"]
            if "followers" in doc and isinstance(doc["followers"], str):
                props["followers"] = doc["followers"]
                job = {
                    "job_type": "collection",
                    "collection_id": doc["followers"],
                    "owner_id": actor_id,
                    "direction": "followers",
                    "depth": depth,
                }
                if not await self.dispatcher.seen(job):
                    await self.dispatcher.enqueue(job)
            if "following" in doc and isinstance(doc["following"], str):
                props["following"] = doc["following"]
                job = {
                    "job_type": "collection",
                    "collection_id": doc["following"],
                    "owner_id": actor_id,
                    "direction": "following",
                    "depth": depth,
                }
                if not await self.dispatcher.seen(job):
                    await self.dispatcher.enqueue(job)

        await self.graph.set_node_properties(actor_id, props)

    def next_available(self, job):
        return self.client.next_available(job["actor_id"])

    async def _set_image_prop(self, props, doc, prop):
        value = doc.get(prop)
        if (
            value
            and isinstance(value, dict)
            and value.get("type") == "Image"
            and isinstance(value.get("url"), str)
        ):
            props[prop] = value.get("url")

    async def _set_other_props(self, props, doc):
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
            props["properties"] = orjson.dumps(other_props).decode()

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

    async def _set_also_known_as(self, props, doc):
        akas = []
        aka = doc.get("alsoKnownAs", None)
        if isinstance(aka, str):
            akas = [aka]
        elif isinstance(aka, list):
            for uri in aka:
                if isinstance(uri, str):
                    akas.append(uri)
        if len(akas) > 0:
            props["also_known_as"] = orjson.dumps(akas).decode()
