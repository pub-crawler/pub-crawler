from pub_crawler.handler import Handler

class CollectionHandler(Handler):

  def __init__(self, client, queue, graph, max_depth):
    super().__init__(queue)
    self.client = client
    self.graph = graph
    self.max_depth = max_depth

  async def handle(self, job):
    collection_id = job["collection_id"]
    owner_id = job["owner_id"]
    direction = job["direction"]
    depth = job["depth"]
    if owner_id not in self.graph.nodes:
      self.graph.add_node(owner_id)
    node = self.graph.nodes[owner_id]
    json = await self.client.get(collection_id)
    self._set_prop(node, json, f"{direction}_count", "totalItems")
    if depth < self.max_depth:
      first = json.get("first", None)
      if first:
        await self.enqueue({
          "job_type": "page",
          "page_id": first,
          "owner_id": owner_id,
          "direction": direction,
          "depth": depth
        })
      else:
        items = json.get('items', json.get('orderedItems', None))
        if items:
              for item in items:
                if type(item) == dict:
                  id = item['id']
                else:
                  id = item
                if not id:
                  # log this
                  continue
                if id not in self.graph.nodes:
                  self.graph.add_node(id)
                if direction == "followers":
                  self.graph.add_edge(id, owner_id)
                elif direction == "following":
                  self.graph.add_edge(owner_id, id)
                await self.enqueue({
                  "job_type": "actor",
                  "actor_id": id,
                  "depth": depth + 1
                })

  def next_available(self, job):
    return self.client.next_available(job['collection_id'])

  def _set_prop(self, node, json, prop, prop2):
    value = json.get(prop2, None)
    if value:
      node[prop] = value