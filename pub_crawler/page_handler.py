from pub_crawler.handler import Handler

class PageHandler(Handler):

  def __init__(self, client, queue, graph):
    super().__init__(queue)
    self.client = client
    self.graph = graph

  async def handle(self, job):
    page_id = job["page_id"]
    owner_id = job["owner_id"]
    direction = job["direction"]
    depth = job["depth"]
    if owner_id not in self.graph.nodes:
      self.graph.add_node(owner_id)
    node = self.graph.nodes[owner_id]
    json = await self.client.get(page_id)
    next = json.get("next", None)
    if next:
      await self.enqueue({
        "job_type": "page",
        "page_id": next,
        "direction": direction,
        "owner_id": owner_id,
        "depth": depth
      })
    items = json.get("items", None)
    if not items:
      items = json.get("orderedItems", None)
    if not items:
      # log this
      return
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