from pub_crawler.handler import Handler

class WebfingerHandler(Handler):

  def __init__(self, client, queue, graph):
    self.client = client
    self.queue = queue
    self.graph = graph

  async def handle(self, job):
    wf = job['webfinger']
    actor_id = await self.client.get_actor_id(wf)
    self.graph.add_node(actor_id)