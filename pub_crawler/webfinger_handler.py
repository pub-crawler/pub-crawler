from pub_crawler.handler import Handler


class WebfingerHandler(Handler):

    def __init__(self, client, dispatcher, graph):
        super().__init__(dispatcher)
        self.client = client
        self.graph = graph

    async def handle(self, job):
        wf = job["webfinger"]
        actor_id = await self.client.get_actor_id(wf)
        self.graph.add_node(actor_id)
        job = {"job_type": "actor", "actor_id": actor_id, "depth": 0}
        await self.dispatcher.enqueue(job)

    def next_available(self, job):
        return self.client.next_available(job["webfinger"])
