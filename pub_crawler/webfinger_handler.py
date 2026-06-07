from pub_crawler.handler import Handler


class WebfingerHandler(Handler):

    def __init__(self, client, dispatcher, graph):
        super().__init__(dispatcher)
        self.client = client
        self.graph = graph

    async def handle(self, job):
        wf = job["webfinger"]
        actor_id = await self.client.get_actor_id(wf)
        await self.graph.ensure_node(actor_id)
        last_fetch_date = await self.graph.get_node_property(
            actor_id, "last_fetch_date"
        )
        if not last_fetch_date:
            await self.dispatcher.enqueue(
                {"job_type": "actor", "actor_id": actor_id, "depth": 0}
            )

    def next_available(self, job):
        return self.client.next_available(job["webfinger"])
