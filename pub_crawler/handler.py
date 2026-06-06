from pub_crawler.dispatcher import Dispatcher


class Handler:
    def __init__(self, dispatcher: Dispatcher):
        self.dispatcher = dispatcher
        pass

    async def handle(self, job):
        pass

    def next_available(self, job):
        return float("inf")
