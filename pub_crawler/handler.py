class Handler:
  def __init__(self, queue):
    self.queue = queue
    pass

  async def handle(self, job):
    pass

  async def enqueue(self, job):
    return await self.queue.put(job)
