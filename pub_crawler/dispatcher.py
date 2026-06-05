class Dispatcher:
  def __init__(self, queue):
    self.queue = queue
    self.handlers = dict()

  def set_handler(self, job_type, handler):
    self.handlers[job_type] = handler

  async def dispatch(self, job):
    handler = self._get_handler(job)
    await handler.handle(job)

  async def enqueue(self, job):
    handler = self._get_handler(job)
    job['next_available'] = handler.next_available(job)
    await self.queue.put(job)

  def _get_handler(self, job):
    handler = self.handlers.get(job['job_type'], None)
    if not handler:
      raise Exception(f"No handler for type {job['job_type']}")
    return handler
