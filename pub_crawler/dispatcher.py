import time


def _epoch_ms():
    return time.time() * 1000


class Dispatcher:
    def __init__(self, queue, now=_epoch_ms):
        self.queue = queue
        self.now = now
        self._handlers = dict()
        self._count = -1

    def set_handler(self, job_type, handler):
        self._handlers[job_type] = handler

    async def dispatch(self, job):
        handler = self._get_handler(job)
        await handler.handle(job)

    async def enqueue(self, job):
        handler = self._get_handler(job)
        next_available = handler.next_available(job)
        await self.queue.put((next_available, self._counter(), job))

    async def get(self):
        while True:
            next_available, counter, job = await self.queue.get()
            if next_available > self.now():
                return job
            handler = self._get_handler(job)
            next_available = handler.next_available(job)
            if next_available <= self.now():
                return job
            else:
                await self.queue.put((next_available, self._counter(), job))
                self.queue.task_done()

    def done(self, job):
        self.queue.task_done()

    async def join(self):
        await self.queue.join()

    def _get_handler(self, job):
        handler = self._handlers.get(job["job_type"], None)
        if not handler:
            raise Exception(f"No handler for type {job['job_type']}")
        return handler

    def _counter(self):
        self._count += 1
        return self._count
