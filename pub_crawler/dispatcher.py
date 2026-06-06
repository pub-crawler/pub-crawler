import time
import json
import asyncio


def _epoch_ms():
    return time.time() * 1000


def _sleep_ms(ms):
    return asyncio.sleep(ms / 1000)


QUEUE = "pub_crawler:queue"
MAX_CLIENTS = 25
TIME_PER_JOB = 100


class Dispatcher:
    def __init__(self, redis, now=_epoch_ms, sleep=_sleep_ms):
        self.redis = redis
        self.now = now
        self.sleep = sleep
        self._handlers = dict()
        self._count = -1
        self._in_flight = 0

    def set_handler(self, job_type, handler):
        self._handlers[job_type] = handler

    async def dispatch(self, job):
        handler = self._get_handler(job)
        await handler.handle(job)

    async def enqueue(self, job):
        handler = self._get_handler(job)
        next_available = handler.next_available(job)
        member = f"{self._counter()}:{json.dumps(job)}"
        await self.redis.zadd(QUEUE, {member: next_available})

    async def get(self):
        while True:
            popped = await self.redis.bzpopmin(QUEUE)
            if not popped:
                raise Exception("Empty queue")
            key, member, next_available = popped
            counter_str, job_json = member.decode().split(":", 1)
            counter = int(counter_str)
            job = json.loads(job_json)
            if next_available > self.now():
                self._in_flight += 1
                return job
            handler = self._get_handler(job)
            next_available = handler.next_available(job)
            if next_available <= self.now():
                self._in_flight += 1
                return job
            else:
                await self.redis.zadd(QUEUE, {member: next_available})

    def done(self, job):
        self._in_flight -= 1

    async def join(self):
        job_count = await self.redis.zcard(QUEUE)
        while job_count > 0 or self._in_flight > 0:
            await self.sleep(
                ((job_count + self._in_flight) * TIME_PER_JOB) / MAX_CLIENTS
            )
            job_count = await self.redis.zcard(QUEUE)

    def _get_handler(self, job):
        handler = self._handlers.get(job["job_type"], None)
        if not handler:
            raise Exception(f"No handler for type {job['job_type']}")
        return handler

    def _counter(self):
        self._count += 1
        return self._count
