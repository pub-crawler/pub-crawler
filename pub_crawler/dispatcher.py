import time
import json
import asyncio
from datetime import datetime, timezone


def _epoch_ms():
    return time.time() * 1000


def _sleep_ms(ms):
    return asyncio.sleep(ms / 1000)


def iso_utc(ms):
    return (
        datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


QUEUE = "pub_crawler:queue"
INFLIGHT = "pub_crawler:inflight"
FAILED = "pub_crawler:failed"

MAX_CLIENTS = 25
TIME_PER_JOB = 100
MAX_INFLIGHT = 15 * 60 * 1000


class Dispatcher:
    def __init__(self, redis, now=_epoch_ms, sleep=_sleep_ms):
        self.redis = redis
        self.now = now
        self.sleep = sleep
        self._handlers = dict()

    def set_handler(self, job_type, handler):
        self._handlers[job_type] = handler

    async def dispatch(self, job):
        handler = self._get_handler(job)
        await handler.handle(job)

    async def enqueue(self, job):
        await self._remove_inflight(job)
        handler = self._get_handler(job)
        next_available = handler.next_available(job)
        ts = iso_utc(self.now())
        member = f"{ts}|{self._job_to_str(job)}"
        await self.redis.zadd(QUEUE, {member: next_available})

    async def get(self):
        while True:
            popped = await self.redis.bzpopmin(QUEUE)
            if not popped:
                raise Exception("Empty queue")
            key, member, next_available = popped
            _, job_json = member.decode().split("|", 1)
            job = json.loads(job_json)
            if next_available > self.now():
                await self._add_inflight(job)
                return job
            handler = self._get_handler(job)
            next_available = handler.next_available(job)
            if next_available <= self.now():
                await self._add_inflight(job)
                return job
            else:
                await self.redis.zadd(QUEUE, {member: next_available})

    async def done(self, job):
        await self._remove_inflight(job)

    async def join(self):
        while True:
            job_count = await self.redis.zcard(QUEUE)
            inflight_count = await self._inflight_count()
            if job_count > 0 or inflight_count > 0:
                await self.sleep(
                    ((job_count + inflight_count) * TIME_PER_JOB) / MAX_CLIENTS
                )
            else:
                break

    async def inflight(self):
        return list(map(self._str_to_job, await self.redis.hkeys(INFLIGHT)))

    async def expired(self):
        exp = []
        now = self.now()
        for jobstr, expiryb in (await self.redis.hgetall(INFLIGHT)).items():
            expiry = float(expiryb)
            if expiry < now:
                exp.append(self._str_to_job(jobstr))
        return exp

    async def fail(self, job):
        await self._remove_inflight(job)
        await self.redis.sadd(FAILED, self._job_to_str(job))

    async def failed(self):
        async for member in self.redis.sscan_iter(FAILED):
            yield self._str_to_job(member)

    def _get_handler(self, job):
        handler = self._handlers.get(job["job_type"], None)
        if not handler:
            raise Exception(f"No handler for type {job['job_type']}")
        return handler

    async def _add_inflight(self, job):
        expected = self.now() + MAX_INFLIGHT
        await self.redis.hset(INFLIGHT, self._job_to_str(job), expected)

    async def _remove_inflight(self, job):
        await self.redis.hdel(INFLIGHT, self._job_to_str(job))

    async def _inflight_count(self):
        return await self.redis.hlen(INFLIGHT)

    def _job_to_str(self, job):
        return json.dumps(job, sort_keys=True)

    def _str_to_job(self, string):
        return json.loads(string)
