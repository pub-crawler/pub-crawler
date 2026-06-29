import time
import asyncio
import math
from datetime import datetime, timezone
from pub_crawler.job_id import job_id
import orjson


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
SEEN = "pub_crawler:seen"

MAX_CLIENTS = 25
TIME_PER_JOB = 100
MAX_INFLIGHT = 15 * 60 * 1000


class Dispatcher:
    def __init__(self, redis, now=_epoch_ms, sleep=_sleep_ms):
        self.redis = redis
        self.now = now
        self.sleep = sleep
        self._handlers = dict()
        self._stopped = False

    def set_handler(self, job_type, handler):
        self._handlers[job_type] = handler

    async def dispatch(self, job):
        handler = self._get_handler(job)
        await handler.handle(job)

    async def enqueue(self, job):
        id = job_id(job)
        if id is None:
            raise Exception(f"Unidentifiable job {self._job_to_str(job)}")
        await self._remove_inflight(job)
        score = self._job_to_score(job)
        member = self._job_to_member(job)
        await self.redis.zadd(QUEUE, {member: score})
        await self.redis.sadd(SEEN, id)

    async def get(self):
        while not self._stopped:
            popped = await self.redis.bzpopmin(QUEUE)
            if not popped:
                raise Exception("Empty queue")
            _, member, score = popped
            job = self._member_to_job(member)
            next_available = self._score_to_na(score)
            if next_available > self.now():
                await self._add_inflight(job)
                return job
            handler = self._get_handler(job)
            next_available = handler.next_available(job)
            if next_available <= self.now():
                await self._add_inflight(job)
                return job
            else:
                score = self._job_to_score(job, next_available=next_available)
                await self.redis.zadd(QUEUE, {member: score})
        return None

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

    def stop(self):
        self._stopped = True

    async def seen(self, job):
        id = job_id(job)
        if id is None:
            raise Exception(f"Unidentifiable job {self._job_to_str(job)}")
        return await self.redis.sismember(SEEN, id)

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
        return orjson.dumps(job, option=orjson.OPT_SORT_KEYS).decode()

    def _str_to_job(self, string):
        return orjson.loads(string)

    def _job_to_score(self, job, next_available=None):
        if next_available is None:
            handler = self._get_handler(job)
            next_available = handler.next_available(job)
        return math.floor(next_available)

    def _score_to_na(self, score):
        return score

    def _job_to_member(self, job):
        depth = job.get("depth", 0)
        job_type = job.get("job_type")
        if job_type == "webfinger":
            job_type_code = 10
        elif job_type == "actor":
            job_type_code = 20
        elif job_type == "collection":
            job_type_code = 30
        elif job_type == "page":
            job_type_code = 40
        else:
            raise Exception(f"unrecognized job type {job_type}")
        ts = iso_utc(self.now())
        depth = max(min(depth, 99), 0)
        job_type_code = max(min(job_type_code, 99), 0)
        return f"{depth:02d}|{job_type_code:02d}|{ts}|{self._job_to_str(job)}"

    def _member_to_job(self, member):
        _, _, _, job = member.decode().split("|", 3)
        return self._str_to_job(job)
