import logging
import asyncio
from pub_crawler.dispatcher import MAX_INFLIGHT
from pub_crawler.job_id import job_id
import random

BASE_SLEEP = 100
MAX_FAILURES = 20


def _sleep_ms(ms):
    return asyncio.sleep(ms / 1000)


async def worker(name, dispatcher, *, rand=random.random, sleep=_sleep_ms):
    dispatcher_failures = 0
    while True:
        try:
            job = await dispatcher.get()
            dispatcher_failures = 0
            if job is None:
                logging.info(f"{name} got None job; quitting")
                break
            try:
                logging.info(f"{name} dispatching job {job_id(job)}")
                await dispatcher.dispatch(job)
            except Exception as e:
                logging.warning(f"{name} job {job_id(job)} failed: {e}")
                await dispatcher.fail(job)
                continue
            await dispatcher.done(job)
        except Exception as e:
            logging.error(f"{name} got dispatcher error {e}")
            dispatcher_failures += 1
            if dispatcher_failures > MAX_FAILURES:
                logging.error(f"{name} too many dispatcher failures, quitting")
                return
            else:
                sleep_time = (
                    BASE_SLEEP * (2 ** (dispatcher_failures - 1)) * (0.5 + rand())
                )
                logging.warning(
                    f"{name} dispatcher failure #{dispatcher_failures}, sleeping {sleep_time}ms"
                )
                await sleep(sleep_time)
                logging.info(f"{name} awake again after {sleep_time}ms")


async def reap(dispatcher):
    for job in await dispatcher.expired():
        await dispatcher.enqueue(job)


async def reap_worker(dispatcher, sleep):
    while True:
        try:
            await reap(dispatcher)
        except Exception as e:
            logging.warning(e, exc_info=True)
        await sleep(MAX_INFLIGHT)


class Crawler:
    def __init__(self, dispatcher, max_workers, *, sleep=_sleep_ms):
        self.dispatcher = dispatcher
        self.max_workers = max_workers
        self.sleep = sleep
        self._workers = []
        self._reaper = None

    async def start(self):
        self._workers = []
        for i in range(self.max_workers):
            self._workers.append(
                asyncio.create_task(worker(f"wfw-{i}", self.dispatcher))
            )
        self._reaper = asyncio.create_task(reap_worker(self.dispatcher, self.sleep))

    async def finish(self):
        await self.dispatcher.join()

        for w in self._workers:
            w.cancel()

        await asyncio.gather(*self._workers, return_exceptions=True)

        if self._reaper is not None:
            self._reaper.cancel()

            await asyncio.gather(self._reaper, return_exceptions=True)

    async def abort(self):
        self.dispatcher.stop()
        await asyncio.gather(*self._workers, return_exceptions=True)
        if self._reaper is not None:
            self._reaper.cancel()
            await asyncio.gather(self._reaper, return_exceptions=True)
