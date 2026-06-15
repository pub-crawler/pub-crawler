import logging
import asyncio
from pub_crawler.dispatcher import MAX_INFLIGHT


async def worker(name, dispatcher):
    while True:
        job = await dispatcher.get()
        if job is None:
            break
        try:
            logging.debug(job)
            await dispatcher.dispatch(job)
        except Exception as e:
            logging.warning(e, exc_info=True)
            await dispatcher.fail(job)
            continue
        await dispatcher.done(job)


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


def _sleep_ms(ms):
    return asyncio.sleep(ms / 1000)


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
