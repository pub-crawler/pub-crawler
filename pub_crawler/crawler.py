import logging
import asyncio


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


class Crawler:
    def __init__(self, dispatcher, max_workers):
        self.dispatcher = dispatcher
        self.max_workers = max_workers
        self._workers = []

    async def start(self):
        self._workers = []
        for i in range(self.max_workers):
            self._workers.append(
                asyncio.create_task(worker(f"wfw-{i}", self.dispatcher))
            )

    async def finish(self):
        await self.dispatcher.join()

        for w in self._workers:
            w.cancel()

        await asyncio.gather(*self._workers, return_exceptions=True)

    async def abort(self):
        self.dispatcher.stop()
        await asyncio.gather(*self._workers, return_exceptions=True)
