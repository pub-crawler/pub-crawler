import time
import asyncio

def _epoch_ms(): return time.time() * 1000
def _sleep_ms(ms): return asyncio.sleep(ms / 1000)

class FixedWindowCounter:
    def __init__(self, tokens, window_ms, *, now=_epoch_ms, sleep=_sleep_ms):
        self.tokens = tokens
        self.window_ms = window_ms
        self.now = now
        self.sleep = sleep
        self._bucket = dict()
        self._last = dict()

    async def acquire(self, origin):
        while True:
          if origin not in self._last:
              self._last[origin] = -1
          if self._last[origin] < self._window_start():
              self._bucket[origin] = self.tokens
          if self._bucket[origin] > 0:
              self._bucket[origin] -= 1
              self._last[origin] = self.now()
              break
          else:
              await self._sleep_till_window_reset()

    def _window_start(self):
        return (self.now() // self.window_ms) * self.window_ms

    async def _sleep_till_window_reset(self):
        return await self.sleep(
            (self._window_start() + self.window_ms) - self.now()
          )