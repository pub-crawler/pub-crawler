"""Tests for the base Handler — shared behaviour the concrete handlers inherit.

`enqueue(job)` is the single choke point for putting work on the queue. Right
now it's a thin wrapper over `self.queue.put`; the NEXT step stamps each job
with its host's `next_available` time (for the priority queue), and this is the
one place that hook will live.

Contract:
  Handler(queue); async Handler.enqueue(job) -> await self.queue.put(job)
  (concrete handlers pass the queue up via super().__init__(queue).)
"""

import asyncio

from pub_crawler.handler import Handler


async def test_enqueue_puts_the_job_on_the_queue():
    queue = asyncio.Queue()
    handler = Handler(queue)

    job = {"job_type": "actor", "actor_id": "https://x.example/users/a", "depth": 1}
    await handler.enqueue(job)

    assert queue.get_nowait() == job
