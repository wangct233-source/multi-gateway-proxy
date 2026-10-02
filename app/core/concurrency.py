from __future__ import annotations

import asyncio
import contextlib
from collections import deque


class AdmissionError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class GatewayGate:
    def __init__(self, limit: int, queue_limit: int, timeout: float):
        self.limit = limit
        self.queue_limit = queue_limit
        self.timeout = timeout
        self.active = 0
        self.waiters = deque()
        self.lock = asyncio.Lock()

    @property
    def queued(self):
        return sum(not waiter.done() for waiter in self.waiters)

    async def acquire(self):
        async with self.lock:
            if self.active < self.limit and not self.waiters:
                self.active += 1
                return
            if self.queued >= self.queue_limit:
                raise AdmissionError("gateway_queue_full")
            waiter = asyncio.get_running_loop().create_future()
            self.waiters.append(waiter)
        try:
            await asyncio.wait_for(asyncio.shield(waiter), self.timeout)
        except BaseException as exc:
            async with self.lock:
                if waiter in self.waiters:
                    self.waiters.remove(waiter)
                elif waiter.done() and not waiter.cancelled() and waiter.result():
                    self.active -= 1
                    self._wake()
            if isinstance(exc, TimeoutError):
                raise AdmissionError("gateway_queue_timeout") from None
            raise

    def _wake(self):
        while self.waiters and self.active < self.limit:
            waiter = self.waiters.popleft()
            if not waiter.done():
                self.active += 1
                waiter.set_result(True)

    async def release(self):
        async with self.lock:
            if self.active <= 0:
                raise RuntimeError("Gateway slot released twice")
            self.active -= 1
            self._wake()

    async def resize(self, limit: int, queue_limit: int, timeout: float):
        async with self.lock:
            self.limit, self.queue_limit, self.timeout = limit, queue_limit, timeout
            self._wake()

    @contextlib.asynccontextmanager
    async def slot(self):
        await self.acquire()
        try:
            yield
        finally:
            await self.release()
