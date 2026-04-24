import asyncio
import time
from types import TracebackType
from typing import Optional


class AsyncRateLimiter:
    """
    Async context manager that enforces N operations per time window.
    Usage:
        limiter = AsyncRateLimiter(rate=2, per=1.0)
        async with limiter:
            await do_work()
    """

    def __init__(self, rate: int, per: float) -> None:
        self._rate = rate
        self._per = per
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> None:
        async with self._lock:
            now = time.monotonic()
            # purge old timestamps outside the window
            self._timestamps = [t for t in self._timestamps if now - t < self._per]

            if len(self._timestamps) >= self._rate:
                # need to wait until the oldest call leaves the window
                sleep_for = self._per - (now - self._timestamps[0])
                await asyncio.sleep(sleep_for)
                now = time.monotonic()
                # purge again after sleeping
                self._timestamps = [t for t in self._timestamps if now - t < self._per]

            self._timestamps.append(now)

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        return None
