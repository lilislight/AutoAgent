"""Short shared queue critical sections; SQL remains owned by the Writer loop."""

import asyncio


class Admission:
    """Exclude local producers while a slow admission or maintenance is pending."""

    def __init__(self, lock):
        self.lock = lock
        self.serial = asyncio.Lock()
        self.waiters = 0

    async def __aenter__(self):
        with self.lock:
            self.waiters += 1
        try:
            await self.serial.acquire()
        except BaseException:
            with self.lock:
                self.waiters -= 1
            raise
        return self

    async def __aexit__(self, *_):
        self.serial.release()
        with self.lock:
            self.waiters -= 1
