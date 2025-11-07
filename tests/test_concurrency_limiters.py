import asyncio
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from restreamsolutions.communicator import ConcurrencyLimiter, AsyncConcurrencyLimiter


def test_concurrency_limiter_enforces_limit_and_conflicting_limit_raises():
    # Use a unique key to avoid interference with other tests/sessions
    key = f"test-sync-{uuid.uuid4()}"
    limit = 3

    current = 0
    max_seen = 0
    lock = threading.Lock()

    def worker():
        nonlocal current, max_seen
        with ConcurrencyLimiter(key, limit):
            with lock:
                current += 1
                max_seen = max(max_seen, current)
            # Make the critical section long enough to create contention
            time.sleep(0.05)
            with lock:
                current -= 1

    # Run more tasks than the limit to ensure contention occurs
    total_tasks = 20
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(worker) for _ in range(total_tasks)]
        for f in as_completed(futures):
            f.result()

    # The limiter should prevent exceeding the limit
    assert max_seen <= limit
    # And re-initializing the same key with a different limit should raise
    with pytest.raises(ValueError):
        ConcurrencyLimiter(key, limit + 1)


@pytest.mark.asyncio
async def test_async_concurrency_limiter_enforces_limit_and_conflicting_limit_raises():
    key = f"test-async-{uuid.uuid4()}"
    limit = 4

    current = 0
    max_seen = 0
    lock = asyncio.Lock()

    async def aworker():
        nonlocal current, max_seen
        async with AsyncConcurrencyLimiter(key, limit):
            async with lock:
                current += 1
                max_seen = max(max_seen, current)
            # Yield control so multiple tasks overlap within the critical section
            await asyncio.sleep(0.05)
            async with lock:
                current -= 1

    total_tasks = 25
    await asyncio.gather(*(aworker() for _ in range(total_tasks)))

    assert max_seen <= limit
    with pytest.raises(ValueError):
        AsyncConcurrencyLimiter(key, limit + 1)
