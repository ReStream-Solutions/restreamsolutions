import asyncio
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from restreamsolutions.communicator import ConcurrencyLimiter, AsyncConcurrencyLimiter


def test_concurrency_limiter_enforces_limit_and_conflicting_limit_raises():
    """ConcurrencyLimiter keeps threads within the limit, rejects a conflicting limit and frees its semaphores."""
    client_key = str(uuid.uuid4())
    url_key_template = "http://example.com/{id}"
    limit = 3

    current = 0
    max_seen = 0
    lock = threading.Lock()

    def worker():
        """Hold a limiter slot for a while and record the highest number of concurrent holders in max_seen."""
        nonlocal current, max_seen
        url_key = url_key_template.format(id=random.randint(1, 1000))
        with ConcurrencyLimiter(url_key, client_key, limit):
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
        with ConcurrencyLimiter(url_key_template.format(id=1001), client_key, limit):
            with ConcurrencyLimiter(url_key_template.format(id=1002), client_key, limit + 1):
                pass

    # All semaphores were deleted
    assert ConcurrencyLimiter._semaphores == {}


@pytest.mark.asyncio
async def test_async_concurrency_limiter_enforces_limit_and_conflicting_limit_raises():
    """AsyncConcurrencyLimiter keeps tasks within the limit, rejects a conflicting limit and frees its semaphores."""
    client_key = str(uuid.uuid4())
    url_key_template = "http://example.com/{id}"
    limit = 3

    current = 0
    max_seen = 0
    lock = asyncio.Lock()

    async def aworker():
        """Hold an async limiter slot for a while and record the highest number of concurrent holders in max_seen."""
        nonlocal current, max_seen
        url_key = url_key_template.format(id=random.randint(1, 1000))
        async with AsyncConcurrencyLimiter(url_key, client_key, limit):
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
        async with AsyncConcurrencyLimiter(url_key_template.format(id=1001), client_key, limit):
            async with AsyncConcurrencyLimiter(url_key_template.format(id=1002), client_key, limit + 1):
                pass

    # All semaphores were deleted
    assert AsyncConcurrencyLimiter._semaphores == {}


def test_limit_one_keeps_the_semaphore_while_someone_waits():
    """A newcomer must wait for the current holder instead of getting a fresh semaphore of its own."""
    url, client_key = 'http://example.com/token/', str(uuid.uuid4())
    first = ConcurrencyLimiter(url, client_key, 1)
    first.__enter__()
    second_inside, release_second, third_inside = threading.Event(), threading.Event(), threading.Event()

    def second():
        """Hold the slot after the first caller until the test releases it."""
        with ConcurrencyLimiter(url, client_key, 1):
            second_inside.set()
            release_second.wait(5)

    def third():
        """Take the slot after the second caller."""
        with ConcurrencyLimiter(url, client_key, 1):
            third_inside.set()

    second_thread = threading.Thread(target=second)
    second_thread.start()
    deadline = time.monotonic() + 5
    while ConcurrencyLimiter._users.get(first._key) != 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    first.__exit__(None, None, None)
    assert second_inside.wait(5)

    third_thread = threading.Thread(target=third)
    third_thread.start()
    assert not third_inside.wait(0.2)
    release_second.set()
    assert third_inside.wait(5)
    second_thread.join()
    third_thread.join()
    assert first._key not in ConcurrencyLimiter._semaphores
    assert first._key not in ConcurrencyLimiter._users


@pytest.mark.asyncio
async def test_async_limit_one_keeps_the_semaphore_while_someone_waits():
    """The async limiter keeps the semaphore for waiters and forgets a cancelled waiter."""
    url, client_key = 'http://example.com/token/', str(uuid.uuid4())
    first = AsyncConcurrencyLimiter(url, client_key, 1)
    await first.__aenter__()

    # A cancelled waiter does not stay registered
    cancelled = asyncio.create_task(AsyncConcurrencyLimiter(url, client_key, 1).__aenter__())
    await asyncio.sleep(0)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    assert AsyncConcurrencyLimiter._users[first._key] == 1

    second_inside, release_second, third_inside = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def second():
        """Hold the slot after the first caller until the test releases it."""
        async with AsyncConcurrencyLimiter(url, client_key, 1):
            second_inside.set()
            await release_second.wait()

    async def third():
        """Take the slot after the second caller."""
        async with AsyncConcurrencyLimiter(url, client_key, 1):
            third_inside.set()

    second_task = asyncio.create_task(second())
    await asyncio.sleep(0)
    assert AsyncConcurrencyLimiter._users[first._key] == 2
    await first.__aexit__(None, None, None)
    await asyncio.wait_for(second_inside.wait(), 5)

    third_task = asyncio.create_task(third())
    await asyncio.sleep(0.05)
    assert not third_inside.is_set()
    release_second.set()
    await asyncio.wait_for(third_task, 5)
    await second_task
    assert first._key not in AsyncConcurrencyLimiter._semaphores
    assert first._key not in AsyncConcurrencyLimiter._users
