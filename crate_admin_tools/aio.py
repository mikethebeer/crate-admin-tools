import asyncio


async def produce(q, coro, iterable):
    for i in iterable:
        task = asyncio.create_task(coro(i))
        await q.put(task)


async def consume(q):
    while True:
        # wait for a task from the producer
        task = await q.get()
        await task
        q.task_done()


async def async_run_many(coro, iterable, concurrency):
    q = asyncio.Queue(maxsize=concurrency)
    consumer = asyncio.create_task(consume(q))
    await produce(q, coro, iterable)
    # block until all items in queue got processed
    await q.join()
    consumer.cancel()


def run_many(coro, iterable, concurrency):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_run_many(coro, iterable, concurrency))
    loop.close()


def run(coro, *args):
    if args:
        gen = coro(*args)
    else:
        gen = coro()
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(gen)
