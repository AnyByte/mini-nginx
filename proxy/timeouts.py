import asyncio


async def wait_with_timeout(operation, timeout_ms):
    return await asyncio.wait_for(operation, timeout=timeout_ms / 1000)
