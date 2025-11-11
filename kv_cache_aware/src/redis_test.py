from redis import asyncio as aioredis
import asyncio

async def test():
    redis = aioredis.from_url("redis://redis:6379", decode_responses=True)
    await redis.set("foo","bar")
    print(await redis.get("foo"))
    await redis.aclose()

asyncio.run(test())
