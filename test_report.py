# test_report.py
import asyncio
import aiohttp

from dotenv import load_dotenv
load_dotenv()

from performance_tracker import send_performance_report


async def _t():
    async with aiohttp.ClientSession() as session:
        await send_performance_report(session)


if __name__ == "__main__":
    asyncio.run(_t())