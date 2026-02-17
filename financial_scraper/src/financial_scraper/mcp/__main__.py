"""Allow running with: python -m financial_scraper.mcp"""

import sys

# Windows asyncio policy for compatibility with aiohttp / curl-cffi
if sys.platform == "win32":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from .server import start_server

start_server()
