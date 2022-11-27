from typing import Any
from dataclasses import dataclass, field
from sqlalchemy import text
import asyncio


@dataclass
class AsyncTaskInstance:
    conn: Any
    query: str
    task: Any = field(init=False)

    async def start(self):
        await self.conn.__aenter__()
        await self.conn.execute(text('commit'))
        self.task = asyncio.ensure_future(self.conn.execute(text(self.query)))
        return self

    async def finish(self):
        result = await self.task
        await self.conn.close()
        await self.conn.__aexit__(None, None, None)
        self.conn = None
        self.task = None
        return result
