import queue
from typing import Any
import asyncio

try:
    from sqlalchemy.ext.asyncio.engine import AsyncEngine
except ImportError as e:
    raise ImportError(f'Async tools requires sqlalchemy version >= 1.4')

from sqldbclient.sql_asyncio.sql_async_executor.sql_async_executor import SqlAsyncExecutor


class SqlAsyncPlanner(SqlAsyncExecutor):
    def __init__(self, engine: AsyncEngine):
        super().__init__(engine)
        self._queue = queue.Queue()

    def put(self, query: str) -> None:
        task = asyncio.ensure_future(super().execute(query))
        self._queue.put(task)

    async def get(self) -> Any:
        task = self._queue.get(block=False)
        return await task
