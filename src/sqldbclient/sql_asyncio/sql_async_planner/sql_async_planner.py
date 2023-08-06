import queue
from typing import Any
import asyncio

try:
    from sqlalchemy.ext.asyncio.engine import AsyncEngine
except ImportError:
    raise ImportError('Async tools requires sqlalchemy version >= 1.4')

from sqldbclient.sql_asyncio.sql_async_executor.sql_async_executor import SqlAsyncExecutor


class SqlAsyncPlanner(SqlAsyncExecutor):
    """Wrapper around SqlAsyncExecutor with builtin tasks queue,
    which is used to store their results.
    All queries in queue are immediately scheduled for execution.
    """
    def __init__(self, engine: AsyncEngine):
        super().__init__(engine)
        self._queue = queue.Queue()

    def put(self, query: str) -> None:
        """Schedules query for execution and corresponding task to queue

        :param query: query text
        """
        task = asyncio.ensure_future(super().execute(query))
        self._queue.put(task)

    async def get(self) -> Any:
        """Tries to get result of query execution from queue.

        :return: (optional) If query selects any rows then a pandas DataFrame will be returned.
        """
        task = self._queue.get(block=False)
        return await task
