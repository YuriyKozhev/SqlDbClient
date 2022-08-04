from ..log_decorators import class_logifier
from .sql_async_executor import SqlAsyncExecutor
from .async_task_instance import AsyncTaskInstance

import queue
from typing import Any


@class_logifier(methods=['plan_execution', 'get_result'])
class SqlAsyncExecutionPlanner(SqlAsyncExecutor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._queue = queue.Queue()

    async def plan_execution(self, query: str) -> None:
        task = await AsyncTaskInstance(self._engine.connect(), query).start()
        self._queue.put(task)

    async def get_result(self) -> Any:
        task = self._queue.get(False)
        return await task.finish()