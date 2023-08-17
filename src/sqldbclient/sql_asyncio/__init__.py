"""
.. note::
   The following tools are available only with sqlalchemy version >= **1.4**
   installed, since the support for asynchronous engines
   was added in that release.

``SqlAsyncExecutor`` is a simplified version of ``SqlExecutor``,
which provides a single method to execute queries asynchronously.
It may be useful for the case when one needs to execute queries in parallel or
to schedule an execution without blocking the main program.

``SqlQueryPreparator`` is a wrapper around ``SqlAsyncExecutor``
with builtin tasks queue, which is used to store and obtain results of
asynchronous executions. All queries are immediately scheduled for execution
once they are added to the queue.
"""

from sqldbclient.sql_asyncio.sql_async_executor.sql_async_executor import SqlAsyncExecutor
from sqldbclient.sql_asyncio.sql_async_planner.sql_async_planner import SqlAsyncPlanner
