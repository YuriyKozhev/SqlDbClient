"""
Main class, ``SqlExecutor``, inherits all functionalities from ``SqlHistoryManager``,
``SqlQueryPreparator`` and ``SqlTransactionManager``:

- ``SqlHistoryManager``
   - stores information about query executions and their results in local SQLite database
   - provides easy access to saved data via UUID
   - performs database cleaning to keep its size limited
- ``SqlQueryPreparator``
   - validates that there is exactly 1 statement in a query which is being executed
   - determines query type
   - formats query
   - automatically adds LIMIT clause to query
- ``SqlTransactionManager``
   - provides context manager for performing transactions

Moreover, ``SqlExecutor`` keeps configuration
(sqlalchemy engine parameters, default LIMIT clause value, file name for history database)
and provides single method for executing SQL queries.

"""

from sqldbclient.sql_executor.sql_executor_config.sql_executor_config import SqlExecutorConf
from sqldbclient.sql_executor.sql_executor import SqlExecutor
from sqldbclient.sql_executor.sql_executor_builder.sql_executor_builder import SqlExecutorBuilder

sql_executor_builder = SqlExecutorBuilder()
SqlExecutor.builder = sql_executor_builder
