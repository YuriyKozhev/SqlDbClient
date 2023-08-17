"""
``SqlQueryPreparator``
   - validates that there is exactly 1 statement in a query which is being executed
   - determines query type
   - formats query
   - automatically adds LIMIT clause to query

"""

from sqldbclient.sql_query_preparator.sql_query_preparator import SqlQueryPreparator
