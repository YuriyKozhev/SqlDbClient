from sqlalchemy.orm import registry


mapper_registry = registry()
EXECUTED_SQL_QUERY_TABLE_NAME = 'executed_sql_query'
EXECUTED_SQL_QUERY_RESULT_TABLE_NAME = 'executed_sql_query_result'