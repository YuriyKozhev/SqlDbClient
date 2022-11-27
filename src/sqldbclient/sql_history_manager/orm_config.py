from sqlalchemy import Table

try:
    from sqlalchemy.orm import registry

    mapper_registry = registry()
    metadata = mapper_registry.metadata

    def orm_map(class_: type, table: Table):
        mapper_registry.map_imperatively(class_, table)

except ImportError:
    # support for legacy sqlalchemy versions (< 1.4)

    from sqlalchemy.orm import mapper
    from sqlalchemy import MetaData

    metadata = MetaData()

    def orm_map(class_: type, table: Table):
        mapper(class_, table)


EXECUTED_SQL_QUERY_TABLE_NAME = 'executed_sql_query'
EXECUTED_SQL_QUERY_RESULT_TABLE_NAME = 'executed_sql_query_result'
