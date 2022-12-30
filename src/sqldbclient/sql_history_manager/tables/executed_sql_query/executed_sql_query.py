import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import sqlparse
from sqlalchemy import String, DateTime, Interval
from sqlalchemy import Table, Column

from sqldbclient.sql_history_manager.orm_config import metadata, orm_map, EXECUTED_SQL_QUERY_TABLE_NAME


executed_sql_query = Table(
    EXECUTED_SQL_QUERY_TABLE_NAME,
    metadata,
    Column('uuid', String, primary_key=True),
    Column('query', String),
    Column('start_time', DateTime),
    Column('finish_time', DateTime),
    Column('duration', Interval),
    Column('query_type', String),
    Column('query_shortened', String),
    extend_existing=True,
)


@dataclass
class ExecutedSqlQuery:
    uuid: str = field(init=False)
    query: str = field(repr=False)
    start_time: datetime
    finish_time: datetime
    duration: timedelta = field(init=False)
    query_type: str = field(init=False)
    query_shortened: str = field(init=False)

    def __post_init__(self):
        self.duration = self.finish_time.replace(microsecond=self.start_time.microsecond) - self.start_time
        self.uuid = uuid.uuid4().hex
        self.query_type = sqlparse.parse(self.query)[0].get_type()
        self.query_shortened = self.query
        if len(self.query_shortened) > 20:
            self.query_shortened = f'{self.query[:10]} ... {self.query[-10:]}'


orm_map(ExecutedSqlQuery, executed_sql_query)
