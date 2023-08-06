import uuid
from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta
import re

import sqlparse
from sqlalchemy import String, DateTime, Interval
from sqlalchemy import Table, Column

from sqldbclient.sql_history_manager.orm_config import metadata, orm_map, EXECUTED_SQL_QUERY_TABLE_NAME

QUERY_TEXT_HALF_MAX_REPR_SIZE = 50


def shorten_query(query: str) -> str:
    query = re.sub(r'\s+', ' ', query)
    if len(query) > QUERY_TEXT_HALF_MAX_REPR_SIZE * 2:
        return f'{query[:QUERY_TEXT_HALF_MAX_REPR_SIZE]} ... {query[-QUERY_TEXT_HALF_MAX_REPR_SIZE:]}'
    return query


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
    query: str
    start_time: datetime
    finish_time: datetime
    duration: timedelta = field(init=False)
    query_type: str = field(init=False)
    query_shortened: str = field(init=False, repr=False)

    def __post_init__(self):
        self.duration = self.finish_time.replace(microsecond=self.start_time.microsecond) - self.start_time
        self.uuid = uuid.uuid4().hex
        self.query_type = sqlparse.parse(self.query)[0].get_type()
        self.query_shortened = shorten_query(self.query)

    def __repr__(self):
        fields_name_value = []
        for f in fields(self):
            if not f.repr:
                continue
            value = getattr(self, f.name)
            if f.name in ('start_time', 'finish_time'):
                value = value.replace(microsecond=0)
            if f.name == 'query':
                value = getattr(self, 'query_shortened')
            fields_name_value.append((f.name, value))
        fields_name_value_str = ", ".join(f"{name}='{value}'" for name, value in fields_name_value)
        return f"{self.__class__.__name__}({fields_name_value_str})"


orm_map(ExecutedSqlQuery, executed_sql_query)
