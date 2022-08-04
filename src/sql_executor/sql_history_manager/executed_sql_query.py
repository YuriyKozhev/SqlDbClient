from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
from typing import Optional
import uuid
from contextlib import suppress

from sqlalchemy import Table, Column, String, DateTime, Interval, TypeDecorator
from sqlalchemy.orm import registry
import pandas as pd


class DataFrame(TypeDecorator):
    impl = String

    def process_literal_param(self, value, dialect):
        return json.dumps(value.to_dict(orient='list'), default=str)

    process_bind_param = process_literal_param

    @staticmethod
    def _parse_dataframe_date_columns(df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            if df[col].dtype != object:
                continue
            with suppress(Exception):
                df[col] = pd.to_datetime(df[col])
        return df

    def process_result_value(self, value, dialect):
        result = pd.DataFrame(json.loads(value))
        result = self._parse_dataframe_date_columns(result)
        return result


mapper_registry = registry()

EXECUTE_SQL_QUERY_TABLE_NAME = 'executed_sql_query'

@mapper_registry.mapped
@dataclass
class ExecutedSqlQuery:
    __table__ = Table(
        EXECUTE_SQL_QUERY_TABLE_NAME,
        mapper_registry.metadata,
        Column('uuid', String, primary_key=True),
        Column('query', String),
        Column('start_time', DateTime),
        Column('finish_time', DateTime),
        Column('duration', Interval),
        Column('result', DataFrame),
        extend_existing=True,
    )

    query: str
    start_time: datetime
    finish_time: datetime
    duration: timedelta = field(init=False)
    result: Optional[pd.DataFrame] = field(repr=False)
    uuid: str = field(init=False)

    def __post_init__(self):
        self.duration = self.finish_time.replace(microsecond=self.start_time.microsecond) - self.start_time
        self.uuid = uuid.uuid4().hex
