import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from sqlalchemy import String, DateTime, Interval
from sqlalchemy import Table, Column

from .config import mapper_registry, EXECUTED_SQL_QUERY_TABLE_NAME


@mapper_registry.mapped
@dataclass
class ExecutedSqlQuery:
    __table__ = Table(
        EXECUTED_SQL_QUERY_TABLE_NAME,
        mapper_registry.metadata,
        Column('uuid', String, primary_key=True),
        Column('query', String),
        Column('start_time', DateTime),
        Column('finish_time', DateTime),
        Column('duration', Interval),
        extend_existing=True,
    )

    uuid: str = field(init=False)
    query: str
    start_time: datetime
    finish_time: datetime
    duration: timedelta = field(init=False)

    def __post_init__(self):
        self.duration = self.finish_time.replace(microsecond=self.start_time.microsecond) - self.start_time
        self.uuid = uuid.uuid4().hex
