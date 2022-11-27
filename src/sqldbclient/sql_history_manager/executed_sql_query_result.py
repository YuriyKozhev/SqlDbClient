import io
import json
from dataclasses import field, dataclass
from typing import List

import pandas as pd
from sqlalchemy import Column, Table
from sqlalchemy import TypeDecorator, String

from .config import mapper_registry, EXECUTED_SQL_QUERY_RESULT_TABLE_NAME
from ..utils import parse_dates


class DataFrame(TypeDecorator):
    impl = String

    def process_literal_param(self, value, dialect):
        return value.to_csv(sep='\x1F', index=False)

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        buffer = io.StringIO(value)
        result = pd.read_csv(buffer, sep='\x1F')  # noqa
        result = parse_dates(result)
        return result


class DataTypes(TypeDecorator):
    impl = String

    def process_literal_param(self, value, dialect):
        return json.dumps(value, default=str)

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        return json.loads(value)


@mapper_registry.mapped
@dataclass
class ExecutedSqlQueryResult:
    __table__ = Table(
        EXECUTED_SQL_QUERY_RESULT_TABLE_NAME,
        mapper_registry.metadata,
        Column('uuid', String, primary_key=True),
        Column('dataframe', DataFrame),
        Column('datatypes', DataTypes),
        extend_existing=True,
    )

    uuid: str
    dataframe: pd.DataFrame = field(repr=False)
    datatypes: List[str] = field(init=False)

    def __post_init__(self):
        self.datatypes = [d.name for d in self.dataframe.dtypes]
