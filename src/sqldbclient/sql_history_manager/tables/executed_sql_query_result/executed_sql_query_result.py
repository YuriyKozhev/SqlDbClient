from dataclasses import field, dataclass
from typing import List

import pandas as pd
from sqlalchemy import Column, Table
from sqlalchemy import String

from ...orm_config import metadata, orm_map, EXECUTED_SQL_QUERY_RESULT_TABLE_NAME
from .custom_sqlalchemy_types.data_types import DataTypes
from .custom_sqlalchemy_types.data_frame import DataFrame


executed_sql_query_result = Table(
        EXECUTED_SQL_QUERY_RESULT_TABLE_NAME,
        metadata,
        Column('uuid', String, primary_key=True),
        Column('dataframe', DataFrame),
        Column('datatypes', DataTypes),
        extend_existing=True,
)


@dataclass
class ExecutedSqlQueryResult:
    uuid: str
    dataframe: pd.DataFrame = field(repr=False)
    datatypes: List[str] = field(init=False)

    def __post_init__(self):
        self.datatypes = [d.name for d in self.dataframe.dtypes]


orm_map(ExecutedSqlQueryResult, executed_sql_query_result)
