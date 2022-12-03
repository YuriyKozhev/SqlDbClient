from typing import Optional, List, Union
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from .orm_config import metadata
from .tables.executed_sql_query.executed_sql_query import ExecutedSqlQuery
from .tables.executed_sql_query_result.executed_sql_query_result import ExecutedSqlQueryResult
from sqldbclient.utils.log_decorators import class_logifier
from sqldbclient.utils.singleton import Singleton
from sqldbclient.utils.deprecated import deprecated


@class_logifier(methods=['dump', 'get_result', 'get_data', 'get_queries'])
class SqlHistoryManager(metaclass=Singleton):

    def __init__(self, history_db_name: str) -> None:
        self._engine = create_engine(f'sqlite:///{history_db_name}')
        self._session = Session(self._engine)

        metadata.create_all(self._engine)

        self._executed_sql_query_results = {}

    @property
    def _executed_sql_queries(self) -> List[ExecutedSqlQuery]:
        return self._session.query(ExecutedSqlQuery).all()

    @staticmethod
    def _parse_executed_sql_query_result(result: ExecutedSqlQueryResult) -> pd.DataFrame:
        df = result.dataframe
        for i, col in enumerate(df.columns):
            df[col] = df[col].astype(result.datatypes[i])
        return df

    @deprecated
    def get_data(self) -> pd.DataFrame:
        return self.get_queries()

    def get_queries(self) -> pd.DataFrame:
        return pd.DataFrame(self._executed_sql_queries)

    def get_result(self, uuid: str, reload: bool = False) -> pd.DataFrame:
        if not reload and uuid in self._executed_sql_query_results:
            return self._executed_sql_query_results[uuid]

        result = self._session.query(ExecutedSqlQueryResult).filter_by(uuid=uuid).first()

        if result is None:
            raise ValueError(f'No result found for uuid = {uuid}')

        self._session.expunge(result)
        df = self._parse_executed_sql_query_result(result)
        self._executed_sql_query_results[uuid] = df
        return df

    def dump(self, executed_query: ExecutedSqlQuery, df: Optional[pd.DataFrame] = None) -> None:
        self._session.add(executed_query)

        if df is not None:
            uuid = executed_query.uuid
            result = ExecutedSqlQueryResult(uuid=uuid, dataframe=df)
            self._session.add(result)
            self._executed_sql_query_results[uuid] = df

        self._session.commit()

    def delete_results(self,
                       up_to_start_time: Optional[Union[datetime, str]] = None,
                       over_estimated_size: Optional[int] = None,
                       with_uuids: Optional[List[str]] = None):
        if up_to_start_time is None and over_estimated_size is None and with_uuids is None:
            raise ValueError('At least one condition should be specified')

        selected_queries = self._session.query(ExecutedSqlQueryResult.uuid).join(
            ExecutedSqlQuery,
            ExecutedSqlQueryResult.uuid == ExecutedSqlQuery.uuid
        )
        if up_to_start_time is not None:
            selected_queries = selected_queries.filter(ExecutedSqlQuery.start_time < up_to_start_time)
        if over_estimated_size is not None:
            selected_queries = selected_queries.filter(ExecutedSqlQueryResult.estimated_size > over_estimated_size)
        if with_uuids is not None:
            selected_queries = selected_queries.filter(ExecutedSqlQueryResult.uuid.in_(with_uuids))

        queries_to_delete = self._session.query(ExecutedSqlQueryResult).filter(
            ExecutedSqlQueryResult.uuid.in_(selected_queries.subquery())
        )
        queries_to_delete.delete(synchronize_session=False)
        self._session.commit()

        self._session.execute(text('VACUUM'))  # free space after deletion
