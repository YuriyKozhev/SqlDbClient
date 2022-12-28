from typing import Optional, Union, List
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from .orm_config import metadata
from .tables.executed_sql_query.executed_sql_query import ExecutedSqlQuery
from sqldbclient.sql_history_manager.tables.executed_sql_query_result.parse_executed_sql_query_result \
    import parse_executed_sql_query_result
from .tables.executed_sql_query_result.executed_sql_query_result import ExecutedSqlQueryResult
from sqldbclient.utils.log_decorators import class_logifier
from sqldbclient.sql_engine_factory import sql_engine_factory


@class_logifier(methods=['dump', 'get_result'])
class SqlHistoryManager:
    def __init__(self, history_db_name: str):
        history_db_engine = sql_engine_factory.get_or_create(f'sqlite:///{history_db_name}')
        metadata.create_all(history_db_engine)
        self._history_db_session = Session(history_db_engine)
        self._cached_query_results = {}

    @property
    def history(self) -> pd.DataFrame:
        executed_sql_queries = self._history_db_session.query(ExecutedSqlQuery).all()
        return pd.DataFrame(executed_sql_queries)

    def get_result(self, uuid: str, reload: bool = False) -> pd.DataFrame:
        if not reload and uuid in self._cached_query_results:
            return self._cached_query_results[uuid]
        result = self._history_db_session.query(ExecutedSqlQueryResult).filter_by(uuid=uuid).first()
        if result is None:
            raise ValueError(f'No result found for uuid = {uuid}')
        self._history_db_session.expunge(result)
        df = parse_executed_sql_query_result(result)
        self._cached_query_results[uuid] = df
        return df

    def __getitem__(self, uuid: str) -> pd.DataFrame:
        return self.get_result(uuid)

    def dump(self, executed_query: ExecutedSqlQuery, df: Optional[pd.DataFrame] = None) -> None:
        self._history_db_session.add(executed_query)
        if df is not None:
            uuid = executed_query.uuid
            result = ExecutedSqlQueryResult(uuid=uuid, dataframe=df)
            self._history_db_session.add(result)
            self._cached_query_results[uuid] = df
        self._history_db_session.commit()

    def delete_results(self,
                       up_to_start_time: Optional[Union[datetime, str]] = None,
                       over_estimated_size: Optional[int] = None,
                       with_uuids: Optional[List[str]] = None):
        if up_to_start_time is None and over_estimated_size is None and with_uuids is None:
            raise ValueError('At least one condition should be specified')

        selected_queries = self._history_db_session.query(ExecutedSqlQueryResult.uuid).join(
            ExecutedSqlQuery,
            ExecutedSqlQueryResult.uuid == ExecutedSqlQuery.uuid
        )
        if up_to_start_time is not None:
            selected_queries = selected_queries.filter(ExecutedSqlQuery.start_time < up_to_start_time)
        if over_estimated_size is not None:
            selected_queries = selected_queries.filter(ExecutedSqlQueryResult.estimated_size > over_estimated_size)
        if with_uuids is not None:
            selected_queries = selected_queries.filter(ExecutedSqlQueryResult.uuid.in_(with_uuids))

        queries_to_delete = self._history_db_session.query(ExecutedSqlQueryResult).filter(
            ExecutedSqlQueryResult.uuid.in_(selected_queries.subquery())
        )
        queries_to_delete.delete(synchronize_session=False)
        self._history_db_session.commit()

        self._history_db_session.execute(text('VACUUM'))  # free space after deletion
