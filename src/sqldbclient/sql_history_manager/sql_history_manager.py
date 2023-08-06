from typing import Optional, Union, List
from datetime import datetime

import pandas as pd
from sqlalchemy import text
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
    """ Class that stores queries execution information and results. SQLLite file-based database is used to save
    data. All interactions with history database is managed here.
    Execution results can be saved via :func:`~dump` method.
    Methods :func:`~get_exec_info`, :func:`~get_result`, :func:`~history` are responsible for reading data
    from history database.
    Disk storage used by database can be freed up by using :func:`~delete_results`.
    """
    def __init__(self, history_db_name: str):
        history_db_engine = sql_engine_factory.get_or_create(f'sqlite:///{history_db_name}')
        metadata.create_all(history_db_engine)
        self._history_db_session = Session(history_db_engine)
        self._cached_query_results = {}

    @property
    def history(self) -> pd.DataFrame:
        """Returns all ExecutedSqlQuery items, that is execution info for each executed query"""
        executed_sql_queries = self._history_db_session.query(ExecutedSqlQuery).all()
        return pd.DataFrame(executed_sql_queries)

    def get_result(self, uuid: str, reload: bool = False) -> pd.DataFrame:
        """Gets result from specified query run via UUID.
        Also performs caching looked up result in memory for easy access.
        If UUID is not found, ValueError is raised.

        :param uuid: UUID of executed query
        :param reload: If ``True``, cache will not be used and result will be loaded from disk.
        :return: pandas DataFrame
        """
        if not reload and uuid in self._cached_query_results:
            return self._cached_query_results[uuid]
        result = self._history_db_session.query(ExecutedSqlQueryResult).filter_by(uuid=uuid).first()
        if result is None:
            raise ValueError(f'No result found for uuid = {uuid}')
        self._history_db_session.expunge(result)
        df = parse_executed_sql_query_result(result)
        self._cached_query_results[uuid] = df
        return df

    def get_execution_info(self, uuid: str) -> ExecutedSqlQuery:
        """Loads execution information for specified query run via UUID.
        If UUID is not found, ValueError is raised.

        :param uuid: UUID of executed query
        :return: ExecutedSqlQuery item
        """
        execution_info = self._history_db_session.query(ExecutedSqlQuery).filter_by(uuid=uuid).first()
        if execution_info is None:
            raise ValueError(f'No executing info found for uuid = {uuid}')
        self._history_db_session.expunge(execution_info)
        return execution_info

    def get_exec_info(self, uuid: str) -> ExecutedSqlQuery:
        """Loads execution information for specified query run via UUID.

        :param uuid: UUID of executed query
        :return: ExecutedSqlQuery item
        """
        return self.get_execution_info(uuid)

    def __getitem__(self, uuid: str) -> pd.DataFrame:
        return self.get_result(uuid)

    def dump(self, executed_query: ExecutedSqlQuery, df: Optional[pd.DataFrame] = None) -> None:
        """Saves query execution information and result to disk.

        :param executed_query: ExecutedSqlQuery item
        :param df: (optional) result of execution in form of pandas DataFrame
        """
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
        """Frees disk memory that is used by history database.
        Parameters to consider are query execution date and time,
        result size, and specified UUIDS.
        They can be set together, but either one of them should be specified.
        Otherwise, ValueError is raised.

        :param up_to_start_time: Datetime, before which results should be deleted.
        :param over_estimated_size: Minimum size to consider for removal.
        :param with_uuids: List of concrete UUIDS, which results should be no longer stored.
        """
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
