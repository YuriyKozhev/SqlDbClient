import logging
from typing import Union, Optional, Tuple
from datetime import datetime
import pandas as pd

from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.elements import TextClause

from sqldbclient.utils.log_decorators import class_logifier
from sqldbclient.sql_transaction_manager.sql_transaction_manager import SqlTransactionManager
from sqldbclient.sql_history_manager.sql_history_manager import SqlHistoryManager
from sqldbclient.sql_history_manager.tables.executed_sql_query.executed_sql_query import ExecutedSqlQuery
from sqldbclient.utils.pandas.cursor_result_to_df import cursor_result_to_df
from sqldbclient.utils.deprecated import deprecated
from sqldbclient.sql_query_preparator.sql_query_preparator import SqlQueryPreparator

logger = logging.getLogger(__name__)


@class_logifier(methods=['execute'])
class SqlExecutor(SqlTransactionManager, SqlQueryPreparator, SqlHistoryManager):
    """Main class for executing SQL queries, inherits all functionalities from
    :class:`SqlTransactionManager <SqlTransactionManager>`,
    :class:`SqlQueryPreparator <SqlQueryPreparator>`,
    :class:`SqlHistoryManager <SqlHistoryManager>`.

    Including

    - query preparation and parsing, with auto adding LIMIT clause to SELECT queries

    - transaction management using context manager::

        with sql_executor:
            sql_executor.execute('DROP TABLE IF EXISTS foo')
            sql_executor.execute('CREATE TABLE foo AS SELECT 1 AS a')
            sql_executor.execute('SELECT * FROM foo')
            sql_executor.commit()

    - storing queries results and accessing them anywhere from local file-based SQLite database via UUID::

        pg_executor['ce19362a9ac54e06b3be66d5cf858932']
    """
    def __init__(self,
                 engine: Engine,
                 max_rows_read: int,
                 history_db_name: str):
        SqlTransactionManager.__init__(self, engine)
        SqlQueryPreparator.__init__(self, max_rows_read)
        SqlHistoryManager.__init__(self, history_db_name)

    def _do_query_execution(
            self,
            query: str,
            use_raw_query: bool = False,
            add_limit: bool = True,
            max_rows_read: Optional[int] = None,
            outside_transaction: bool = False,
            force_result_fetching: bool = False,
    ) -> Tuple[Optional[pd.DataFrame], ExecutedSqlQuery]:
        connection = super()._get_connection(outside_transaction=outside_transaction)

        query_to_execute = query
        query_to_save = query
        if not use_raw_query:
            prepared_sql_query = super().prepare(query, add_limit, max_rows_read)
            query_to_execute = prepared_sql_query.text_sa_clause
            query_to_save = prepared_sql_query.text

        start_time = datetime.now()
        cursor_result = connection.execute(query_to_execute)
        result = cursor_result_to_df(cursor_result, force_result_fetching)
        finish_time = datetime.now()

        if not super()._is_in_transaction:
            connection.close()

        executed_query = ExecutedSqlQuery(
            query=query_to_save,
            start_time=start_time,
            finish_time=finish_time
        )
        return result, executed_query

    def execute(
        self,
        query: Union[TextClause, str],
        use_raw_query: bool = False,
        add_limit: bool = True,
        max_rows_read: Optional[int] = None,
        outside_transaction: bool = False,
        force_result_fetching: bool = False,
        dump_execution_info: bool = True,
        dump_result: bool = True,
    ) -> Optional[pd.DataFrame]:
        """Executes a SQL statement, and when applicable,
        saves result to local database and returns it in form of pandas DataFrame.

        :param query: query text to execute in format of str or sqlalchemy TextClause.
        :param use_raw_query: If ``True``, no preparation or checking will be applied to query (including limit adding),
            that is query will be executed as is. May come in handy when query is parsed incorrectly, for some reason.
            By default, it is recommended to leave it set to ``False``.
        :param add_limit: If ``True``, tries to add limit to query statement if it doesn't exist,
            or decrease the limit value to 'max_rows_read' in case of exceeding.
        :param max_rows_read: Number of rows used to limit SELECT query.
            If not specified, the default value from SqlExecutor instance will be used.
        :param outside_transaction: If ``True``, sqlalchemy will not create separate transaction to execute query.
            It may come in handy while executing stored procedures (e. g. in PostgreSQL),
            which commit results themselves. Otherwise, InvalidTransactionTermination may be raised.
        :param force_result_fetching: If ``True``, will try to fetch rows from cursor result that is obtained
            after executing query, even when the type of query does not imply returning any rows.
        :param dump_execution_info: If ``True``, query execution info will be dumped to history database.
            If ``False``, query execution info will be logged but will not be accessible via UUID from history database.
        :param dump_result: If ``True``, query result will be dumped to history database (when query selects any rows).
            If ``False``, query result will be returned but will not be accessible via UUID from history database.
        :return: (optional) If query selects any rows then a pandas DataFrame will be returned.
        """
        if use_raw_query is True and add_limit is True:
            raise ValueError("Argument 'add_limit' should be set to False when 'use_raw_query' is set to True")
        if add_limit is False and max_rows_read is not None:
            raise ValueError("Argument 'max_rows_read' cannot be set when 'add_limit' is set to False")
        if dump_execution_info is False and dump_result is True:
            raise ValueError("Argument 'dump_result' should be set to False when 'dump_execution_info' is set to False")
        if isinstance(query, TextClause):
            query = query.text
        result, executed_query = self._do_query_execution(
            query,
            use_raw_query,
            add_limit,
            max_rows_read,
            outside_transaction,
            force_result_fetching,
        )
        logger.warning(f'Executed {executed_query}')
        if dump_execution_info and dump_result:
            super().dump(executed_query, result)
        elif dump_result:
            super().dump(executed_query)
        return result

    @deprecated
    def read_query(self, query: Union[TextClause, str]) -> Optional[pd.DataFrame]:
        """Deprecated method, use execute"""
        return self.execute(query)

    @deprecated
    def execute_query(self, query: Union[TextClause, str], outside_transaction: bool = False) -> Optional[pd.DataFrame]:
        """Deprecated method, use execute"""
        return self.execute(query, outside_transaction)
