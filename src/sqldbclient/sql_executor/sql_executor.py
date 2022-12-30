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
from sqldbclient.sql_query_preparator.incorrect_sql_query_exception import IncorrectSqlQueryException

logger = logging.getLogger(__name__)


@class_logifier(methods=['execute'])
class SqlExecutor(SqlTransactionManager, SqlQueryPreparator, SqlHistoryManager):
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
            max_rows_read: Optional[int] = None,
            outside_transaction: bool = False
    ) -> Tuple[Optional[pd.DataFrame], ExecutedSqlQuery]:
        connection = super()._get_connection(outside_transaction=outside_transaction)
        prepared_sql_query = super().prepare(query, max_rows_read)

        start_time = datetime.now()
        result = None
        if prepared_sql_query.query_type == 'SELECT':
            if prepared_sql_query.nstatements > 1:
                raise IncorrectSqlQueryException('Use one statement for SELECT')
            cursor_result = connection.execute(prepared_sql_query.text_sa_clause)
            result = cursor_result_to_df(cursor_result)
        else:
            connection.execute(prepared_sql_query.text_sa_clause)
        finish_time = datetime.now()

        if not super()._is_in_transaction:
            connection.close()

        executed_query = ExecutedSqlQuery(
            query=prepared_sql_query.text,
            start_time=start_time,
            finish_time=finish_time
        )
        return result, executed_query

    def execute(self, query: Union[TextClause, str],
                max_rows_read: Optional[int] = None,
                outside_transaction: bool = False) -> Optional[pd.DataFrame]:
        if isinstance(query, TextClause):
            query = query.text
        result, executed_query = self._do_query_execution(query, max_rows_read, outside_transaction)

        logger.warning(f'Executed {executed_query}')
        super().dump(executed_query, result)
        return result

    @deprecated
    def read_query(self, query: Union[TextClause, str]) -> Optional[pd.DataFrame]:
        return self.execute(query)

    @deprecated
    def execute_query(self, query: Union[TextClause, str], outside_transaction: bool = False) -> Optional[pd.DataFrame]:
        return self.execute(query, outside_transaction)
