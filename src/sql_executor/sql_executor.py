from typing import Union, Optional
import re
from datetime import datetime
import sqlparse
import pandas as pd

from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.elements import TextClause
from sqlalchemy import text

from .log_decorators import class_logifier, logger
from .sql_transaction_manager import SqlTransactionManager
from .sql_history_manager.sql_history_manager import SqlHistoryManager
from .sql_history_manager.executed_sql_query import ExecutedSqlQuery
from .utils import parse_dates


@class_logifier(methods=['read_query', 'execute_query'])
class SqlExecutor(SqlTransactionManager):
    METHOD_READ = 'read'
    METHOD_EXECUTE = 'execute'
    LIMIT_REGEX = r'limit\s*(\d*)$'

    def __init__(self,
                 engine: Engine,
                 max_rows_read: int = 10_000,
                 history_db_name: str = 'sql_executor_history_v1.db'):
        super().__init__(engine)
        self._max_rows_read = max_rows_read
        self._history_manager = SqlHistoryManager(history_db_name)

    def _prepare_query(self, query: Union[TextClause, str], method: str) -> TextClause:
        if isinstance(query, TextClause):
            query = query.text

        statements = sqlparse.parse(query)
        if len(statements) != 1:
            raise ValueError(f'Use separate call for each statement of query: \n{query}')
        query_type = statements[0].get_type()
        if method == self.METHOD_READ and query_type not in ('SELECT', 'UNKNOWN'):
            raise ValueError(f'Incorrect query for method {self.METHOD_READ}: \n{query}')
        if method == self.METHOD_EXECUTE and query_type == 'SELECT':
            raise ValueError(f'Incorrect method for SELECT query: \n{query}')

        query = query.strip().replace(';', '')
        if method == self.METHOD_READ:
            limit = re.findall(self.LIMIT_REGEX, query, flags=re.IGNORECASE)
            if not limit:
                logger.warning(f'SELECT query will be limited to {self._max_rows_read}')
                query += f' LIMIT {self._max_rows_read}'
            elif int(limit[0]) > self._max_rows_read:
                logger.warning(f'SELECT query limit will be changed to {self._max_rows_read}')
                query = re.sub(self.LIMIT_REGEX, f' LIMIT {self._max_rows_read}', query, flags=re.IGNORECASE)

        query = sqlparse.format(query, reindent=True, keyword_case='upper')
        query = text(query)
        return query

    def _execute(self, query: Union[TextClause, str], method: str):
        connection = self._engine.connect()
        if self._is_in_transaction:
            connection = self._transaction.connection

        query = self._prepare_query(query, method)

        result = None
        start_time = datetime.now()
        if method == self.METHOD_READ:
            result = pd.read_sql(query, connection)
            result = parse_dates(result)
        elif method == self.METHOD_EXECUTE:
            connection.execute(query)
        else:
            raise ValueError(f'Unrecognized method "{method}"')
        finish_time = datetime.now()

        if not self._is_in_transaction:
            connection.close()

        executed_query = ExecutedSqlQuery(query=query.text, start_time=start_time,
                                          finish_time=finish_time)
        logger.warning('Executed: ' + str(executed_query))
        self._history_manager.dump(executed_query, result)

        return result

    def read_query(self, query: Union[TextClause, str]) -> pd.DataFrame:
        return self._execute(query, 'read')

    def execute_query(self, query: Union[TextClause, str], outside_transaction: bool = False) -> None:
        if outside_transaction:
            if self._is_in_transaction:
                raise ValueError('Unable to execute outside transaction when in transaction')
            query = self._prepare_query(query, 'execute')
            conn = self._engine.connect()
            conn.execute('commit')
            try:
                conn.execute(query)
            finally:
                conn.close()
            return
        return self._execute(query, 'execute')

    @property
    def history(self):
        return self._history_manager.get_data()

    @property
    def last_result(self):
        raise NotImplementedError()

    def get_result(self, uuid: str, reload: bool = False):
        return self._history_manager.get_result(uuid, reload)

    def __getitem__(self, uuid: str):
        return self.get_result(uuid)

    def peek_table(self, table_fullname: Optional[str] = None,
                   name: Optional[str] = None,
                   schema: Optional[str] = None,
                   rows_to_peek: int = 5) -> pd.DataFrame:
        if table_fullname is None:
            table_fullname = f'"{schema}"."{name}"'

        return self.read_query(f'''
            SELECT * FROM {table_fullname}
            LIMIT {rows_to_peek}
        ''')
