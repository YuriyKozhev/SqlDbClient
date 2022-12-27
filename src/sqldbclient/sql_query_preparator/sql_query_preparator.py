from typing import Union, Optional
import re

import sqlparse
from sqlalchemy.sql.elements import TextClause

from sqldbclient.utils.log_decorators import logger
from sqldbclient.sql_query_preparator.incorrect_sql_query_exception import IncorrectSqlQueryException
from sqldbclient.sql_query_preparator.prepared_sql_query import PreparedSqlQuery


class SqlQueryPreparator:
    LIMIT_REGEX = r'LIMIT\s*(\d*)$'

    def __init__(self, limit_nrows: Optional[int]):
        self._limit_nrows = limit_nrows

    def _get_limit(self, query_text: str) -> Optional[int]:
        limit = re.findall(self.LIMIT_REGEX, query_text, flags=re.IGNORECASE)
        if not limit:
            return None
        return int(limit[0])

    def _add_limit(self, query_text: str) -> str:
        limit = self._get_limit(query_text)
        if not limit:
            logger.warning(f'SELECT query will be limited to {self._limit_nrows}')
            query_text += f' LIMIT {self._limit_nrows}'
        elif limit > self._limit_nrows:
            logger.warning(
                f'SELECT query limit will be reduced from {limit} to {self._limit_nrows}'
            )
            query_text = re.sub(
                self.LIMIT_REGEX, f' LIMIT {self._limit_nrows}', query_text, flags=re.IGNORECASE
            )
        return query_text

    def prepare(self, query: Union[TextClause, str]) -> PreparedSqlQuery:
        if isinstance(query, TextClause):
            query_text = query.text
        else:
            query_text = query

        statements = sqlparse.parse(query_text)
        if len(statements) == 0:
            raise IncorrectSqlQueryException('Empty')
        query_type = statements[-1].get_type()
        query_nstatements = len(statements)

        query_text = query_text.strip()
        query_text = sqlparse.format(query_text, reindent=True, keyword_case='upper')
        if self._limit_nrows and query_type == 'SELECT':
            query_text = self._add_limit(query_text)

        prepared_sql_query = PreparedSqlQuery(
            text=query_text,
            query_type=query_type,
            nstatements=query_nstatements
        )
        return prepared_sql_query
