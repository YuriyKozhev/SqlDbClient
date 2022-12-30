import logging
from typing import Optional
import re

import sqlparse

from sqldbclient.sql_query_preparator.incorrect_sql_query_exception import IncorrectSqlQueryException
from sqldbclient.sql_query_preparator.prepared_sql_query import PreparedSqlQuery

logger = logging.getLogger(__name__)


class SqlQueryPreparator:
    LIMIT_REGEX = r'LIMIT\s*(\d*)$'

    def __init__(self, limit_nrows: int):
        self._limit_nrows = limit_nrows

    def _get_limit(self, query_text: str) -> Optional[int]:
        limit = re.findall(self.LIMIT_REGEX, query_text, flags=re.IGNORECASE)
        if not limit:
            return None
        return int(limit[0])

    def _add_limit(self, query_text: str, limit_nrows: Optional[int] = None) -> str:
        if not limit_nrows:
            limit_nrows = self._limit_nrows
        limit = self._get_limit(query_text)
        if not limit:
            logger.warning(f'SELECT query will be limited to {limit_nrows}')
            query_text += f' LIMIT {limit_nrows}'
        elif limit > limit_nrows:
            logger.warning(
                f'SELECT query limit will be reduced from {limit} to {limit_nrows}'
            )
            query_text = re.sub(
                self.LIMIT_REGEX, f' LIMIT {limit_nrows}', query_text, flags=re.IGNORECASE
            )
        return query_text

    def prepare(self, query_text: str, limit_nrows: Optional[int] = None) -> PreparedSqlQuery:
        logger.debug(f'Initial query text: {query_text}')
        statements = sqlparse.parse(query_text)
        if len(statements) == 0:
            raise IncorrectSqlQueryException('Empty')
        query_type = statements[-1].get_type()
        query_nstatements = len(statements)

        query_text = query_text.strip()
        #  remove redundant ';' at the end of the query
        if query_text[-1] == ';':
            query_text = query_text[:-1]
        query_text = sqlparse.format(query_text, reindent=True, keyword_case='upper')
        if (self._limit_nrows or limit_nrows) and query_type == 'SELECT':
            query_text = self._add_limit(query_text, limit_nrows)

        prepared_sql_query = PreparedSqlQuery(
            text=query_text,
            query_type=query_type,
            nstatements=query_nstatements
        )
        logger.debug(f'Prepared query: {prepared_sql_query}')
        return prepared_sql_query
