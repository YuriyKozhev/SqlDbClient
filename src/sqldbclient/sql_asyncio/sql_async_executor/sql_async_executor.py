from typing import Optional

import sqlalchemy
import pandas as pd
try:
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.ext.asyncio.engine import AsyncEngine, AsyncConnection
    from sqlalchemy.exc import ResourceClosedError
except ImportError:
    raise ImportError('Async tools requires sqlalchemy version >= 1.4')

from sqldbclient.utils.log_decorators import class_logifier


@class_logifier(methods=['execute'])
class SqlAsyncExecutor:
    """ Simplified asynchronous version of SqlExecutor.
    It provides a handy async execute method.
    """
    def __init__(self, engine: AsyncEngine):
        self._engine = engine

    async def execute(self, query: str, outside_transaction: bool = False) -> Optional[pd.DataFrame]:
        """Executes query asynchronously.

        :param query: query text.
        :param outside_transaction: If ``True``, sqlalchemy will not create separate transaction to execute query.
            It may come in handy while executing stored procedures (e. g. in PostgreSQL),
            which commit results themselves. Otherwise, InvalidTransactionTermination may be raised.
        :return: (optional) If query selects any rows then a pandas DataFrame will be returned.
        """
        connection = self._engine.connect()
        async with connection:
            if outside_transaction:
                await connection.execute(sqlalchemy.text('COMMIT'))
            text_sa_clause = sqlalchemy.text(query)
            result = await connection.execute(text_sa_clause)
            await connection.commit()
        try:
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df
        except ResourceClosedError:
            return None
        finally:
            await connection.close()
