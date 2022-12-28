from typing import Optional

import sqlalchemy
import pandas as pd
try:
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.ext.asyncio.engine import AsyncEngine, AsyncConnection
    from sqlalchemy.exc import ResourceClosedError
except ImportError as e:
    raise ImportError(f'Async tools requires sqlalchemy version >= 1.4')

from sqldbclient.utils.log_decorators import class_logifier


@class_logifier(methods=['execute'])
class SqlAsyncExecutor:
    def __init__(self, engine: AsyncEngine):
        self._engine = engine

    async def execute(self, query: str, outside_transaction: bool = False) -> Optional[pd.DataFrame]:
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
