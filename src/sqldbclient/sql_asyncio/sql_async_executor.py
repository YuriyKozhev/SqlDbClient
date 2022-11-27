from ..log_decorators import class_logifier
from ..singleton import Singleton

from sqlalchemy import text
import pandas as pd

try:
    from sqlalchemy.ext.asyncio import create_async_engine
except ImportError as e:
    raise ImportError(f'Async tools requires sqlalchemy version >= 1.4')


@class_logifier(methods=['read_query', 'execute_query'])
class SqlAsyncExecutor(metaclass=Singleton):
    def __init__(self, **kwargs):
        self._engine = create_async_engine(**kwargs)
#         sql_asyncio.run(self._test_engine)

    async def test_engine(self) -> None:
        assert (await self.read_query('SELECT 1 as a') == pd.DataFrame({'a': [1]})).all().iloc[0]

    async def execute_query(self, query: str) -> None:
        async with self._engine.connect() as conn:
            await conn.execute(text(query))

    async def read_query(self, query: str) -> pd.DataFrame:
        async with self._engine.connect() as conn:
            result = await conn.execute(text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
