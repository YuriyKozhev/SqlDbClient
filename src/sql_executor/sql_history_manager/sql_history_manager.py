from typing import Optional, List

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from .config import mapper_registry
from .executed_sql_query import ExecutedSqlQuery
from .executed_sql_query_result import ExecutedSqlQueryResult
from ..log_decorators import class_logifier
from ..singleton import Singleton


@class_logifier(methods=['dump', 'get_result', 'get_data'])
class SqlHistoryManager(metaclass=Singleton):

    def __init__(self, history_db_name: str) -> None:
        self._engine = create_engine(f'sqlite:///{history_db_name}', echo=False, future=True)
        self._session = Session(self._engine)

        mapper_registry.metadata.create_all(self._engine)

        self._query_results = {}

    @property
    def _executed_queries(self) -> List[ExecutedSqlQuery]:
        return self._session.query(ExecutedSqlQuery).all()

    def get_data(self) -> pd.DataFrame:
        data = pd.DataFrame(self._executed_queries)
        return data

    @staticmethod
    def _parse_result(result: ExecutedSqlQueryResult) -> pd.DataFrame:
        df = result.dataframe
        for i, col in enumerate(df.columns):
            df[col] = df[col].astype(result.datatypes[i])
        return df

    def get_result(self, uuid: str, reload: bool = False) -> pd.DataFrame:
        if not reload and uuid in self._query_results:
            return self._query_results[uuid]

        result = self._session.query(ExecutedSqlQueryResult).where(
            ExecutedSqlQueryResult.uuid == uuid
        ).first()

        if result is None:
            raise ValueError(f'No result found for uuid = {uuid}')

        self._session.expunge(result)
        df = self._parse_result(result)
        self._query_results[uuid] = df
        return df

    def dump(self, executed_query: ExecutedSqlQuery, df: Optional[pd.DataFrame] = None) -> None:
        self._session.add(executed_query)

        if df is not None:
            uuid = executed_query.uuid
            result = ExecutedSqlQueryResult(uuid=uuid, dataframe=df)
            self._session.add(result)
            self._query_results[uuid] = df

        self._session.commit()

    # TODO section

    # estimated_size_in_mbs = '''
    #         CAST(length(uuid) + length(query) + length(start_time) + length(finish_time) + length(duration)
    #             + length(result) AS REAL) / 1024 / 1024
    #     '''
    #
    # def _execute(self, query):
    #     with self._engine.connect() as conn:
    #         conn.commit()
    #         conn.execute(text(query))
    #         conn.commit()
    #
    # def _read(self, query):
    #     with self._engine.connect() as conn:
    #         return pd.read_sql(text(query), conn)
    #
    # def calc_size(self) -> pd.DataFrame:
    #     return self._read(f'''
    #         SELECT uuid, query, start_time, {self.estimated_size_in_mbs} as estimated_size_in_mbs
    #         FROM {EXECUTED_SQL_QUERY_TABLE_NAME}
    #     ''')
    #
    # def _vacuum(self):
    #     self._execute('VACUUM')
    #
    # def delete_records(self, min_start_time: Optional[Union[datetime, str]] = None,
    #                    max_estimated_size_in_mbs: Optional[float] = None):
    #     delete_query = f'DELETE FROM {EXECUTED_SQL_QUERY_TABLE_NAME} WHERE '
    #
    #     if min_start_time:
    #         if max_estimated_size_in_mbs:
    #             delete_query += f"""
    #                 start_time < '{str(min_start_time)}' AND
    #                 {self.estimated_size_in_mbs} > {max_estimated_size_in_mbs}
    #             """
    #         else:
    #             delete_query += f"""start_time < '{str(min_start_time)}'"""
    #     else:
    #         if max_estimated_size_in_mbs:
    #             delete_query += f"""{self.estimated_size_in_mbs} > {max_estimated_size_in_mbs}"""
    #         else:
    #             raise ValueError('Both arguments are None')
    #     logger.warning(f'Sqlite executing:\n{delete_query}')
    #     self._execute(delete_query)
    #     self._vacuum()
