from typing import Optional, Union
from datetime import datetime

from .executed_sql_query import ExecutedSqlQuery
from ..log_decorators import class_logifier, logger
from ..singleton import Singleton

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import registry, Session
import pandas as pd


@class_logifier(methods=['dump', 'delete_records'])
class SqlHistoryManager(metaclass=Singleton):
    def __init__(self, history_db_name: str):
        self._engine = create_engine(f'sqlite:///{history_db_name}', echo=False, future=True)
        self._session = Session(self._engine)

        self._mapper_registry = registry()
        self._mapper_registry.metadata.create_all(self._engine)

        self._data = self._session.query(ExecutedSqlQuery).all()

    def _refresh_data(self) -> None:
        uuids = [x.uuid for x in self._data]
        new_data = self._session.scalars(select(ExecutedSqlQuery).where(ExecutedSqlQuery.uuid.not_in(uuids))).all()
        self._data.extend(new_data)

    @property
    def data(self) -> pd.DataFrame:
        self._refresh_data()
        return pd.DataFrame(self._data)

    def dump(self, executed_query: ExecutedSqlQuery) -> None:
        self._session.add(executed_query)
        self._session.commit()
        self._refresh_data()

    def _execute(self, query):
        with self._history_db_engine.connect() as conn:
            conn.commit()
            conn.execute(text(query))
            conn.commit()

    def _read(self, query):
        with self._history_db_engine.connect() as conn:
            return pd.read_sql(text(query), conn)

    def calc_size(self) -> pd.DataFrame:
        return self._read(f'''
            SELECT uuid, query, start_time, {self.estimated_size_in_mbs} as estimated_size_in_mbs
            FROM executed_query
        ''')

    def _vacuum(self):
        self._execute('VACUUM executed_query')

    def delete_records(self, min_start_time: Optional[Union[datetime, str]] = None,
                       max_estimated_size_in_mbs: Optional[float] = None):
        delete_query = 'DELETE FROM executed_query WHERE '

        if min_start_time:
            if max_estimated_size_in_mbs:
                delete_query += f"""
                    start_time < '{str(min_start_time)}' AND
                    {self.estimated_size_in_mbs} > {max_estimated_size_in_mbs}
                """
            else:
                delete_query += f"""start_time < '{str(min_start_time)}'"""
        else:
            if max_estimated_size_in_mbs:
                delete_query += f"""{self.estimated_size_in_mbs} > {max_estimated_size_in_mbs}"""
            else:
                raise ValueError('Both arguments are None')
        logger.warning(f'Sqlite executing:\n{delete_query}')
        self._execute(delete_query)
        self._vacuum()
