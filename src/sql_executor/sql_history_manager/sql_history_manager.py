from .executed_sql_query import ExecutedSqlQuery
from ..log_decorators import class_logifier
from ..singleton import Singleton

from sqlalchemy import create_engine, select
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
