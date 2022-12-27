from typing import Optional
import time
import sqlalchemy
from sqlalchemy.engine.base import Engine, RootTransaction
from sqldbclient.utils.log_decorators import logger
from sqldbclient.utils.deprecated import deprecated
from sqldbclient.sql_transaction_manager.not_in_transaction_exception import NotInTransActionException


class SqlTransactionManager:
    def __init__(self, engine: Engine):
        self._engine = engine
        self._transaction: Optional[RootTransaction] = None
        self._start: Optional[float] = None

    @property
    def _is_in_transaction(self) -> bool:
        if self._transaction is None:
            return False
        return self._transaction.is_active

    def _get_connection(self, outside_transaction: bool = False):
        if self._is_in_transaction:
            if outside_transaction:
                raise ValueError('Unable to get connection outside transaction while in transaction')
            return self._transaction.connection
        connection = self._engine.connect()
        if outside_transaction:
            # workaround to get outside of implicit transaction
            connection.execute(sqlalchemy.text('COMMIT'))
        return connection

    def __enter__(self):
        logger.warning(f'Starting transaction')
        self._start = time.perf_counter()
        connection = self._get_connection()
        self._transaction = connection.begin()
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        finish = time.perf_counter()
        logger.warning(f'Exiting transaction, duration={finish - self._start:0.2f} seconds')
        if self._is_in_transaction:
            self.rollback()
        self._transaction.connection.close()

    def commit(self):
        if not self._is_in_transaction:
            raise NotInTransActionException()
        self._transaction.commit()
        logger.warning(f'Transaction committed')

    def rollback(self):
        if not self._is_in_transaction:
            raise NotInTransActionException()
        self._transaction.rollback()
        logger.warning(f'Transaction rolled back')

    @deprecated
    def commit_transaction(self):
        return self.commit()

    @deprecated
    def rollback_transaction(self):
        return self.rollback()
