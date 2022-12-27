from typing import Optional
import time
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

    def __enter__(self):
        logger.warning(f'Starting transaction')
        self._start = time.perf_counter()
        self._transaction = self._engine.connect().begin()
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
    def start_transaction(self):
        return self

    @deprecated
    def commit_transaction(self):
        return self.commit()

    @deprecated
    def rollback_transaction(self):
        return self.rollback()
