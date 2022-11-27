from typing import Optional
from datetime import datetime
from sqlalchemy.engine.base import Engine, RootTransaction
from .log_decorators import logger


class SqlTransactionManager:
    def __init__(self, engine: Engine):
        self._engine = engine
        self._is_in_transaction: bool = False
        self._transaction: Optional[RootTransaction] = None
        self._transaction_start: Optional[datetime] = None

    def __enter__(self):
        logger.warning(f'Starting transaction')
        self._transaction_start = datetime.now()
        self._is_in_transaction = True
        self._transaction = self._engine.connect().begin()
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        finish = datetime.now().replace(microsecond=self._transaction_start.microsecond)
        logger.warning(f'Exiting transaction, duration={finish - self._transaction_start} seconds')
        self._transaction.rollback()
        self._transaction.connection.close()
        self._is_in_transaction = False

    def start_transaction(self):
        return self

    def commit_transaction(self):
        if not self._is_in_transaction:
            raise Exception("Not in transaction")
        logger.warning(f'Transaction committed')
        self._transaction.commit()

    def rollback_transaction(self):
        if not self._is_in_transaction:
            raise Exception("Not in transaction")
        logger.warning(f'Transaction rolled back')
        self._transaction.rollback()
