import logging
from typing import Optional
from datetime import datetime

import sqlalchemy
from sqlalchemy.engine.base import Engine, RootTransaction

from sqldbclient.utils.deprecated import deprecated
from sqldbclient.sql_transaction_manager.not_in_transaction_exception import NotInTransActionException

logger = logging.getLogger(__name__)


class SqlTransactionManager:
    """Class that is responsible for transaction management.
    Provides handy context manager, which used as follows (sql_executor is an instance of SqlTransactionManager)::

        with sql_executor:
            sql_executor.execute('DROP TABLE IF EXISTS foo')
            sql_executor.execute('CREATE TABLE foo AS SELECT 1 AS a')
            sql_executor.execute('SELECT * FROM foo')
            sql_executor.commit()

    """
    def __init__(self, engine: Engine):
        self._engine = engine
        self._transaction: Optional[RootTransaction] = None
        self._start: Optional[datetime] = None

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
        if self._is_in_transaction:
            raise NotImplementedError('Nested transaction are not supported yet')
        logger.warning('Starting transaction')
        self._start = datetime.now()
        connection = self._get_connection()
        self._transaction = connection.begin()
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        finish = datetime.now()
        finish = finish.replace(microsecond=self._start.microsecond)
        logger.warning(f'Exiting transaction, duration = {finish - self._start}')
        if self._is_in_transaction:
            self.rollback()
        self._transaction.connection.close()

    def commit(self):
        """Commits transaction"""
        if not self._is_in_transaction:
            raise NotInTransActionException()
        self._transaction.commit()
        logger.warning('Transaction committed')

    def rollback(self):
        """Rolls transaction back"""
        if not self._is_in_transaction:
            raise NotInTransActionException()
        self._transaction.rollback()
        logger.warning('Transaction rolled back')

    @deprecated
    def commit_transaction(self):
        """Deprecated, use commit"""
        return self.commit()

    @deprecated
    def rollback_transaction(self):
        """Deprecated, use rollback"""
        return self.rollback()
