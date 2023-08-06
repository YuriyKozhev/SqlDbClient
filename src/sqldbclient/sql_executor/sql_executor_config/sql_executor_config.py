from typing import Optional

from sqlalchemy.engine.base import Engine
from sqldbclient.sql_engine_factory import sql_engine_factory


class SqlExecutorConf:
    """Class that stores parameters used to build SqlExecutor"""
    def __init__(self,
                 engine: Optional[Engine] = None,
                 max_rows_read: Optional[int] = 10_000,
                 history_db_name: Optional[str] = 'sql_executor_history_v1'):
        self.engine = engine
        self.max_rows_read = max_rows_read
        self.history_db_name = history_db_name

    def set(self, parameter: str, *args, **kwargs) -> 'SqlExecutorConf':
        """Sets value for parameter.
        For parameter == 'engine_options', instance of sqlalchemy Engine will be created.
        Available parameters at the moment:

        - engine_options: args and kwargs to pass to sqlalchemy create_engine function

        - max_rows_read: default value for LIMIT clause

        - history_db_name: a file name for SQLLite database

        """
        if parameter == 'engine_options':
            self.engine = sql_engine_factory.get_or_create(*args, **kwargs)
            return self
        value = args[0]
        self.__setattr__(parameter, value)
        return self
