import functools

from sqldbclient.sql_executor.sql_executor import SqlExecutor
from sqldbclient.sql_executor.sql_executor_builder.parameter_not_specified_exception \
    import ParameterNotSpecifiedException
from sqldbclient.sql_executor.sql_executor_config.sql_executor_config import SqlExecutorConf


class SqlExecutorBuilder:
    """Class that defines builder for SqlExecutor class,
    creates only one instance per unique set of arguments given SqlExecutorConf
    """
    __slots__ = ['engine', 'max_rows_read', 'history_db_name']

    def config(self, config: SqlExecutorConf) -> 'SqlExecutorBuilder':
        """Reads parameter values from config"""
        for parameter in self.__slots__:
            if not hasattr(config, parameter):
                raise ParameterNotSpecifiedException(parameter)
            value = getattr(config, parameter)
            if value is None:
                raise ParameterNotSpecifiedException(parameter)
            self.__setattr__(parameter, value)
        return self

    @functools.lru_cache(maxsize=None, typed=False)
    def _get_or_create_instance(self, *args, **kwargs) -> SqlExecutor:
        return SqlExecutor(*args, **kwargs)

    def get_or_create(self) -> SqlExecutor:
        """Creates SqlExecutor instance from SqlExecutorConf parameters.
        Only one instance will be created per unique set of arguments.
        """
        sql_executor = self._get_or_create_instance(
            engine=self.engine,
            max_rows_read=self.max_rows_read,
            history_db_name=self.history_db_name
        )
        return sql_executor
