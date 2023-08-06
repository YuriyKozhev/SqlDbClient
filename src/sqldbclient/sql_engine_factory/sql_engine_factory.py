from sqlalchemy.engine.base import Engine
import functools
from sqlalchemy import create_engine
from sqldbclient.utils.log_decorators import class_logifier


@class_logifier(['get_or_create'])
class SqlEngineFactory:
    """ Class that provides factory for creating engines,
    and makes only one engine for a unique combination of arguments.
    """
    @functools.lru_cache(maxsize=None, typed=False)
    def get_or_create(self, *args, **kwargs) -> Engine:
        """Wrapping around sqlalchemy create_engine function.
        Only one engine will be created for each unique combination of arguments.
        """
        engine = create_engine(*args, **kwargs)
        return engine
