from sqlalchemy.engine.base import Engine
import functools
from sqlalchemy import create_engine
from sqldbclient.utils.log_decorators import class_logifier
from sqldbclient.utils.singleton import Singleton


@class_logifier(['get_or_create'])
class SqlEngineFactory(metaclass=Singleton):
    @functools.lru_cache(maxsize=None, typed=False)
    def get_or_create(self, *args, **kwargs) -> Engine:
        engine = create_engine(*args, **kwargs)
        return engine