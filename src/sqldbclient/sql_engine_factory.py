from sqlalchemy.engine.base import Engine
import functools
from sqlalchemy import create_engine
from .log_decorators import method_logifier, class_logifier
from .singleton import Singleton


@class_logifier(['make_engine'])
class SqlEngineFactory(metaclass=Singleton):
    def __init__(self, *default_args, **default_kwargs):
        self._default_args = default_args
        self._default_kwargs = default_kwargs

    def _make_engine(self, **kwargs):
        engine_kwargs = self._default_kwargs
        engine_kwargs.update(kwargs)
        engine = create_engine(*self._default_args, **engine_kwargs)
        engine.execute = method_logifier(engine.execute, Engine)
        return engine

    @staticmethod
    def _test_engine(engine: Engine):
        assert engine.execute('SELECT 1').fetchone() == (1,), 'Engine failed test'
        engine.dispose()

    @functools.lru_cache()
    def make_engine(self, **kwargs) -> Engine:
        engine = self._make_engine(**kwargs)
        self._test_engine(engine)
        return engine
