from sqlalchemy.engine.base import Engine
import functools
from sqlalchemy import create_engine
from .log_decorators import method_logifier, class_logifier
from .singleton import Singleton


@class_logifier(['make_engine'])
class SqlEngineFactory(metaclass=Singleton):
    def __init__(self, **default_params):
        self._default_params = default_params

    def _make_engine(self, **kwargs):
        engine_params = self._default_params
        engine_params.update(kwargs)
        engine = create_engine(**engine_params)
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
