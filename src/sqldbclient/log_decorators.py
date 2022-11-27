import inspect
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def method_logifier(method, klass):
    def _logged_method(*args, **kwargs):
        logger.warning(f'Started {method.__name__} of {klass.__name__}')
        start = datetime.now()
        result = method(*args, **kwargs)
        finish = datetime.now()
        finish = finish.replace(microsecond=start.microsecond)
        logger.warning(f'Finished {method.__name__} of {klass.__name__}, duration={finish - start} seconds')
        return result

    async def _async_logged_method(*args, **kwargs):
        logger.warning(f'Started {method.__name__} of {klass.__name__}')
        start = datetime.now()
        result = await method(*args, **kwargs)
        finish = datetime.now()
        finish = finish.replace(microsecond=start.microsecond)
        logger.warning(f'Finished {method.__name__} of {klass.__name__}, duration={finish - start} seconds')
        return result

    if inspect.iscoroutinefunction(method):
        return _async_logged_method
    else:
        return _logged_method


def class_logifier(methods):
    def _logifier(klass):
        for method in methods:
            if not callable(getattr(klass, method)):
                raise Exception(f'Member {method} of class {klass} not callable!')
            logged_method = method_logifier(getattr(klass, method), klass)
            setattr(klass, method, logged_method)
        return klass

    return _logifier
