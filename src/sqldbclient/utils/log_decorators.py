import inspect
from datetime import datetime
import logging
import functools

logger = logging.getLogger(__name__)


def time_logifier(method, class_):
    @functools.wraps(method)
    def _logged_method(*args, **kwargs):
        logger.debug(f'Started {method.__name__} of {class_.__name__}')
        start = datetime.now()
        result = method(*args, **kwargs)
        finish = datetime.now()
        finish = finish.replace(microsecond=start.microsecond)
        logger.debug(f'Finished {method.__name__} of {class_.__name__}, duration = {finish - start}')
        return result

    async def _async_logged_method(*args, **kwargs):
        logger.debug(f'Started {method.__name__} of {class_.__name__}')
        start = datetime.now()
        result = await method(*args, **kwargs)
        finish = datetime.now()
        finish = finish.replace(microsecond=start.microsecond)
        logger.debug(f'Finished {method.__name__} of {class_.__name__}, duration = {finish - start}')
        return result

    if inspect.iscoroutinefunction(method):
        return _async_logged_method
    else:
        return _logged_method


def class_logifier(methods):
    def _logifier(class_):
        for method in methods:
            if not callable(getattr(class_, method)):
                raise Exception(f'Member {method} of class {class_} not callable!')
            logged_method = time_logifier(getattr(class_, method), class_)
            setattr(class_, method, logged_method)
        return class_

    return _logifier
