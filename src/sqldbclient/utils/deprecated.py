from typing import Callable
import warnings
import functools


def deprecated(foo: Callable):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""

    @functools.wraps(foo)
    def deprecated_foo(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn(f"Function {foo.__name__} is deprecated",
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return foo(*args, **kwargs)

    return deprecated_foo
