import functools
from functools import wraps
from time import time

from core.settings import logger


def ignore_unhashable(func):
    uncached = func.__wrapped__
    attributes = functools.WRAPPER_ASSIGNMENTS + ('cache_info', 'cache_clear')
    @functools.wraps(func, assigned=attributes)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TypeError as error:
            if 'unhashable type' in str(error):
                return uncached(*args, **kwargs)
            raise
    wrapper.__uncached__ = uncached
    return wrapper

def logtime(tag):
    def decorator(func):
        @wraps(func)
        def wrapper(*fargs, **fkwargs):
            start = time()
            value = func(*fargs, **fkwargs)
            title = ''
            logger.info(f"{title or tag} {func.__name__!r} tardó {format(time() - start, '.4f')}s.")
            return value
        return wrapper
    return decorator