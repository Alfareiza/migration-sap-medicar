import functools
import pickle
import time
from datetime import datetime, timedelta
from functools import wraps

from googleapiclient.errors import HttpError

from base.exceptions import RetryMaxException
from core.settings import logger as log, BASE_DIR, DEBUG
from utils.resources import login_check

login_pkl = BASE_DIR / 'login.pickle'


def ignore_unhashable(func):
    uncached = func.__wrapped__
    attributes = functools.WRAPPER_ASSIGNMENTS + ('cache_info', 'cache_clear')

    @wraps(func, assigned=attributes)
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
            start = time.time()
            value = func(*fargs, **fkwargs)
            title = ''
            log.info(f"{title or tag} {func.__name__!r} tardó {format(time.time() - start, '.4f')}s.")
            return value

        return wrapper

    return decorator


def retry_until_true(max_attempts=3, retry_interval=3):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(max_attempts):
                try:
                    result = func(*args, **kwargs)
                except HttpError:
                    log.warning(f'Tentativa #{i+1} fallida... intentando de nuevo')
                    time.sleep(retry_interval)
                else:
                    return result

            raise RetryMaxException  # Return None if max_attempts are exhausted without success

        return wrapper

    return decorator


def login_required(func):
    @wraps(func)
    def wrapper(*fargs, **fkwargs):
        self = fargs[0]  # Instancia de SAPData
        login_succeed = login_check(self)
        if login_succeed:
            return func(*fargs, **fkwargs)

    return wrapper


def once_in_interval(interval_seconds):
    def decorator(func):
        last_execution_time = datetime.min

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal last_execution_time
            current_time = datetime.now()
            if current_time - last_execution_time >= timedelta(seconds=interval_seconds):
                # Execute the function
                result = func(*args, **kwargs)
                # Update the last execution time
                last_execution_time = datetime.now()
                return result
            else:
                # Function was not executed due to repeated attempt
                log.warning(
                    f"{func.__name__!r} wasn\'t executed due to a repeated attempt within {interval_seconds} seconds.")

        return wrapper

    return decorator


def not_on_debug(func):
    """Once decorate a func, avoid his execution
     when the DEBUG env var is True"""

    @wraps(func)
    def wrapper(*fargs, **fkwargs):
        if not DEBUG:
            return func(*fargs, **fkwargs)

    return wrapper
