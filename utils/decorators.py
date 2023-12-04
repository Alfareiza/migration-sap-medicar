import functools
import pickle
import time
from functools import wraps

from core.settings import logger as log, BASE_DIR
from utils.resources import moment, datetime_str

login_pkl = BASE_DIR / 'login.pickle'


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
            start = time.time()
            value = func(*fargs, **fkwargs)
            title = ''
            log.info(f"{title or tag} {func.__name__!r} tardó {format(time.time() - start, '.4f')}s.")
            return value
        return wrapper
    return decorator


def retry_until_true(max_attempts=3, retry_interval=3):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for _ in range(max_attempts):
                result = func(*args, **kwargs)
                log.info(f'Tentativa # max_attempts {max_attempts}')
                if result:
                    return result
                time.sleep(retry_interval)
            return None  # Return None if max_attempts are exhausted without success
        return wrapper
    return decorator

def login_required(func):
        @wraps(func)
        def wrapper(*fargs, **fkwargs):
            self = fargs[0]  # Instancia de SAPData
            login_succeed = False
            if not login_pkl.exists():
                log.info('Login vencido ...')
                login_succeed = self.login()
            else:
                with open(login_pkl, 'rb') as f:
                    sess_id, sess_timeout = pickle.load(f)
                    now = moment()
                    if now > sess_timeout:
                        log.info('Login vencido ...')
                        login_succeed = self.login()
                    else:
                        # log.info(f"Login válido. {datetime_str(now)} es menor que {datetime_str(sess_timeout)}")
                        self.sess_id = sess_id
                        login_succeed = True
            if login_succeed:
                return func(*fargs, **fkwargs)

        return wrapper