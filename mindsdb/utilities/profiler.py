from functools import wraps

from mindsdb.utilities.context import context as ctx


def start(tag):
    ctx.profiling_start_node(tag)


def set_meta(**kwargs):
    ctx.profiling_set_meta(**kwargs)


def stop():
    ctx.profiling_stop_current_node()


def profile(tag: str = None):
    def decorator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            start(tag or f'{function.__module__}.{function.__name__}')
            result = function(*args, **kwargs)
            stop()
            return result
        return wrapper
    return decorator


class Context:
    def __init__(self, tag):
        self.tag = tag

    def __enter__(self):
        start(self.tag)

    def __exit__(self, exc_type, exc_value, traceback):
        stop()
