"""This module contains some useful features which
help to increase solution stability in network layer"""

import time


def sending_attempts(exception_type=Exception, attempts_number=3, delay=0.5):
    """Decorator factory for functions where several call attempts may require for success"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            to_raise = None
            res = None
            for _ in range(attempts_number):
                try:
                    res = func(*args, **kwargs)
                    break
                # Save catched exception
                except Exception as e:
                    to_raise = e
                    time.sleep(delay)
            # Else executed if there was no loop breaks
            else:
                raise exception_type(f"{type(to_raise)}: {to_raise}") from to_raise  # noqa: pylint: disable=raising-bad-type
            return res
        return wrapper
    return decorator
