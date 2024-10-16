import signal


def timeout(seconds):
    def process_timeout(func):
        def handle_timeout(signum, frame):
            raise TimeoutError("The function timed out")

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, handle_timeout)
            signal.alarm(seconds)

            try:
                func(*args, **kwargs)
            finally:
                signal.alarm(0)

        return wrapper

    return process_timeout
