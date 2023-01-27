import random
import time

import openai


def retry_with_exponential_backoff(
        initial_delay: float = 1,
        hour_budget: float = 0.3,
        jitter: bool = False,
        max_retries: int = 10,
        errors: tuple = (openai.error.RateLimitError,),
):
    """
    Wrapper to enable optional arguments. It means this decorator always needs to be called with parenthesis:
    
    > @retry_with_exponential_backoff()  # optional argument override here
    > def f(): [...]
    
    """  # noqa
    max_retries = max(1, max_retries)

    def _retry_with_exponential_backoff(func):
        """
        Exponential backoff to retry requests on a rate-limited API call, as recommended by OpenAI.
        Loops the call until a successful response or max_retries is hit or an exception is raised.

        Slight changes in the implementation, but originally from:
        https://github.com/openai/openai-cookbook/blob/main/examples/How_to_handle_rate_limits.ipynb
        """  # noqa

        def wrapper(*args, **kwargs):
            num_retries = 0
            delay = initial_delay

            if isinstance(hour_budget, float) or isinstance(hour_budget, int):
                exponential_base = ((hour_budget * 3600) / initial_delay) ** (1 / max_retries)
            else:
                exponential_base = 2

            while True:
                try:
                    return func(*args, **kwargs)
                except errors:
                    num_retries += 1
                    if num_retries > max_retries:
                        raise Exception(
                            f"Maximum number of retries ({max_retries}) exceeded."
                        )
                    # Increment the delay and wait
                    delay *= exponential_base * (1 + jitter * random.random())
                    time.sleep(delay)

                except Exception as e:
                    raise e

        return wrapper

    return _retry_with_exponential_backoff


# @retry_with_exponential_backoff(errors=(Exception,))
# def f():
#     p = random.random()
#     if p > 0.5:
#         return
#     else:
#         raise Exception
