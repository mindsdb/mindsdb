import random
import time
import math

import openai


def retry_with_exponential_backoff(
        initial_delay: float = 1,
        hour_budget: float = 0.3,
        jitter: bool = False,
        exponential_base: int = 2,
        errors: tuple = (openai.error.RateLimitError, openai.error.OpenAIError),
):
    """
    Wrapper to enable optional arguments. It means this decorator always needs to be called with parenthesis:
    
    > @retry_with_exponential_backoff()  # optional argument override here
    > def f(): [...]
    
    """  # noqa

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
                try:
                    max_retries = round(
                        (math.log((hour_budget * 3600) / initial_delay)) /
                        math.log(exponential_base))
                except ValueError:
                    max_retries = 10
            else:
                max_retries = 10
            max_retries = max(1, max_retries)

            while True:
                try:
                    return func(*args, **kwargs)
                except errors as e:
                    if e.error['type'] == 'insufficient_quota':
                        raise Exception(
                            'API key has exceeded its quota, please try 1) increasing it or 2) using another key.')  # noqa

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
