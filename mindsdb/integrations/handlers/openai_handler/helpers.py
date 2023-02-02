import random
import time

import openai


def retry_with_exponential_backoff(
    func,
    initial_delay: float = 1,
    exponential_base: float = 2,
    jitter: bool = False,
    max_retries: int = 10,
    errors: tuple = (openai.error.RateLimitError,),
):
    """
    Exponential backoff to retry requests on a rate-limited API call, as recommended by OpenAI.
    Loops the call until a successful response or max_retries is hit or an exception is raised.

    Slight changes in the implementation, but originally from:
    https://github.com/openai/openai-cookbook/blob/main/examples/How_to_handle_rate_limits.ipynb
    """

    def wrapper(*args, **kwargs):
        num_retries = 0
        delay = initial_delay

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
