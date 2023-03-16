import random
import time
import math

import openai
import tiktoken


def retry_with_exponential_backoff(
        initial_delay: float = 1,
        hour_budget: float = 0.3,
        jitter: bool = False,
        exponential_base: int = 2,
        errors: tuple = (openai.error.RateLimitError,),
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
                    if e.error['type'] == 'invalid_request_error' and \
                            'Too many parallel completions' in e.error['message'] or \
                            'Please reduce the length of the messages' in e.error['message']:
                        raise e  # InvalidRequestError triggers batched mode in the previous call
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

                except openai.error.OpenAIError as e:
                    if e.error['type'] == 'insufficient_quota':
                        raise Exception(
                            'API key has exceeded its quota, please try 1) increasing it or 2) using another key.')  # noqa
                    raise e

                except Exception as e:
                    raise e

        return wrapper

    return _retry_with_exponential_backoff


def truncate_msgs_for_token_limit(messages, model_name, max_tokens, truncate='first'):
    """ 
    Truncates message list to fit within the token limit.
    Note: first message for chat completion models are general directives with the system role, which will ideally be kept at all times. 
    """  # noqa
    encoder = tiktoken.encoding_for_model(model_name)
    sys_priming = messages[0:1]
    n_tokens = count_tokens(messages, encoder, model_name)
    while n_tokens > max_tokens:
        if len(messages) == 2:
            return messages[-1]  # edge case: if limit is surpassed by just one input, we remove initial instruction
        elif len(messages) == 1:
            return messages

        if truncate == 'first':
            messages = sys_priming + messages[2:]
        else:
            messages = sys_priming + messages[1:-1]

        n_tokens = count_tokens(messages, encoder, model_name)
    return messages


def count_tokens(messages, encoder, model_name='gpt-3.5-turbo-0301'):
    """ Original token count implementation can be found in the OpenAI cookbook. """
    if "gpt-3.5-turbo" in model_name:  # note: future models may deviate from this (only 0301 really complies)
        num_tokens = 0
        for message in messages:
            num_tokens += 4  # every message follows <im_start>{role/name}\n{content}<im_end>\n
            for key, value in message.items():
                num_tokens += len(encoder.encode(value))
                if key == "name":  # if there's a name, the role is omitted
                    num_tokens += -1  # role is always required and always 1 token
        num_tokens += 2  # every reply is primed with <im_start>assistant
        return num_tokens
    else:
        raise NotImplementedError(f"""_count_tokens() is not presently implemented for model {model_name}.""")
