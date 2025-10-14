from typing import Text, List, Dict
import random
import time
import math

import openai

import tiktoken

import mindsdb.utilities.profiler as profiler


class PendingFT(openai.OpenAIError):
    """
    Custom exception to handle pending fine-tuning status.
    """

    message: str

    def __init__(self, message) -> None:
        super().__init__()
        self.message = message


def retry_with_exponential_backoff(
    initial_delay: float = 1,
    hour_budget: float = 0.3,
    jitter: bool = False,
    exponential_base: int = 2,
    wait_errors: tuple = (openai.APITimeoutError, openai.APIConnectionError, PendingFT),
    status_errors: tuple = (openai.APIStatusError, openai.APIResponseValidationError),
):
    """
    Wrapper to enable optional arguments. It means this decorator always needs to be called with parenthesis:

    > @retry_with_exponential_backoff()  # optional argument override here
    > def f(): [...]

    """  # noqa

    @profiler.profile()
    def _retry_with_exponential_backoff(func):
        """
        Exponential backoff to retry requests on a rate-limited API call, as recommended by OpenAI.
        Loops the call until a successful response or max_retries is hit or an exception is raised.

        Slight changes in the implementation, but originally from:
        https://github.com/openai/openai-cookbook/blob/main/examples/How_to_handle_rate_limits.ipynb

        Args:
            func: Function to be wrapped
            initial_delay: Initial delay in seconds
            hour_budget: Hourly budget in seconds
            jitter: Adds randomness to the delay
            exponential_base: Base for the exponential backoff
            wait_errors: Tuple of errors to retry on
            status_errors: Tuple of status errors to raise

        Returns:
            Wrapper function with exponential backoff
        """  # noqa

        def wrapper(*args, **kwargs):
            num_retries = 0
            delay = initial_delay

            if isinstance(hour_budget, float) or isinstance(hour_budget, int):
                try:
                    max_retries = round((math.log((hour_budget * 3600) / initial_delay)) / math.log(exponential_base))
                except ValueError:
                    max_retries = 10
            else:
                max_retries = 10
            max_retries = max(1, max_retries)

            while True:
                try:
                    return func(*args, **kwargs)

                except status_errors as e:
                    raise Exception(
                        f"Error status {e.status_code} raised by Lemonade API: {e.body.get('message', 'Please refer to the Lemonade documentation for more information.')}"  # noqa
                    )  # noqa

                except wait_errors:
                    num_retries += 1
                    if num_retries > max_retries:
                        raise Exception(f"Maximum number of retries ({max_retries}) exceeded.")
                    # Increment the delay and wait
                    delay *= exponential_base * (1 + jitter * random.random())
                    time.sleep(delay)

                except openai.OpenAIError as e:
                    raise Exception(
                        f"General {str(e)} error raised by Lemonade. Please refer to the Lemonade documentation for more information."  # noqa
                    )

                except Exception as e:
                    raise e

        return wrapper

    return _retry_with_exponential_backoff


def truncate_msgs_for_token_limit(messages: List[Dict], model_name: Text, max_tokens: int, truncate: Text = "first"):
    """
    Truncates message list to fit within the token limit.
    The first message for chat completion models are general directives with the system role, which will ideally be kept at all times.

    Slight changes in the implementation, but originally from:
    https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb

    Args:
        messages (List[Dict]): List of messages
        model_name (Text): Model name
        max_tokens (int): Maximum token limit
        truncate (Text): Truncate strategy, either 'first' or 'last'

    Returns:
        List[Dict]: Truncated message list
    """  # noqa
    try:
        encoder = tiktoken.encoding_for_model(model_name)
    except KeyError:
        # If the encoding is not found, default to cl100k_base.
        # This is applicable for handlers that extend the OpenAI handler such as Lemonade.
        model_name = "gpt-3.5-turbo-0301"
        encoder = tiktoken.get_encoding("cl100k_base")

    # Validate messages format
    valid_messages = []
    for message in messages:
        if isinstance(message, dict) and "role" in message and "content" in message:
            valid_messages.append(message)
        else:
            # Skip invalid messages or convert them to a valid format
            if isinstance(message, dict):
                # Try to extract role and content from the message
                role = message.get("role", "user")
                content = message.get("content", str(message))
                valid_messages.append({"role": role, "content": content})
            else:
                # Convert non-dict messages to user messages
                valid_messages.append({"role": "user", "content": str(message)})
    
    if not valid_messages:
        return messages  # Return original if no valid messages
    
    messages = valid_messages
    sys_priming = messages[0:1]
    n_tokens = count_tokens(messages, encoder, model_name)
    while n_tokens > max_tokens:
        if len(messages) == 2:
            return messages[:-1]  # edge case: if limit is surpassed by just one input, we remove initial instruction
        elif len(messages) == 1:
            return messages

        if truncate == "first":
            messages = sys_priming + messages[2:]
        else:
            messages = sys_priming + messages[1:-1]

        n_tokens = count_tokens(messages, encoder, model_name)
    return messages


def count_tokens(messages: List[Dict], encoder: tiktoken.core.Encoding, model_name: Text = "gpt-3.5-turbo-0301"):
    """
    Counts the number of tokens in a list of messages.

    Args:
        messages: List of messages
        encoder: Tokenizer
        model_name: Model name
    """
    if "gpt-3.5-turbo" in model_name:  # note: future models may deviate from this (only 0301 really complies)
        tokens_per_message = 4  # every message follows <|start|>{role/name}\n{content}<|end|>\n
        tokens_per_name = -1
    else:
        tokens_per_message = 3
        tokens_per_name = 1

    num_tokens = 0
    for message in messages:
        if not isinstance(message, dict):
            continue  # Skip invalid messages
            
        num_tokens += tokens_per_message

        for key, value in message.items():
            # Ensure value is a string before encoding
            try:
                if isinstance(value, str):
                    num_tokens += len(encoder.encode(value))
                else:
                    num_tokens += len(encoder.encode(str(value)))
                if key == "name":  # if there's a name, the role is omitted
                    num_tokens += tokens_per_name
            except Exception:
                # Skip problematic values
                continue
    num_tokens += 2  # every reply is primed with <im_start>assistant
    return num_tokens


def get_available_models(client) -> List[Text]:
    """
    Returns a list of available models for the given Lemonade server.

    Args:
        client: openai sdk client

    Returns:
        List[Text]: List of available models
    """
    try:
        res = client.models.list()
        return [models.id for models in res.data]
    except Exception:
        # If we can't get the models list, return a default set of common models
        return [
            "Llama-3.2-1B-Instruct-Hybrid",
            "Llama-3.2-3B-Instruct-Hybrid", 
            "Llama-3.2-8B-Instruct-Hybrid",
            "Llama-3.2-11B-Instruct-Hybrid",
            "Llama-3.2-90B-Instruct-Hybrid",
            "Gemma-3-4b-it-GGUF",
            "Gemma-3-8b-it-GGUF",
            "Mistral-7B-Instruct-v0.3",
            "Qwen2.5-7B-Instruct",
            "Phi-3.5-mini-instruct"
        ]
