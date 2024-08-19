from __future__ import annotations

import functools
import logging
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    DefaultDict,
    Dict,
    List,
    Mapping,
    Optional,
    Type,
    TypeVar,
    Union,
)

from typing_extensions import TypedDict

from langsmith import client as ls_client
from langsmith import run_helpers

if TYPE_CHECKING:
    from openai import AsyncOpenAI, OpenAI
    from openai.types.chat.chat_completion_chunk import (
        ChatCompletionChunk,
        Choice,
        ChoiceDeltaToolCall,
    )
    from openai.types.completion import Completion

# Any is used since it may work with Azure or other providers
C = TypeVar("C", bound=Union["OpenAI", "AsyncOpenAI", Any])
logger = logging.getLogger(__name__)


@functools.lru_cache
def _get_not_given() -> Optional[Type]:
    try:
        from openai._types import NotGiven

        return NotGiven
    except ImportError:
        return None


def _strip_not_given(d: dict) -> dict:
    try:
        not_given = _get_not_given()
        if not_given is None:
            return d
        return {k: v for k, v in d.items() if not isinstance(v, not_given)}
    except Exception as e:
        logger.error(f"Error stripping NotGiven: {e}")
        return d


def _infer_invocation_params(model_type: str, kwargs: dict):
    stripped = _strip_not_given(kwargs)

    stop = stripped.get("stop")
    if stop and isinstance(stop, str):
        stop = [stop]

    return {
        "ls_provider": "openai",
        "ls_model_type": model_type,
        "ls_model_name": stripped.get("model", None),
        "ls_temperature": stripped.get("temperature", None),
        "ls_max_tokens": stripped.get("max_tokens", None),
        "ls_stop": stop,
    }


def _reduce_choices(choices: List[Choice]) -> dict:
    reversed_choices = list(reversed(choices))
    message: Dict[str, Any] = {
        "role": "assistant",
        "content": "",
    }
    for c in reversed_choices:
        if c.delta.role:
            message["role"] = c.delta.role
            break
    tool_calls: DefaultDict[int, List[ChoiceDeltaToolCall]] = defaultdict(list)
    for c in choices:
        if c.delta.content:
            message["content"] += c.delta.content
        if c.delta.function_call:
            if not message.get("function_call"):
                message["function_call"] = {"name": "", "arguments": ""}
            if c.delta.function_call.name:
                message["function_call"]["name"] += c.delta.function_call.name
            if c.delta.function_call.arguments:
                message["function_call"]["arguments"] += c.delta.function_call.arguments
        if c.delta.tool_calls:
            for tool_call in c.delta.tool_calls:
                tool_calls[c.index].append(tool_call)
    if tool_calls:
        message["tool_calls"] = [None for _ in tool_calls.keys()]
        for index, tool_call_chunks in tool_calls.items():
            message["tool_calls"][index] = {
                "index": index,
                "id": next((c.id for c in tool_call_chunks if c.id), None),
                "type": next((c.type for c in tool_call_chunks if c.type), None),
            }
            for chunk in tool_call_chunks:
                if chunk.function:
                    if not message["tool_calls"][index].get("function"):
                        message["tool_calls"][index]["function"] = {
                            "name": "",
                            "arguments": "",
                        }
                    if chunk.function.name:
                        message["tool_calls"][index]["function"][
                            "name"
                        ] += chunk.function.name
                    if chunk.function.arguments:
                        message["tool_calls"][index]["function"][
                            "arguments"
                        ] += chunk.function.arguments
    return {
        "index": choices[0].index,
        "finish_reason": next(
            (c.finish_reason for c in reversed_choices if c.finish_reason),
            None,
        ),
        "message": message,
    }


def _reduce_chat(all_chunks: List[ChatCompletionChunk]) -> dict:
    choices_by_index: DefaultDict[int, List[Choice]] = defaultdict(list)
    for chunk in all_chunks:
        for choice in chunk.choices:
            choices_by_index[choice.index].append(choice)
    if all_chunks:
        d = all_chunks[-1].model_dump()
        d["choices"] = [
            _reduce_choices(choices) for choices in choices_by_index.values()
        ]
    else:
        d = {"choices": [{"message": {"role": "assistant", "content": ""}}]}
    return d


def _reduce_completions(all_chunks: List[Completion]) -> dict:
    all_content = []
    for chunk in all_chunks:
        content = chunk.choices[0].text
        if content is not None:
            all_content.append(content)
    content = "".join(all_content)
    if all_chunks:
        d = all_chunks[-1].model_dump()
        d["choices"] = [{"text": content}]
    else:
        d = {"choices": [{"text": content}]}

    return d


def _get_wrapper(
    original_create: Callable,
    name: str,
    reduce_fn: Callable,
    tracing_extra: Optional[TracingExtra] = None,
    invocation_params_fn: Optional[Callable] = None,
) -> Callable:
    textra = tracing_extra or {}

    @functools.wraps(original_create)
    def create(*args, stream: bool = False, **kwargs):
        decorator = run_helpers.traceable(
            name=name,
            run_type="llm",
            reduce_fn=reduce_fn if stream else None,
            process_inputs=_strip_not_given,
            _invocation_params_fn=invocation_params_fn,
            **textra,
        )

        return decorator(original_create)(*args, stream=stream, **kwargs)

    @functools.wraps(original_create)
    async def acreate(*args, stream: bool = False, **kwargs):
        kwargs = _strip_not_given(kwargs)
        decorator = run_helpers.traceable(
            name=name,
            run_type="llm",
            reduce_fn=reduce_fn if stream else None,
            process_inputs=_strip_not_given,
            _invocation_params_fn=invocation_params_fn,
            **textra,
        )
        if stream:
            # TODO: This slightly alters the output to be a generator instead of the
            # stream object. We can probably fix this with a bit of simple changes
            res = decorator(original_create)(*args, stream=stream, **kwargs)
            return res
        return await decorator(original_create)(*args, stream=stream, **kwargs)

    return acreate if run_helpers.is_async(original_create) else create


class TracingExtra(TypedDict, total=False):
    metadata: Optional[Mapping[str, Any]]
    tags: Optional[List[str]]
    client: Optional[ls_client.Client]


def wrap_openai(
    client: C,
    *,
    tracing_extra: Optional[TracingExtra] = None,
    chat_name: str = "ChatOpenAI",
    completions_name: str = "OpenAI",
) -> C:
    """Patch the OpenAI client to make it traceable.

    Args:
        client (Union[OpenAI, AsyncOpenAI]): The client to patch.
        tracing_extra (Optional[TracingExtra], optional): Extra tracing information.
            Defaults to None.
        chat_name (str, optional): The run name for the chat completions endpoint.
            Defaults to "ChatOpenAI".
        completions_name (str, optional): The run name for the completions endpoint.
            Defaults to "OpenAI".

    Returns:
        Union[OpenAI, AsyncOpenAI]: The patched client.

    """
    client.chat.completions.create = _get_wrapper(  # type: ignore[method-assign]
        client.chat.completions.create,
        chat_name,
        _reduce_chat,
        tracing_extra=tracing_extra,
        invocation_params_fn=functools.partial(_infer_invocation_params, "chat"),
    )
    client.completions.create = _get_wrapper(  # type: ignore[method-assign]
        client.completions.create,
        completions_name,
        _reduce_completions,
        tracing_extra=tracing_extra,
        invocation_params_fn=functools.partial(_infer_invocation_params, "text"),
    )
    return client
