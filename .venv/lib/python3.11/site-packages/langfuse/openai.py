"""If you use the OpenAI Python SDK, you can use the Langfuse drop-in replacement to get full logging by changing only the import.

```diff
- import openai
+ from langfuse.openai import openai
```

Langfuse automatically tracks:

- All prompts/completions with support for streaming, async and functions
- Latencies
- API Errors
- Model usage (tokens) and cost (USD)

The integration is fully interoperable with the `observe()` decorator and the low-level tracing SDK.

See docs for more details: https://langfuse.com/docs/integrations/openai
"""

import copy
import logging
import types
from typing import List, Optional

from packaging.version import Version
from wrapt import wrap_function_wrapper

from langfuse import Langfuse
from langfuse.client import StatefulGenerationClient
from langfuse.decorators import langfuse_context
from langfuse.utils import _get_timestamp
from langfuse.utils.langfuse_singleton import LangfuseSingleton

try:
    import openai
except ImportError:
    raise ModuleNotFoundError(
        "Please install OpenAI to use this feature: 'pip install openai'"
    )

try:
    from openai import AsyncAzureOpenAI, AsyncOpenAI, AzureOpenAI, OpenAI  # noqa: F401
except ImportError:
    AsyncAzureOpenAI = None
    AsyncOpenAI = None
    AzureOpenAI = None
    OpenAI = None

log = logging.getLogger("langfuse")


class OpenAiDefinition:
    module: str
    object: str
    method: str
    type: str
    sync: bool

    def __init__(self, module: str, object: str, method: str, type: str, sync: bool):
        self.module = module
        self.object = object
        self.method = method
        self.type = type
        self.sync = sync


OPENAI_METHODS_V0 = [
    OpenAiDefinition(
        module="openai",
        object="ChatCompletion",
        method="create",
        type="chat",
        sync=True,
    ),
    OpenAiDefinition(
        module="openai",
        object="Completion",
        method="create",
        type="completion",
        sync=True,
    ),
]


OPENAI_METHODS_V1 = [
    OpenAiDefinition(
        module="openai.resources.chat.completions",
        object="Completions",
        method="create",
        type="chat",
        sync=True,
    ),
    OpenAiDefinition(
        module="openai.resources.completions",
        object="Completions",
        method="create",
        type="completion",
        sync=True,
    ),
    OpenAiDefinition(
        module="openai.resources.chat.completions",
        object="AsyncCompletions",
        method="create",
        type="chat",
        sync=False,
    ),
    OpenAiDefinition(
        module="openai.resources.completions",
        object="AsyncCompletions",
        method="create",
        type="completion",
        sync=False,
    ),
]


class OpenAiArgsExtractor:
    def __init__(
        self,
        name=None,
        metadata=None,
        trace_id=None,
        session_id=None,
        user_id=None,
        tags=None,
        parent_observation_id=None,
        langfuse_prompt=None,  # we cannot use prompt because it's an argument of the old OpenAI completions API
        **kwargs,
    ):
        self.args = {}
        self.args["name"] = name
        self.args["metadata"] = metadata
        self.args["trace_id"] = trace_id
        self.args["session_id"] = session_id
        self.args["user_id"] = user_id
        self.args["tags"] = tags
        self.args["parent_observation_id"] = parent_observation_id
        self.args["langfuse_prompt"] = langfuse_prompt
        self.kwargs = kwargs

    def get_langfuse_args(self):
        return {**self.args, **self.kwargs}

    def get_openai_args(self):
        return self.kwargs


def _langfuse_wrapper(func):
    def _with_langfuse(open_ai_definitions, initialize):
        def wrapper(wrapped, instance, args, kwargs):
            return func(open_ai_definitions, initialize, wrapped, args, kwargs)

        return wrapper

    return _with_langfuse


def _extract_chat_prompt(kwargs: any):
    """Extracts the user input from prompts. Returns an array of messages or dict with messages and functions"""
    prompt = {}

    if kwargs.get("functions") is not None:
        prompt.update({"functions": kwargs["functions"]})

    if kwargs.get("function_call") is not None:
        prompt.update({"function_call": kwargs["function_call"]})

    if kwargs.get("tools") is not None:
        prompt.update({"tools": kwargs["tools"]})

    if prompt:
        # uf user provided functions, we need to send these together with messages to langfuse
        prompt.update(
            {
                "messages": _filter_image_data(kwargs.get("messages", [])),
            }
        )
        return prompt
    else:
        # vanilla case, only send messages in openai format to langfuse
        return _filter_image_data(kwargs.get("messages", []))


def _extract_chat_response(kwargs: any):
    """Extracts the llm output from the response."""
    response = {
        "role": kwargs.get("role", None),
    }

    if kwargs.get("function_call") is not None:
        response.update({"function_call": kwargs["function_call"]})

    if kwargs.get("tool_calls") is not None:
        response.update({"tool_calls": kwargs["tool_calls"]})

    response.update(
        {
            "content": kwargs.get("content", None),
        }
    )
    return response


def _get_langfuse_data_from_kwargs(
    resource: OpenAiDefinition, langfuse: Langfuse, start_time, kwargs
):
    name = kwargs.get("name", "OpenAI-generation")

    if name is None:
        name = "OpenAI-generation"

    if name is not None and not isinstance(name, str):
        raise TypeError("name must be a string")

    decorator_context_observation_id = langfuse_context.get_current_observation_id()
    decorator_context_trace_id = langfuse_context.get_current_trace_id()

    trace_id = kwargs.get("trace_id", None) or decorator_context_trace_id
    if trace_id is not None and not isinstance(trace_id, str):
        raise TypeError("trace_id must be a string")

    session_id = kwargs.get("session_id", None)
    if session_id is not None and not isinstance(session_id, str):
        raise TypeError("session_id must be a string")

    user_id = kwargs.get("user_id", None)
    if user_id is not None and not isinstance(user_id, str):
        raise TypeError("user_id must be a string")

    tags = kwargs.get("tags", None)
    if tags is not None and (
        not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags)
    ):
        raise TypeError("tags must be a list of strings")

    # Update trace params in decorator context if specified in openai call
    if decorator_context_trace_id:
        langfuse_context.update_current_trace(
            session_id=session_id, user_id=user_id, tags=tags
        )

    parent_observation_id = kwargs.get("parent_observation_id", None) or (
        decorator_context_observation_id
        if decorator_context_observation_id != decorator_context_trace_id
        else None
    )
    if parent_observation_id is not None and not isinstance(parent_observation_id, str):
        raise TypeError("parent_observation_id must be a string")
    if parent_observation_id is not None and trace_id is None:
        raise ValueError("parent_observation_id requires trace_id to be set")

    metadata = kwargs.get("metadata", {})

    if metadata is not None and not isinstance(metadata, dict):
        raise TypeError("metadata must be a dictionary")

    model = kwargs.get("model", None) or None

    prompt = None

    if resource.type == "completion":
        prompt = kwargs.get("prompt", None)
    elif resource.type == "chat":
        prompt = _extract_chat_prompt(kwargs)

    is_nested_trace = False
    if trace_id:
        is_nested_trace = True
        langfuse.trace(id=trace_id, session_id=session_id, user_id=user_id, tags=tags)
    else:
        trace_id = (
            decorator_context_trace_id
            or langfuse.trace(
                session_id=session_id,
                user_id=user_id,
                tags=tags,
                name=name,
                input=prompt,
                metadata=metadata,
            ).id
        )

    modelParameters = {
        "temperature": kwargs.get("temperature", 1),
        "max_tokens": kwargs.get("max_tokens", float("inf")),  # casing?
        "top_p": kwargs.get("top_p", 1),
        "frequency_penalty": kwargs.get("frequency_penalty", 0),
        "presence_penalty": kwargs.get("presence_penalty", 0),
    }

    langfuse_prompt = kwargs.get("langfuse_prompt", None)

    return {
        "name": name,
        "metadata": metadata,
        "trace_id": trace_id,
        "parent_observation_id": parent_observation_id,
        "user_id": user_id,
        "start_time": start_time,
        "input": prompt,
        "model_parameters": modelParameters,
        "model": model or None,
        "prompt": langfuse_prompt,
    }, is_nested_trace


def _create_langfuse_update(
    completion, generation: StatefulGenerationClient, completion_start_time, model=None
):
    update = {
        "end_time": _get_timestamp(),
        "output": completion,
        "completion_start_time": completion_start_time,
    }
    if model is not None:
        update["model"] = model

    generation.update(**update)


def _extract_openai_response(resource, responses):
    completion = [] if resource.type == "chat" else ""
    model = None
    completion_start_time = None

    for index, i in enumerate(responses):
        if index == 0:
            completion_start_time = _get_timestamp()

        if _is_openai_v1():
            i = i.__dict__

        model = model or i.get("model", None) or None

        choices = i.get("choices", [])

        for choice in choices:
            if _is_openai_v1():
                choice = choice.__dict__
            if resource.type == "chat":
                delta = choice.get("delta", None)

                if _is_openai_v1():
                    delta = delta.__dict__

                if delta.get("role", None) is not None:
                    completion.append(
                        {
                            "role": delta.get("role", None),
                            "function_call": None,
                            "tool_calls": None,
                            "content": None,
                        }
                    )

                elif delta.get("content", None) is not None:
                    completion[-1]["content"] = (
                        delta.get("content", None)
                        if completion[-1]["content"] is None
                        else completion[-1]["content"] + delta.get("content", None)
                    )

                elif delta.get("function_call", None) is not None:
                    completion[-1]["function_call"] = (
                        delta.get("function_call", None)
                        if completion[-1]["function_call"] is None
                        else completion[-1]["function_call"]
                        + delta.get("function_call", None)
                    )
                elif delta.get("tools_call", None) is not None:
                    completion[-1]["tool_calls"] = (
                        delta.get("tools_call", None)
                        if completion[-1]["tool_calls"] is None
                        else completion[-1]["tool_calls"]
                        + delta.get("tools_call", None)
                    )
            if resource.type == "completion":
                completion += choice.get("text", None)

    def get_response_for_chat():
        if len(completion) > 0:
            if completion[-1].get("content", None) is not None:
                return completion[-1]["content"]
            elif completion[-1].get("function_call", None) is not None:
                return completion[-1]["function_call"]
            elif completion[-1].get("tool_calls", None) is not None:
                return completion[-1]["tool_calls"]
        return None

    return (
        model,
        completion_start_time,
        get_response_for_chat() if resource.type == "chat" else completion,
    )


def _get_langfuse_data_from_default_response(resource: OpenAiDefinition, response):
    model = response.get("model", None) or None

    completion = None
    if resource.type == "completion":
        choices = response.get("choices", [])
        if len(choices) > 0:
            choice = choices[-1]

            completion = choice.text if _is_openai_v1() else choice.get("text", None)
    elif resource.type == "chat":
        choices = response.get("choices", [])
        if len(choices) > 0:
            choice = choices[-1]
            completion = (
                _extract_chat_response(choice.message.__dict__)
                if _is_openai_v1()
                else choice.get("message", None)
            )

    usage = response.get("usage", None)

    return model, completion, usage.__dict__ if _is_openai_v1() else usage


def _is_openai_v1():
    return Version(openai.__version__) >= Version("1.0.0")


def _is_streaming_response(response):
    return (
        isinstance(response, types.GeneratorType)
        or isinstance(response, types.AsyncGeneratorType)
        or (_is_openai_v1() and isinstance(response, openai.Stream))
        or (_is_openai_v1() and isinstance(response, openai.AsyncStream))
    )


@_langfuse_wrapper
def _wrap(open_ai_resource: OpenAiDefinition, initialize, wrapped, args, kwargs):
    new_langfuse: Langfuse = initialize()

    start_time = _get_timestamp()
    arg_extractor = OpenAiArgsExtractor(*args, **kwargs)

    generation, is_nested_trace = _get_langfuse_data_from_kwargs(
        open_ai_resource, new_langfuse, start_time, arg_extractor.get_langfuse_args()
    )
    generation = new_langfuse.generation(**generation)
    try:
        openai_response = wrapped(**arg_extractor.get_openai_args())

        if _is_streaming_response(openai_response):
            return LangfuseResponseGeneratorSync(
                resource=open_ai_resource,
                response=openai_response,
                generation=generation,
                langfuse=new_langfuse,
                is_nested_trace=is_nested_trace,
            )

        else:
            model, completion, usage = _get_langfuse_data_from_default_response(
                open_ai_resource,
                openai_response.__dict__ if _is_openai_v1() else openai_response,
            )
            generation.update(
                model=model, output=completion, end_time=_get_timestamp(), usage=usage
            )

            # Avoiding the trace-update if trace-id is provided by user.
            if not is_nested_trace:
                new_langfuse.trace(id=generation.trace_id, output=completion)

        return openai_response
    except Exception as ex:
        log.warning(ex)
        model = kwargs.get("model", None) or None
        generation.update(
            end_time=_get_timestamp(),
            status_message=str(ex),
            level="ERROR",
            model=model,
            usage={"input_cost": 0, "output_cost": 0, "total_cost": 0},
        )
        raise ex


@_langfuse_wrapper
async def _wrap_async(
    open_ai_resource: OpenAiDefinition, initialize, wrapped, args, kwargs
):
    new_langfuse = initialize()
    start_time = _get_timestamp()
    arg_extractor = OpenAiArgsExtractor(*args, **kwargs)

    generation, is_nested_trace = _get_langfuse_data_from_kwargs(
        open_ai_resource, new_langfuse, start_time, arg_extractor.get_langfuse_args()
    )
    generation = new_langfuse.generation(**generation)
    try:
        openai_response = await wrapped(**arg_extractor.get_openai_args())

        if _is_streaming_response(openai_response):
            return LangfuseResponseGeneratorAsync(
                resource=open_ai_resource,
                response=openai_response,
                generation=generation,
                langfuse=new_langfuse,
                is_nested_trace=is_nested_trace,
            )

        else:
            model, completion, usage = _get_langfuse_data_from_default_response(
                open_ai_resource,
                openai_response.__dict__ if _is_openai_v1() else openai_response,
            )
            generation.update(
                model=model,
                output=completion,
                end_time=_get_timestamp(),
                usage=usage,
            )
            # Avoiding the trace-update if trace-id is provided by user.
            if not is_nested_trace:
                new_langfuse.trace(id=generation.trace_id, output=completion)

        return openai_response
    except Exception as ex:
        model = kwargs.get("model", None) or None
        generation.update(
            end_time=_get_timestamp(),
            status_message=str(ex),
            level="ERROR",
            model=model,
            usage={"input_cost": 0, "output_cost": 0, "total_cost": 0},
        )
        raise ex


class OpenAILangfuse:
    _langfuse: Optional[Langfuse] = None

    def initialize(self):
        self._langfuse = LangfuseSingleton().get(
            public_key=openai.langfuse_public_key,
            secret_key=openai.langfuse_secret_key,
            host=openai.langfuse_host,
            debug=openai.langfuse_debug,
            enabled=openai.langfuse_enabled,
            sdk_integration="openai",
        )

        return self._langfuse

    def flush(cls):
        cls._langfuse.flush()

    def register_tracing(self):
        resources = OPENAI_METHODS_V1 if _is_openai_v1() else OPENAI_METHODS_V0

        for resource in resources:
            wrap_function_wrapper(
                resource.module,
                f"{resource.object}.{resource.method}",
                _wrap(resource, self.initialize)
                if resource.sync
                else _wrap_async(resource, self.initialize),
            )

        setattr(openai, "langfuse_public_key", None)
        setattr(openai, "langfuse_secret_key", None)
        setattr(openai, "langfuse_host", None)
        setattr(openai, "langfuse_debug", None)
        setattr(openai, "langfuse_enabled", True)
        setattr(openai, "flush_langfuse", self.flush)


modifier = OpenAILangfuse()
modifier.register_tracing()


def auth_check():
    if modifier._langfuse is None:
        modifier.initialize()

    return modifier._langfuse.auth_check()


def _filter_image_data(messages: List[dict]):
    """https://platform.openai.com/docs/guides/vision?lang=python

    The messages array remains the same, but the 'image_url' is removed from the 'content' array.
    It should only be removed if the value starts with 'data:image/jpeg;base64,'

    """
    output_messages = copy.deepcopy(messages)

    for message in output_messages:
        if message.get("content", None) is not None:
            content = message["content"]
            for index, item in enumerate(content):
                if isinstance(item, dict) and item.get("image_url", None) is not None:
                    url = item["image_url"]["url"]
                    if url.startswith("data:image/"):
                        del content[index]["image_url"]

    return output_messages


class LangfuseResponseGeneratorSync:
    def __init__(
        self,
        *,
        resource,
        response,
        generation,
        langfuse,
        is_nested_trace,
    ):
        self.items = []

        self.resource = resource
        self.response = response
        self.generation = generation
        self.langfuse = langfuse
        self.is_nested_trace = is_nested_trace

    def __iter__(self):
        try:
            for i in self.response:
                self.items.append(i)

                yield i
        finally:
            self._finalize()

    def __enter__(self):
        return self.__iter__()

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def _finalize(self):
        model, completion_start_time, completion = _extract_openai_response(
            self.resource, self.items
        )

        # Avoiding the trace-update if trace-id is provided by user.
        if not self.is_nested_trace:
            self.langfuse.trace(id=self.generation.trace_id, output=completion)

        _create_langfuse_update(
            completion, self.generation, completion_start_time, model=model
        )


class LangfuseResponseGeneratorAsync:
    def __init__(
        self,
        *,
        resource,
        response,
        generation,
        langfuse,
        is_nested_trace,
    ):
        self.items = []

        self.resource = resource
        self.response = response
        self.generation = generation
        self.langfuse = langfuse
        self.is_nested_trace = is_nested_trace

    async def __aiter__(self):
        try:
            async for i in self.response:
                self.items.append(i)

                yield i
        finally:
            await self._finalize()

    async def __aenter__(self):
        return self.__aiter__()

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass

    async def _finalize(self):
        model, completion_start_time, completion = _extract_openai_response(
            self.resource, self.items
        )

        # Avoiding the trace-update if trace-id is provided by user.
        if not self.is_nested_trace:
            self.langfuse.trace(id=self.generation.trace_id, output=completion)

        _create_langfuse_update(
            completion, self.generation, completion_start_time, model=model
        )
