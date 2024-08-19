import asyncio
from collections import defaultdict
from contextvars import ContextVar
from datetime import datetime
from functools import wraps
import httpx
import inspect
import json
import logging
from typing import (
    Any,
    Callable,
    DefaultDict,
    List,
    Optional,
    Union,
    Literal,
    Dict,
    Tuple,
    Iterable,
    AsyncGenerator,
    Generator,
    TypeVar,
    cast,
)

from langfuse.client import (
    Langfuse,
    StatefulSpanClient,
    StatefulTraceClient,
    StatefulGenerationClient,
    PromptClient,
    ModelUsage,
    MapValue,
)
from langfuse.serializer import EventSerializer
from langfuse.types import ObservationParams, SpanLevel
from langfuse.utils import _get_timestamp
from langfuse.utils.langfuse_singleton import LangfuseSingleton
from langfuse.utils.error_logging import catch_and_log_errors

from pydantic import BaseModel

_observation_stack_context: ContextVar[
    List[Union[StatefulTraceClient, StatefulSpanClient, StatefulGenerationClient]]
] = ContextVar("observation_stack_context", default=[])
_observation_params_context: ContextVar[DefaultDict[str, ObservationParams]] = (
    ContextVar(
        "observation_params_context",
        default=defaultdict(
            lambda: {
                "name": None,
                "user_id": None,
                "session_id": None,
                "version": None,
                "release": None,
                "metadata": None,
                "tags": None,
                "input": None,
                "output": None,
                "level": None,
                "status_message": None,
                "start_time": None,
                "end_time": None,
                "completion_start_time": None,
                "model": None,
                "model_parameters": None,
                "usage": None,
                "prompt": None,
                "public": None,
            },
        ),
    )
)
_root_trace_id_context: ContextVar[Optional[str]] = ContextVar(
    "root_trace_id_context", default=None
)

# For users with mypy type checking, we need to define a TypeVar for the decorated function
# Otherwise, mypy will infer the return type of the decorated function as Any
# Docs: https://mypy.readthedocs.io/en/stable/generics.html#declaring-decorators
F = TypeVar("F", bound=Callable[..., Any])


class LangfuseDecorator:
    _log = logging.getLogger("langfuse")

    def observe(
        self,
        *,
        name: Optional[str] = None,
        as_type: Optional[Literal["generation"]] = None,
        capture_input: bool = True,
        capture_output: bool = True,
        transform_to_string: Optional[Callable[[Iterable], str]] = None,
    ) -> Callable[[F], F]:
        """Wrap a function to create and manage Langfuse tracing around its execution, supporting both synchronous and asynchronous functions.

        It captures the function's execution context, including start/end times, input/output data, and automatically handles trace/span generation within the Langfuse observation context.
        In case of an exception, the observation is updated with error details. The top-most decorated function is treated as a trace, with nested calls captured as spans or generations.

        Attributes:
            name (Optional[str]): Name of the created trace or span. Overwrites the function name as the default used for the trace or span name.
            as_type (Optional[Literal["generation"]]): Specify "generation" to treat the observation as a generation type, suitable for language model invocations.
            capture_input (bool): If True, captures the args and kwargs of the function as input. Default is True.
            capture_output (bool): If True, captures the return value of the function as output. Default is True.
            transform_to_string (Optional[Callable[[Iterable], str]]): When the decorated function returns a generator, this function transforms yielded values into a string representation for output capture

        Returns:
            Callable: A wrapped version of the original function that, upon execution, is automatically observed and managed by Langfuse.

        Example:
            For general tracing (functions/methods):
            ```python
            @observe()
            def your_function(args):
                # Your implementation here
            ```
            For observing language model generations:
            ```python
            @observe(as_type="generation")
            def your_LLM_function(args):
                # Your LLM invocation here
            ```

        Raises:
            Exception: Propagates exceptions from the wrapped function after logging and updating the observation with error details.

        Note:
        - Automatic observation ID and context management is provided. Optionally, an observation ID can be specified using the `langfuse_observation_id` keyword when calling the wrapped function.
        - To update observation or trace parameters (e.g., metadata, session_id), use `langfuse.update_current_observation` and `langfuse.update_current_trace` methods within the wrapped function.
        """

        def decorator(func: F) -> F:
            return (
                self._async_observe(
                    func,
                    name=name,
                    as_type=as_type,
                    capture_input=capture_input,
                    capture_output=capture_output,
                    transform_to_string=transform_to_string,
                )
                if asyncio.iscoroutinefunction(func)
                else self._sync_observe(
                    func,
                    name=name,
                    as_type=as_type,
                    capture_input=capture_input,
                    capture_output=capture_output,
                    transform_to_string=transform_to_string,
                )
            )

        return decorator

    def _async_observe(
        self,
        func: F,
        *,
        name: Optional[str],
        as_type: Optional[Literal["generation"]],
        capture_input: bool,
        capture_output: bool,
        transform_to_string: Optional[Callable[[Iterable], str]] = None,
    ) -> F:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            observation = self._prepare_call(
                name=name or func.__name__,
                as_type=as_type,
                capture_input=capture_input,
                is_method=self._is_method(func),
                func_args=args,
                func_kwargs=kwargs,
            )
            result = None

            try:
                result = await func(*args, **kwargs)
            except Exception as e:
                self._handle_exception(observation, e)
            finally:
                result = self._finalize_call(
                    observation, result, capture_output, transform_to_string
                )

                # Returning from finally block may swallow errors, so only return if result is not None
                if result is not None:
                    return result

        return cast(F, async_wrapper)

    def _sync_observe(
        self,
        func: F,
        *,
        name: Optional[str],
        as_type: Optional[Literal["generation"]],
        capture_input: bool,
        capture_output: bool,
        transform_to_string: Optional[Callable[[Iterable], str]] = None,
    ) -> F:
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            observation = self._prepare_call(
                name=name or func.__name__,
                as_type=as_type,
                capture_input=capture_input,
                is_method=self._is_method(func),
                func_args=args,
                func_kwargs=kwargs,
            )
            result = None

            try:
                result = func(*args, **kwargs)
            except Exception as e:
                self._handle_exception(observation, e)
            finally:
                result = self._finalize_call(
                    observation, result, capture_output, transform_to_string
                )

                # Returning from finally block may swallow errors, so only return if result is not None
                if result is not None:
                    return result

        return cast(F, sync_wrapper)

    @staticmethod
    def _is_method(func: Callable) -> bool:
        """Check if a callable is likely an class or instance method based on its signature.

        This method inspects the given callable's signature for the presence of a 'cls' or 'self' parameter, which is conventionally used for class and instance methods in Python classes. It returns True if 'class' or 'self' is found among the parameters, suggesting the callable is a method.

        Note: This method relies on naming conventions and may not accurately identify instance methods if unconventional parameter names are used or if static or class methods incorrectly include a 'self' or 'cls' parameter. Additionally, during decorator execution, inspect.ismethod does not work as expected because the function has not yet been bound to an instance; it is still a function, not a method. This check attempts to infer method status based on signature, which can be useful in decorator contexts where traditional method identification techniques fail.

        Returns:
        bool: True if 'cls' or 'self' is in the callable's parameters, False otherwise.
        """
        return (
            "self" in inspect.signature(func).parameters
            or "cls" in inspect.signature(func).parameters
        )

    def _prepare_call(
        self,
        *,
        name: str,
        as_type: Optional[Literal["generation"]],
        capture_input: bool,
        is_method: bool = False,
        func_args: Tuple = (),
        func_kwargs: Dict = {},
    ) -> Optional[
        Union[StatefulSpanClient, StatefulTraceClient, StatefulGenerationClient]
    ]:
        try:
            langfuse = self._get_langfuse()
            stack = _observation_stack_context.get().copy()
            parent = stack[-1] if stack else None

            # Collect default observation data
            observation_id = func_kwargs.pop("langfuse_observation_id", None)
            id = str(observation_id) if observation_id else None
            start_time = _get_timestamp()

            input = (
                self._get_input_from_func_args(
                    is_method=is_method,
                    func_args=func_args,
                    func_kwargs=func_kwargs,
                )
                if capture_input
                else None
            )

            params = {
                "id": id,
                "name": name,
                "start_time": start_time,
                "input": input,
            }

            # Create observation
            if parent and as_type == "generation":
                observation = parent.generation(**params)
            elif as_type == "generation":
                # Create wrapper trace if generation is top-level
                # Do not add wrapper trace to stack, as it does not have a corresponding end that will pop it off again
                trace = langfuse.trace(id=id, name=name, start_time=start_time)
                observation = langfuse.generation(
                    name=name, start_time=start_time, input=input, trace_id=trace.id
                )
            elif parent:
                observation = parent.span(**params)
            else:
                params["id"] = self._get_context_trace_id() or params["id"]
                observation = langfuse.trace(**params)

            _observation_stack_context.set(stack + [observation])

            return observation
        except Exception as e:
            self._log.error(f"Failed to prepare observation: {e}")

    def _get_input_from_func_args(
        self,
        *,
        is_method: bool = False,
        func_args: Tuple = (),
        func_kwargs: Dict = {},
    ) -> Any:
        # Remove implicitly passed "self" or "cls" argument for instance or class methods
        logged_args = func_args[1:] if is_method else func_args
        raw_input = {
            "args": logged_args,
            "kwargs": func_kwargs,
        }

        # Serialize and deserialize to ensure proper JSON serialization.
        # Objects are later serialized again so deserialization is necessary here to avoid unnecessary escaping of quotes.
        return json.loads(json.dumps(raw_input, cls=EventSerializer))

    def _get_context_trace_id(self):
        context_trace_id = _root_trace_id_context.get()

        if context_trace_id is not None:
            # Clear the context trace ID to avoid leaking it to other traces
            _root_trace_id_context.set(None)

            return context_trace_id

        return None

    def _finalize_call(
        self,
        observation: Optional[
            Union[
                StatefulSpanClient,
                StatefulTraceClient,
                StatefulGenerationClient,
            ]
        ],
        result: Any,
        capture_output: bool,
        transform_to_string: Optional[Callable[[Iterable], str]] = None,
    ):
        if inspect.isgenerator(result):
            return self._wrap_sync_generator_result(
                observation, result, capture_output, transform_to_string
            )
        elif inspect.isasyncgen(result):
            return self._wrap_async_generator_result(
                observation, result, capture_output, transform_to_string
            )

        else:
            return self._handle_call_result(observation, result, capture_output)

    def _handle_call_result(
        self,
        observation: Optional[
            Union[
                StatefulSpanClient,
                StatefulTraceClient,
                StatefulGenerationClient,
            ]
        ],
        result: Any,
        capture_output: bool,
    ):
        try:
            if observation is None:
                raise ValueError("No observation found in the current context")

            # Collect final observation data
            observation_params = _observation_params_context.get()[
                observation.id
            ].copy()
            del _observation_params_context.get()[
                observation.id
            ]  # Remove observation params to avoid leaking

            end_time = observation_params["end_time"] or _get_timestamp()
            raw_output = observation_params["output"] or (
                result if result and capture_output else None
            )

            # Serialize and deserialize to ensure proper JSON serialization.
            # Objects are later serialized again so deserialization is necessary here to avoid unnecessary escaping of quotes.
            output = json.loads(json.dumps(raw_output, cls=EventSerializer))
            observation_params.update(end_time=end_time, output=output)

            if isinstance(observation, (StatefulSpanClient, StatefulGenerationClient)):
                observation.end(**observation_params)
            elif isinstance(observation, StatefulTraceClient):
                observation.update(**observation_params)

            # Remove observation from top of stack
            stack = _observation_stack_context.get()
            _observation_stack_context.set(stack[:-1])

        except Exception as e:
            self._log.error(f"Failed to finalize observation: {e}")

        finally:
            return result

    def _handle_exception(
        self,
        observation: Optional[
            Union[StatefulSpanClient, StatefulTraceClient, StatefulGenerationClient]
        ],
        e: Exception,
    ):
        if observation:
            _observation_params_context.get()[observation.id].update(
                level="ERROR", status_message=str(e)
            )
        raise e

    def _wrap_sync_generator_result(
        self,
        observation: Optional[
            Union[
                StatefulSpanClient,
                StatefulTraceClient,
                StatefulGenerationClient,
            ]
        ],
        generator: Generator,
        capture_output: bool,
        transform_to_string: Optional[Callable[[Iterable], str]] = None,
    ):
        items = []

        try:
            for item in generator:
                items.append(item)

                yield item

        finally:
            output = items

            if transform_to_string is not None:
                output = transform_to_string(items)

            elif all(isinstance(item, str) for item in items):
                output = "".join(items)

            self._handle_call_result(observation, output, capture_output)

    async def _wrap_async_generator_result(
        self,
        observation: Optional[
            Union[
                StatefulSpanClient,
                StatefulTraceClient,
                StatefulGenerationClient,
            ]
        ],
        generator: AsyncGenerator,
        capture_output: bool,
        transform_to_string: Optional[Callable[[Iterable], str]] = None,
    ) -> AsyncGenerator:
        items = []

        try:
            async for item in generator:
                items.append(item)

                yield item

        finally:
            output = items

            if transform_to_string is not None:
                output = transform_to_string(items)

            elif all(isinstance(item, str) for item in items):
                output = "".join(items)

            self._handle_call_result(observation, output, capture_output)

    def get_current_llama_index_handler(self):
        """Retrieve the current LlamaIndexCallbackHandler associated with the most recent observation in the observation stack.

        This method fetches the current observation from the observation stack and returns a LlamaIndexCallbackHandler initialized with this observation.
        It is intended to be used within the context of a trace, allowing access to a callback handler for operations that require interaction with the LlamaIndex API based on the current observation context.

        See the Langfuse documentation for more information on integrating the LlamaIndexCallbackHandler.

        Returns:
            LlamaIndexCallbackHandler or None: Returns a LlamaIndexCallbackHandler instance if there is an active observation in the current context; otherwise, returns None if no observation is found.

        Note:
            - This method should be called within the context of a trace (i.e., within a function wrapped by @observe) to ensure that an observation context exists.
            - If no observation is found in the current context (e.g., if called outside of a trace or if the observation stack is empty), the method logs a warning and returns None.
        """
        try:
            from langfuse.llama_index import LlamaIndexCallbackHandler
        except ImportError:
            self._log.error(
                "LlamaIndexCallbackHandler is not available, most likely because llama-index is not installed. pip install llama-index"
            )

            return None

        observation = _observation_stack_context.get()[-1]

        if observation is None:
            self._log.warn("No observation found in the current context")

            return None

        if isinstance(observation, StatefulGenerationClient):
            self._log.warn(
                "Current observation is of type GENERATION, LlamaIndex handler is not supported for this type of observation"
            )

            return None

        callback_handler = LlamaIndexCallbackHandler()
        callback_handler.set_root(observation)

        return callback_handler

    def get_current_langchain_handler(self):
        """Retrieve the current LangchainCallbackHandler associated with the most recent observation in the observation stack.

        This method fetches the current observation from the observation stack and returns a LangchainCallbackHandler initialized with this observation.
        It is intended to be used within the context of a trace, allowing access to a callback handler for operations that require interaction with Langchain based on the current observation context.

        See the Langfuse documentation for more information on integrating the LangchainCallbackHandler.

        Returns:
            LangchainCallbackHandler or None: Returns a LangchainCallbackHandler instance if there is an active observation in the current context; otherwise, returns None if no observation is found.

        Note:
            - This method should be called within the context of a trace (i.e., within a function wrapped by @observe) to ensure that an observation context exists.
            - If no observation is found in the current context (e.g., if called outside of a trace or if the observation stack is empty), the method logs a warning and returns None.
        """
        observation = _observation_stack_context.get()[-1]

        if observation is None:
            self._log.warn("No observation found in the current context")

            return None

        if isinstance(observation, StatefulGenerationClient):
            self._log.warn(
                "Current observation is of type GENERATION, Langchain handler is not supported for this type of observation"
            )

            return None

        return observation.get_langchain_handler()

    def get_current_trace_id(self):
        """Retrieve the ID of the current trace from the observation stack context.

        This method examines the observation stack to find the root trace and returns its ID. It is useful for operations that require the trace ID,
        such as setting trace parameters or querying trace information. The trace ID is typically the ID of the first observation in the stack,
        representing the entry point of the traced execution context.

        Returns:
            str or None: The ID of the current trace if available; otherwise, None. A return value of None indicates that there is no active trace in the current context,
            possibly due to the method being called outside of any @observe-decorated function execution.

        Note:
            - This method should be called within the context of a trace (i.e., inside a function wrapped with the @observe decorator) to ensure that a current trace is indeed present and its ID can be retrieved.
            - If called outside of a trace context, or if the observation stack has somehow been corrupted or improperly managed, this method will log a warning and return None, indicating the absence of a traceable context.
        """
        stack = _observation_stack_context.get()
        should_log_warning = self._get_caller_module_name() != "langfuse.openai"

        if not stack:
            if should_log_warning:
                self._log.warn("No trace found in the current context")

            return None

        return stack[0].id

    def _get_caller_module_name(self):
        try:
            caller_module = inspect.getmodule(inspect.stack()[2][0])
        except Exception as e:
            self._log.warn(f"Failed to get caller module: {e}")

            return None

        return caller_module.__name__ if caller_module else None

    def get_current_trace_url(self) -> Optional[str]:
        """Retrieve the URL of the current trace in context.

        Returns:
            str or None: The URL of the current trace if available; otherwise, None. A return value of None indicates that there is no active trace in the current context,
            possibly due to the method being called outside of any @observe-decorated function execution.

        Note:
            - This method should be called within the context of a trace (i.e., inside a function wrapped with the @observe decorator) to ensure that a current trace is indeed present and its ID can be retrieved.
            - If called outside of a trace context, or if the observation stack has somehow been corrupted or improperly managed, this method will log a warning and return None, indicating the absence of a traceable context.
        """
        try:
            trace_id = self.get_current_trace_id()
            langfuse = self._get_langfuse()

            if not trace_id:
                raise ValueError("No trace found in the current context")

            return f"{langfuse.client._client_wrapper._base_url}/trace/{trace_id}"

        except Exception as e:
            self._log.error(f"Failed to get current trace URL: {e}")

            return None

    def get_current_observation_id(self):
        """Retrieve the ID of the current observation in context.

        Returns:
            str or None: The ID of the current observation if available; otherwise, None. A return value of None indicates that there is no active trace or observation in the current context,
            possibly due to the method being called outside of any @observe-decorated function execution.

        Note:
            - This method should be called within the context of a trace or observation (i.e., inside a function wrapped with the @observe decorator) to ensure that a current observation is indeed present and its ID can be retrieved.
            - If called outside of a trace or observation context, or if the observation stack has somehow been corrupted or improperly managed, this method will log a warning and return None, indicating the absence of a traceable context.
            - If called at the top level of a trace, it will return the trace ID.
        """
        stack = _observation_stack_context.get()
        should_log_warning = self._get_caller_module_name() != "langfuse.openai"

        if not stack:
            if should_log_warning:
                self._log.warn("No observation found in the current context")

            return None

        return stack[-1].id

    def update_current_trace(
        self,
        name: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        version: Optional[str] = None,
        release: Optional[str] = None,
        metadata: Optional[Any] = None,
        tags: Optional[List[str]] = None,
        public: Optional[bool] = None,
    ):
        """Set parameters for the current trace, updating the trace's metadata and context information.

        This method allows for dynamically updating the trace parameters at any point during the execution of a trace.
        It updates the parameters of the current trace based on the provided arguments. These parameters include metadata, session information,
        and other trace attributes that can be useful for categorization, filtering, and analysis in the Langfuse UI.

        Arguments:
            name (Optional[str]): Identifier of the trace. Useful for sorting/filtering in the UI..
            user_id (Optional[str]): The id of the user that triggered the execution. Used to provide user-level analytics.
            session_id (Optional[str]): Used to group multiple traces into a session in Langfuse. Use your own session/thread identifier.
            version (Optional[str]): The version of the trace type. Used to understand how changes to the trace type affect metrics. Useful in debugging.
            release (Optional[str]): The release identifier of the current deployment. Used to understand how changes of different deployments affect metrics. Useful in debugging.
            metadata (Optional[Any]): Additional metadata of the trace. Can be any JSON object. Metadata is merged when being updated via the API.
            tags (Optional[List[str]]): Tags are used to categorize or label traces. Traces can be filtered by tags in the Langfuse UI and GET API.

        Returns:
            None

        Note:
            - This method should be used within the context of an active trace, typically within a function that is being traced using the @observe decorator.
            - The method updates the trace parameters for the currently executing trace. In nested trace scenarios, it affects the most recent trace context.
            - If called outside of an active trace context, a warning is logged, and a ValueError is raised to indicate the absence of a traceable context.
        """
        trace_id = self.get_current_trace_id()

        if trace_id is None:
            self._log.warn("No trace found in the current context")

            return

        params_to_update = {
            k: v
            for k, v in {
                "name": name,
                "user_id": user_id,
                "session_id": session_id,
                "version": version,
                "release": release,
                "metadata": metadata,
                "tags": tags,
                "public": public,
            }.items()
            if v is not None
        }

        _observation_params_context.get()[trace_id].update(params_to_update)

    def update_current_observation(
        self,
        *,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        name: Optional[str] = None,
        version: Optional[str] = None,
        metadata: Optional[Any] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        release: Optional[str] = None,
        tags: Optional[List[str]] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        level: Optional[SpanLevel] = None,
        status_message: Optional[str] = None,
        completion_start_time: Optional[datetime] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, MapValue]] = None,
        usage: Optional[Union[BaseModel, ModelUsage]] = None,
        prompt: Optional[PromptClient] = None,
        public: Optional[bool] = None,
    ):
        """Update parameters for the current observation within an active trace context.

        This method dynamically adjusts the parameters of the most recent observation on the observation stack.
        It allows for the enrichment of observation data with additional details such as input parameters, output results, metadata, and more,
        enhancing the observability and traceability of the execution context.

        Note that if a param is not available on a specific observation type, it will be ignored.

        Shared params:
            - `input` (Optional[Any]): The input parameters of the trace or observation, providing context about the observed operation or function call.
            - `output` (Optional[Any]): The output or result of the trace or observation
            - `name` (Optional[str]): Identifier of the trace or observation. Useful for sorting/filtering in the UI.
            - `metadata` (Optional[Any]): Additional metadata of the trace. Can be any JSON object. Metadata is merged when being updated via the API.
            - `start_time` (Optional[datetime]): The start time of the observation, allowing for custom time range specification.
            - `end_time` (Optional[datetime]): The end time of the observation, enabling precise control over the observation duration.
            - `version` (Optional[str]): The version of the trace type. Used to understand how changes to the trace type affect metrics. Useful in debugging.

        Trace-specific params:
            - `user_id` (Optional[str]): The id of the user that triggered the execution. Used to provide user-level analytics.
            - `session_id` (Optional[str]): Used to group multiple traces into a session in Langfuse. Use your own session/thread identifier.
            - `release` (Optional[str]): The release identifier of the current deployment. Used to understand how changes of different deployments affect metrics. Useful in debugging.
            - `tags` (Optional[List[str]]): Tags are used to categorize or label traces. Traces can be filtered by tags in the Langfuse UI and GET API.
            - `public` (Optional[bool]): You can make a trace public to share it via a public link. This allows others to view the trace without needing to log in or be members of your Langfuse project.

        Span-specific params:
            - `level` (Optional[SpanLevel]): The severity or importance level of the observation, such as "INFO", "WARNING", or "ERROR".
            - `status_message` (Optional[str]): A message or description associated with the observation's status, particularly useful for error reporting.

        Generation-specific params:
            - `completion_start_time` (Optional[datetime]): The time at which the completion started (streaming). Set it to get latency analytics broken down into time until completion started and completion duration.
            - `model_parameters` (Optional[Dict[str, MapValue]]): The parameters of the model used for the generation; can be any key-value pairs.
            - `usage` (Optional[Union[BaseModel, ModelUsage]]): The usage object supports the OpenAi structure with {promptTokens, completionTokens, totalTokens} and a more generic version {input, output, total, unit, inputCost, outputCost, totalCost} where unit can be of value "TOKENS", "CHARACTERS", "MILLISECONDS", "SECONDS", or "IMAGES". Refer to the docs on how to automatically infer token usage and costs in Langfuse.
            - `prompt`(Optional[PromptClient]): The prompt object used for the generation.

        Returns:
            None

        Raises:
            ValueError: If no current observation is found in the context, indicating that this method was called outside of an observation's execution scope.

        Note:
            - This method is intended to be used within the context of an active observation, typically within a function wrapped by the @observe decorator.
            - It updates the parameters of the most recently created observation on the observation stack. Care should be taken in nested observation contexts to ensure the updates are applied as intended.
            - Parameters set to `None` will not overwrite existing values for those parameters. This behavior allows for selective updates without clearing previously set information.
        """
        stack = _observation_stack_context.get()
        observation = stack[-1] if stack else None

        if not observation:
            self._log.warn("No observation found in the current context")

            return

        update_params = {
            k: v
            for k, v in {
                "input": input,
                "output": output,
                "name": name,
                "version": version,
                "metadata": metadata,
                "start_time": start_time,
                "end_time": end_time,
                "release": release,
                "tags": tags,
                "user_id": user_id,
                "session_id": session_id,
                "level": level,
                "status_message": status_message,
                "completion_start_time": completion_start_time,
                "model": model,
                "model_parameters": model_parameters,
                "usage": usage,
                "prompt": prompt,
                "public": public,
            }.items()
            if v is not None
        }

        _observation_params_context.get()[observation.id].update(update_params)

    def score_current_observation(
        self,
        *,
        name: str,
        value: float,
        comment: Optional[str] = None,
        id: Optional[str] = None,
    ):
        """Score the current observation within an active trace. If called on the top level of a trace, it will score the trace.

        Arguments:
            name (str): The name of the score metric. This should be a clear and concise identifier for the metric being recorded.
            value (float): The numerical value of the score. This could represent performance metrics, error rates, or any other quantifiable measure.
            comment (Optional[str]): An optional comment or description providing context or additional details about the score.
            id (Optional[str]): An optional custom ID for the scoring event. Useful for linking scores with external systems or for detailed tracking.

        Returns:
            None

        Note:
            This method is intended to be used within the context of an active trace or observation.
        """
        try:
            langfuse = self._get_langfuse()
            trace_id = self.get_current_trace_id()
            current_observation_id = self.get_current_observation_id()

            observation_id = (
                current_observation_id if current_observation_id != trace_id else None
            )

            if trace_id:
                langfuse.score(
                    trace_id=trace_id,
                    observation_id=observation_id,
                    name=name,
                    value=value,
                    comment=comment,
                    id=id,
                )
            else:
                raise ValueError("No trace or observation found in the current context")

        except Exception as e:
            self._log.error(f"Failed to score observation: {e}")

    def score_current_trace(
        self,
        *,
        name: str,
        value: float,
        comment: Optional[str] = None,
        id: Optional[str] = None,
    ):
        """Score the current trace in context. This can be called anywhere in the nested trace to score the trace.

        Arguments:
            name (str): The name of the score metric. This should be a clear and concise identifier for the metric being recorded.
            value (float): The numerical value of the score. This could represent performance metrics, error rates, or any other quantifiable measure.
            comment (Optional[str]): An optional comment or description providing context or additional details about the score.
            id (Optional[str]): An optional custom ID for the scoring event. Useful for linking scores with external systems or for detailed tracking.

        Returns:
            None

        Note:
            This method is intended to be used within the context of an active trace or observation.
        """
        try:
            langfuse = self._get_langfuse()
            trace_id = self.get_current_trace_id()

            if trace_id:
                langfuse.score(
                    trace_id=trace_id,
                    name=name,
                    value=value,
                    comment=comment,
                    id=id,
                )
            else:
                raise ValueError("No trace found in the current context")

        except Exception as e:
            self._log.error(f"Failed to score observation: {e}")

    @catch_and_log_errors
    def flush(self):
        """Force immediate flush of all buffered observations to the Langfuse backend.

        This method triggers the explicit sending of all accumulated trace and observation data that has not yet been sent to Langfuse servers.
        It is typically used to ensure that data is promptly available for analysis, especially at the end of an execution context or before the application exits.

        Usage:
            - This method can be called at strategic points in the application where it's crucial to ensure that all telemetry data captured up to that point is made persistent and visible on the Langfuse platform.
            - It's particularly useful in scenarios where the application might terminate abruptly or in batch processing tasks that require periodic flushing of trace data.

        Returns:
            None

        Raises:
            ValueError: If it fails to find a Langfuse client object in the current context, indicating potential misconfiguration or initialization issues.

        Note:
            - The flush operation may involve network I/O to send data to the Langfuse backend, which could impact performance if called too frequently in performance-sensitive contexts.
            - In long-running applications, it's often sufficient to rely on the automatic flushing mechanism provided by the Langfuse client.
            However, explicit calls to `flush` can be beneficial in certain edge cases or for debugging purposes.
        """
        langfuse = self._get_langfuse()
        if langfuse:
            langfuse.flush()
        else:
            self._log.warn("No langfuse object found in the current context")

    def configure(
        self,
        *,
        public_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        host: Optional[str] = None,
        release: Optional[str] = None,
        debug: Optional[bool] = None,
        threads: Optional[int] = None,
        flush_at: Optional[int] = None,
        flush_interval: Optional[int] = None,
        max_retries: Optional[int] = None,
        timeout: Optional[int] = None,
        httpx_client: Optional[httpx.Client] = None,
        enabled: Optional[bool] = None,
    ):
        """Configure the Langfuse client.

        If called, this method must be called before any other langfuse_context or observe decorated function to configure the Langfuse client with the necessary credentials and settings.

        Args:
            public_key: Public API key of Langfuse project. Can be set via `LANGFUSE_PUBLIC_KEY` environment variable.
            secret_key: Secret API key of Langfuse project. Can be set via `LANGFUSE_SECRET_KEY` environment variable.
            host: Host of Langfuse API. Can be set via `LANGFUSE_HOST` environment variable. Defaults to `https://cloud.langfuse.com`.
            release: Release number/hash of the application to provide analytics grouped by release. Can be set via `LANGFUSE_RELEASE` environment variable.
            debug: Enables debug mode for more verbose logging. Can be set via `LANGFUSE_DEBUG` environment variable.
            threads: Number of consumer threads to execute network requests. Helps scaling the SDK for high load. Only increase this if you run into scaling issues.
            flush_at: Max batch size that's sent to the API.
            flush_interval: Max delay until a new batch is sent to the API.
            max_retries: Max number of retries in case of API/network errors.
            timeout: Timeout of API requests in seconds. Default is 20 seconds.
            httpx_client: Pass your own httpx client for more customizability of requests.
            enabled: Enables or disables the Langfuse client. Defaults to True. If disabled, no observability data will be sent to Langfuse. If data is requested while disabled, an error will be raised.
        """
        langfuse_singleton = LangfuseSingleton()
        langfuse_singleton.reset()

        langfuse_singleton.get(
            public_key=public_key,
            secret_key=secret_key,
            host=host,
            release=release,
            debug=debug,
            threads=threads,
            flush_at=flush_at,
            flush_interval=flush_interval,
            max_retries=max_retries,
            timeout=timeout,
            httpx_client=httpx_client,
            enabled=enabled,
        )

    def _get_langfuse(self) -> Langfuse:
        return LangfuseSingleton().get()

    def _set_root_trace_id(self, trace_id: str):
        if _observation_stack_context.get():
            self._log.warn(
                "Root Trace ID cannot be set on a already running trace. Skipping root trace ID assignment."
            )
            return

        _root_trace_id_context.set(trace_id)

    def auth_check(self) -> bool:
        """Check if the current Langfuse client is authenticated.

        Returns:
            bool: True if the client is authenticated, False otherwise
        """
        try:
            langfuse = self._get_langfuse()

            return langfuse.auth_check()
        except Exception as e:
            self._log.error("No Langfuse object found in the current context", e)

            return False


langfuse_context = LangfuseDecorator()
observe = langfuse_context.observe
