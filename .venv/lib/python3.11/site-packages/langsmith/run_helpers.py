"""Decorator for creating a run tree from functions."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import datetime
import functools
import inspect
import logging
import uuid
import warnings
from contextvars import copy_context
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Generic,
    List,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    Type,
    TypedDict,
    TypeVar,
    Union,
    cast,
    overload,
    runtime_checkable,
)

from typing_extensions import ParamSpec, TypeGuard

from langsmith import client as ls_client
from langsmith import run_trees, utils
from langsmith._internal import _aiter as aitertools
from langsmith.env import _runtime_env

if TYPE_CHECKING:
    from langchain_core.runnables import Runnable

LOGGER = logging.getLogger(__name__)
_PARENT_RUN_TREE = contextvars.ContextVar[Optional[run_trees.RunTree]](
    "_PARENT_RUN_TREE", default=None
)
_PROJECT_NAME = contextvars.ContextVar[Optional[str]]("_PROJECT_NAME", default=None)
_TAGS = contextvars.ContextVar[Optional[List[str]]]("_TAGS", default=None)
_METADATA = contextvars.ContextVar[Optional[Dict[str, Any]]]("_METADATA", default=None)
_TRACING_ENABLED = contextvars.ContextVar[Optional[bool]](
    "_TRACING_ENABLED", default=None
)
_CONTEXT_KEYS: Dict[str, contextvars.ContextVar] = {
    "parent": _PARENT_RUN_TREE,
    "project_name": _PROJECT_NAME,
    "tags": _TAGS,
    "metadata": _METADATA,
    "enabled": _TRACING_ENABLED,
}


def get_current_run_tree() -> Optional[run_trees.RunTree]:
    """Get the current run tree."""
    return _PARENT_RUN_TREE.get()


def get_tracing_context(
    context: Optional[contextvars.Context] = None,
) -> Dict[str, Any]:
    """Get the current tracing context."""
    if context is None:
        return {
            "parent": _PARENT_RUN_TREE.get(),
            "project_name": _PROJECT_NAME.get(),
            "tags": _TAGS.get(),
            "metadata": _METADATA.get(),
            "enabled": _TRACING_ENABLED.get(),
        }
    return {k: context.get(v) for k, v in _CONTEXT_KEYS.items()}


def _set_tracing_context(context: Dict[str, Any]):
    """Set the tracing context."""
    for k, v in context.items():
        var = _CONTEXT_KEYS[k]
        var.set(v)


@contextlib.contextmanager
def tracing_context(
    *,
    project_name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    parent: Optional[Union[run_trees.RunTree, Mapping, str]] = None,
    enabled: Optional[bool] = None,
    **kwargs: Any,
) -> Generator[None, None, None]:
    """Set the tracing context for a block of code.

    Args:
        project_name: The name of the project to log the run to. Defaults to None.
        tags: The tags to add to the run. Defaults to None.
        metadata: The metadata to add to the run. Defaults to None.
        parent: The parent run to use for the context. Can be a Run/RunTree object,
            request headers (for distributed tracing), or the dotted order string.
            Defaults to None.
        enabled: Whether tracing is enabled. Defaults to None, meaning it will use the
            current context value or environment variables.

    """
    if kwargs:
        # warn
        warnings.warn(
            f"Unrecognized keyword arguments: {kwargs}.",
            DeprecationWarning,
        )
    current_context = get_tracing_context()
    parent_run = _get_parent_run({"parent": parent or kwargs.get("parent_run")})
    if parent_run is not None:
        tags = sorted(set(tags or []) | set(parent_run.tags or []))
        metadata = {**parent_run.metadata, **(metadata or {})}
    enabled = enabled if enabled is not None else current_context.get("enabled")

    _set_tracing_context(
        {
            "parent": parent_run,
            "project_name": project_name,
            "tags": tags,
            "metadata": metadata,
            "enabled": enabled,
        }
    )
    try:
        yield
    finally:
        _set_tracing_context(current_context)


# Alias for backwards compatibility
get_run_tree_context = get_current_run_tree


def is_traceable_function(
    func: Callable[P, R],
) -> TypeGuard[SupportsLangsmithExtra[P, R]]:
    """Check if a function is @traceable decorated."""
    return (
        _is_traceable_function(func)
        or (isinstance(func, functools.partial) and _is_traceable_function(func.func))
        or (hasattr(func, "__call__") and _is_traceable_function(func.__call__))
    )


def ensure_traceable(func: Callable[P, R]) -> SupportsLangsmithExtra[P, R]:
    """Ensure that a function is traceable."""
    if is_traceable_function(func):
        return func
    return traceable()(func)


def is_async(func: Callable) -> bool:
    """Inspect function or wrapped function to see if it is async."""
    return inspect.iscoroutinefunction(func) or (
        hasattr(func, "__wrapped__") and inspect.iscoroutinefunction(func.__wrapped__)
    )


class LangSmithExtra(TypedDict, total=False):
    """Any additional info to be injected into the run dynamically."""

    reference_example_id: Optional[ls_client.ID_TYPE]
    run_extra: Optional[Dict]
    parent: Optional[Union[run_trees.RunTree, str, Mapping]]
    run_tree: Optional[run_trees.RunTree]  # TODO: Deprecate
    project_name: Optional[str]
    metadata: Optional[Dict[str, Any]]
    tags: Optional[List[str]]
    run_id: Optional[ls_client.ID_TYPE]
    client: Optional[ls_client.Client]
    on_end: Optional[Callable[[run_trees.RunTree], Any]]


R = TypeVar("R", covariant=True)
P = ParamSpec("P")


@runtime_checkable
class SupportsLangsmithExtra(Protocol, Generic[P, R]):
    """Implementations of this Protoc accept an optional langsmith_extra parameter.

    Args:
        *args: Variable length arguments.
        langsmith_extra (Optional[LangSmithExtra): Optional dictionary of
            additional parameters for Langsmith.
        **kwargs: Keyword arguments.

    Returns:
        R: The return type of the callable.
    """

    def __call__(
        self,
        *args: P.args,
        langsmith_extra: Optional[LangSmithExtra] = None,
        **kwargs: P.kwargs,
    ) -> R:
        """Call the instance when it is called as a function.

        Args:
            *args: Variable length argument list.
            langsmith_extra: Optional dictionary containing additional
                parameters specific to Langsmith.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            R: The return value of the method.

        """
        ...


@overload
def traceable(
    func: Callable[P, R],
) -> SupportsLangsmithExtra[P, R]: ...


@overload
def traceable(
    run_type: ls_client.RUN_TYPE_T = "chain",
    *,
    name: Optional[str] = None,
    metadata: Optional[Mapping[str, Any]] = None,
    tags: Optional[List[str]] = None,
    client: Optional[ls_client.Client] = None,
    reduce_fn: Optional[Callable] = None,
    project_name: Optional[str] = None,
    process_inputs: Optional[Callable[[dict], dict]] = None,
    _invocation_params_fn: Optional[Callable[[dict], dict]] = None,
) -> Callable[[Callable[P, R]], SupportsLangsmithExtra[P, R]]: ...


def traceable(
    *args: Any,
    **kwargs: Any,
) -> Union[Callable, Callable[[Callable], Callable]]:
    """Trace a function with langsmith.

    Args:
        run_type: The type of run (span) to create. Examples: llm, chain, tool, prompt,
            retriever, etc. Defaults to "chain".
        name: The name of the run. Defaults to the function name.
        metadata: The metadata to add to the run. Defaults to None.
        tags: The tags to add to the run. Defaults to None.
        client: The client to use for logging the run to LangSmith. Defaults to
            None, which will use the default client.
        reduce_fn: A function to reduce the output of the function if the function
            returns a generator. Defaults to None, which means the values will be
                logged as a list. Note: if the iterator is never exhausted (e.g.
                the function returns an infinite generator), this will never be
                called, and the run itself will be stuck in a pending state.
        project_name: The name of the project to log the run to. Defaults to None,
            which will use the default project.
        process_inputs: A function to filter the inputs to the run. Defaults to None.


    Returns:
            Union[Callable, Callable[[Callable], Callable]]: The decorated function.

    Note:
            - Requires that LANGSMITH_TRACING_V2 be set to 'true' in the environment.

    Examples:
        .. code-block:: python
            import httpx
            import asyncio

            from typing import Iterable
            from langsmith import traceable, Client


            # Basic usage:
            @traceable
            def my_function(x: float, y: float) -> float:
                return x + y


            my_function(5, 6)


            @traceable
            async def my_async_function(query_params: dict) -> dict:
                async with httpx.AsyncClient() as http_client:
                    response = await http_client.get(
                        "https://api.example.com/data",
                        params=query_params,
                    )
                    return response.json()


            asyncio.run(my_async_function({"param": "value"}))


            # Streaming data with a generator:
            @traceable
            def my_generator(n: int) -> Iterable:
                for i in range(n):
                    yield i


            for item in my_generator(5):
                print(item)


            # Async streaming data
            @traceable
            async def my_async_generator(query_params: dict) -> Iterable:
                async with httpx.AsyncClient() as http_client:
                    response = await http_client.get(
                        "https://api.example.com/data",
                        params=query_params,
                    )
                    for item in response.json():
                        yield item


            async def async_code():
                async for item in my_async_generator({"param": "value"}):
                    print(item)


            asyncio.run(async_code())


            # Specifying a run type and name:
            @traceable(name="CustomName", run_type="tool")
            def another_function(a: float, b: float) -> float:
                return a * b


            another_function(5, 6)


            # Logging with custom metadata and tags:
            @traceable(
                metadata={"version": "1.0", "author": "John Doe"}, tags=["beta", "test"]
            )
            def tagged_function(x):
                return x**2


            tagged_function(5)

            # Specifying a custom client and project name:
            custom_client = Client(api_key="your_api_key")


            @traceable(client=custom_client, project_name="My Special Project")
            def project_specific_function(data):
                return data


            project_specific_function({"data": "to process"})


            # Manually passing langsmith_extra:
            @traceable
            def manual_extra_function(x):
                return x**2


            manual_extra_function(5, langsmith_extra={"metadata": {"version": "1.0"}})
    """  # noqa: E501
    run_type: ls_client.RUN_TYPE_T = (
        args[0]
        if args and isinstance(args[0], str)
        else (kwargs.pop("run_type", None) or "chain")
    )
    if run_type not in _VALID_RUN_TYPES:
        warnings.warn(
            f"Unrecognized run_type: {run_type}. Must be one of: {_VALID_RUN_TYPES}."
            f" Did you mean @traceable(name='{run_type}')?"
        )
    if len(args) > 1:
        warnings.warn(
            "The `traceable()` decorator only accepts one positional argument, "
            "which should be the run_type. All other arguments should be passed "
            "as keyword arguments."
        )
    if "extra" in kwargs:
        warnings.warn(
            "The `extra` keyword argument is deprecated. Please use `metadata` "
            "instead.",
            DeprecationWarning,
        )
    reduce_fn = kwargs.pop("reduce_fn", None)
    container_input = _ContainerInput(
        # TODO: Deprecate raw extra
        extra_outer=kwargs.pop("extra", None),
        name=kwargs.pop("name", None),
        metadata=kwargs.pop("metadata", None),
        tags=kwargs.pop("tags", None),
        client=kwargs.pop("client", None),
        project_name=kwargs.pop("project_name", None),
        run_type=run_type,
        process_inputs=kwargs.pop("process_inputs", None),
        invocation_params_fn=kwargs.pop("_invocation_params_fn", None),
    )
    if kwargs:
        warnings.warn(
            f"The following keyword arguments are not recognized and will be ignored: "
            f"{sorted(kwargs.keys())}.",
            DeprecationWarning,
        )

    def decorator(func: Callable):
        func_sig = inspect.signature(func)
        func_accepts_parent_run = func_sig.parameters.get("run_tree", None) is not None
        func_accepts_config = func_sig.parameters.get("config", None) is not None

        @functools.wraps(func)
        async def async_wrapper(
            *args: Any,
            langsmith_extra: Optional[LangSmithExtra] = None,
            **kwargs: Any,
        ) -> Any:
            """Async version of wrapper function."""
            run_container = _setup_run(
                func,
                container_input=container_input,
                langsmith_extra=langsmith_extra,
                args=args,
                kwargs=kwargs,
            )

            try:
                accepts_context = aitertools.accepts_context(asyncio.create_task)
                if func_accepts_parent_run:
                    kwargs["run_tree"] = run_container["new_run"]
                if not func_accepts_config:
                    kwargs.pop("config", None)
                fr_coro = func(*args, **kwargs)
                if accepts_context:
                    function_result = await asyncio.create_task(  # type: ignore[call-arg]
                        fr_coro, context=run_container["context"]
                    )
                else:
                    # Python < 3.11
                    with tracing_context(
                        **get_tracing_context(run_container["context"])
                    ):
                        function_result = await fr_coro
            except BaseException as e:
                _container_end(run_container, error=e)
                raise e
            _container_end(run_container, outputs=function_result)
            return function_result

        @functools.wraps(func)
        async def async_generator_wrapper(
            *args: Any, langsmith_extra: Optional[LangSmithExtra] = None, **kwargs: Any
        ) -> AsyncGenerator:
            run_container = _setup_run(
                func,
                container_input=container_input,
                langsmith_extra=langsmith_extra,
                args=args,
                kwargs=kwargs,
            )
            results: List[Any] = []
            try:
                if func_accepts_parent_run:
                    kwargs["run_tree"] = run_container["new_run"]
                    # TODO: Nesting is ambiguous if a nested traceable function is only
                    # called mid-generation. Need to explicitly accept run_tree to get
                    # around this.
                if not func_accepts_config:
                    kwargs.pop("config", None)
                async_gen_result = func(*args, **kwargs)
                # Can't iterate through if it's a coroutine
                accepts_context = aitertools.accepts_context(asyncio.create_task)
                if inspect.iscoroutine(async_gen_result):
                    if accepts_context:
                        async_gen_result = await asyncio.create_task(
                            async_gen_result, context=run_container["context"]
                        )  # type: ignore
                    else:
                        # Python < 3.11
                        with tracing_context(
                            **get_tracing_context(run_container["context"])
                        ):
                            async_gen_result = await async_gen_result
                try:
                    while True:
                        if accepts_context:
                            item = await asyncio.create_task(  # type: ignore[call-arg, var-annotated]
                                aitertools.py_anext(async_gen_result),  # type: ignore[arg-type]
                                context=run_container["context"],
                            )
                        else:
                            # Python < 3.11
                            with tracing_context(
                                **get_tracing_context(run_container["context"])
                            ):
                                item = await aitertools.py_anext(async_gen_result)
                        if run_type == "llm":
                            if run_container["new_run"]:
                                run_container["new_run"].add_event(
                                    {
                                        "name": "new_token",
                                        "time": datetime.datetime.now(
                                            datetime.timezone.utc
                                        ).isoformat(),
                                        "kwargs": {"token": item},
                                    }
                                )
                        results.append(item)
                        yield item
                except StopAsyncIteration:
                    pass
            except BaseException as e:
                _container_end(run_container, error=e)
                raise e
            if results:
                if reduce_fn:
                    try:
                        function_result = reduce_fn(results)
                    except BaseException as e:
                        LOGGER.error(e)
                        function_result = results
                else:
                    function_result = results
            else:
                function_result = None
            _container_end(run_container, outputs=function_result)

        @functools.wraps(func)
        def wrapper(
            *args: Any,
            langsmith_extra: Optional[LangSmithExtra] = None,
            **kwargs: Any,
        ) -> Any:
            """Create a new run or create_child() if run is passed in kwargs."""
            run_container = _setup_run(
                func,
                container_input=container_input,
                langsmith_extra=langsmith_extra,
                args=args,
                kwargs=kwargs,
            )
            func_accepts_parent_run = (
                inspect.signature(func).parameters.get("run_tree", None) is not None
            )
            try:
                if func_accepts_parent_run:
                    kwargs["run_tree"] = run_container["new_run"]
                if not func_accepts_config:
                    kwargs.pop("config", None)
                function_result = run_container["context"].run(func, *args, **kwargs)
            except BaseException as e:
                _container_end(run_container, error=e)
                raise e
            _container_end(run_container, outputs=function_result)
            return function_result

        @functools.wraps(func)
        def generator_wrapper(
            *args: Any, langsmith_extra: Optional[LangSmithExtra] = None, **kwargs: Any
        ) -> Any:
            run_container = _setup_run(
                func,
                container_input=container_input,
                langsmith_extra=langsmith_extra,
                args=args,
                kwargs=kwargs,
            )
            func_accepts_parent_run = (
                inspect.signature(func).parameters.get("run_tree", None) is not None
            )
            results: List[Any] = []
            try:
                if func_accepts_parent_run:
                    kwargs["run_tree"] = run_container["new_run"]
                    # TODO: Nesting is ambiguous if a nested traceable function is only
                    # called mid-generation. Need to explicitly accept run_tree to get
                    # around this.
                if not func_accepts_config:
                    kwargs.pop("config", None)
                generator_result = run_container["context"].run(func, *args, **kwargs)
                try:
                    while True:
                        item = run_container["context"].run(next, generator_result)
                        if run_type == "llm":
                            if run_container["new_run"]:
                                run_container["new_run"].add_event(
                                    {
                                        "name": "new_token",
                                        "time": datetime.datetime.now(
                                            datetime.timezone.utc
                                        ).isoformat(),
                                        "kwargs": {"token": item},
                                    }
                                )
                        results.append(item)
                        try:
                            yield item
                        except GeneratorExit:
                            break
                except StopIteration:
                    pass

            except BaseException as e:
                _container_end(run_container, error=e)
                raise e
            if results:
                if reduce_fn:
                    try:
                        function_result = reduce_fn(results)
                    except BaseException as e:
                        LOGGER.error(e)
                        function_result = results
                else:
                    function_result = results
            else:
                function_result = None
            _container_end(run_container, outputs=function_result)

        if inspect.isasyncgenfunction(func):
            selected_wrapper: Callable = async_generator_wrapper
        elif is_async(func):
            if reduce_fn:
                selected_wrapper = async_generator_wrapper
            else:
                selected_wrapper = async_wrapper
        elif reduce_fn or inspect.isgeneratorfunction(func):
            selected_wrapper = generator_wrapper
        else:
            selected_wrapper = wrapper
        setattr(selected_wrapper, "__langsmith_traceable__", True)
        sig = inspect.signature(selected_wrapper)
        if not sig.parameters.get("config"):
            sig = sig.replace(
                parameters=[
                    *(
                        param
                        for param in sig.parameters.values()
                        if param.kind != inspect.Parameter.VAR_KEYWORD
                    ),
                    inspect.Parameter(
                        "config", inspect.Parameter.KEYWORD_ONLY, default=None
                    ),
                    *(
                        param
                        for param in sig.parameters.values()
                        if param.kind == inspect.Parameter.VAR_KEYWORD
                    ),
                ]
            )
            selected_wrapper.__signature__ = sig  # type: ignore[attr-defined]
        return selected_wrapper

    # If the decorator is called with no arguments, then it's being used as a
    # decorator, so we return the decorator function
    if len(args) == 1 and callable(args[0]) and not kwargs:
        return decorator(args[0])
    # Else it's being used as a decorator factory, so we return the decorator
    return decorator


@contextlib.contextmanager
def trace(
    name: str,
    run_type: ls_client.RUN_TYPE_T = "chain",
    *,
    inputs: Optional[Dict] = None,
    extra: Optional[Dict] = None,
    project_name: Optional[str] = None,
    parent: Optional[Union[run_trees.RunTree, str, Mapping]] = None,
    tags: Optional[List[str]] = None,
    metadata: Optional[Mapping[str, Any]] = None,
    client: Optional[ls_client.Client] = None,
    run_id: Optional[ls_client.ID_TYPE] = None,
    reference_example_id: Optional[ls_client.ID_TYPE] = None,
    exceptions_to_handle: Optional[Tuple[Type[BaseException], ...]] = None,
    **kwargs: Any,
) -> Generator[run_trees.RunTree, None, None]:
    """Context manager for creating a run tree."""
    if kwargs:
        # In case someone was passing an executor before.
        warnings.warn(
            "The `trace` context manager no longer supports the following kwargs: "
            f"{sorted(kwargs.keys())}.",
            DeprecationWarning,
        )
    old_ctx = get_tracing_context()
    outer_tags = _TAGS.get()
    outer_metadata = _METADATA.get()
    outer_project = _PROJECT_NAME.get() or utils.get_tracer_project()
    parent_run_ = _get_parent_run(
        {"parent": parent, "run_tree": kwargs.get("run_tree"), "client": client}
    )

    # Merge and set context variables
    tags_ = sorted(set((tags or []) + (outer_tags or [])))
    _TAGS.set(tags_)
    metadata = {**(metadata or {}), **(outer_metadata or {}), "ls_method": "trace"}
    _METADATA.set(metadata)

    extra_outer = extra or {}
    extra_outer["metadata"] = metadata

    project_name_ = project_name or outer_project
    if parent_run_ is not None:
        new_run = parent_run_.create_child(
            name=name,
            run_id=run_id,
            run_type=run_type,
            extra=extra_outer,
            inputs=inputs,
            tags=tags_,
        )
    else:
        new_run = run_trees.RunTree(
            name=name,
            id=ls_client._ensure_uuid(run_id),
            reference_example_id=ls_client._ensure_uuid(
                reference_example_id, accept_null=True
            ),
            run_type=run_type,
            extra=extra_outer,
            project_name=project_name_,  # type: ignore[arg-type]
            inputs=inputs or {},
            tags=tags_,
            client=client,  # type: ignore[arg-type]
        )
    new_run.post()
    _PARENT_RUN_TREE.set(new_run)
    _PROJECT_NAME.set(project_name_)

    try:
        yield new_run
    except (Exception, KeyboardInterrupt, BaseException) as e:
        if exceptions_to_handle and isinstance(e, exceptions_to_handle):
            tb = None
        else:
            tb = utils._format_exc()
            tb = f"{e.__class__.__name__}: {e}\n\n{tb}"
        new_run.end(error=tb)
        new_run.patch()
        raise e
    finally:
        # Reset the old context
        _set_tracing_context(old_ctx)
    new_run.patch()


def as_runnable(traceable_fn: Callable) -> Runnable:
    """Convert a function wrapped by the LangSmith @traceable decorator to a Runnable.

    Args:
        traceable_fn (Callable): The function wrapped by the @traceable decorator.

    Returns:
        Runnable: A Runnable object that maintains a consistent LangSmith
            tracing context.

    Raises:
        ImportError: If langchain module is not installed.
        ValueError: If the provided function is not wrapped by the @traceable decorator.

    Example:
        >>> @traceable
        ... def my_function(input_data):
        ...     # Function implementation
        ...     pass
        >>> runnable = as_runnable(my_function)
    """
    try:
        from langchain_core.runnables import RunnableConfig, RunnableLambda
        from langchain_core.runnables.utils import Input, Output
    except ImportError as e:
        raise ImportError(
            "as_runnable requires langchain-core to be installed. "
            "You can install it with `pip install langchain-core`."
        ) from e
    if not is_traceable_function(traceable_fn):
        try:
            fn_src = inspect.getsource(traceable_fn)
        except Exception:
            fn_src = "<source unavailable>"
        raise ValueError(
            f"as_runnable expects a function wrapped by the LangSmith"
            f" @traceable decorator. Got {traceable_fn} defined as:\n{fn_src}"
        )

    class RunnableTraceable(RunnableLambda):
        """Converts a @traceable decorated function to a Runnable.

        This helps maintain a consistent LangSmith tracing context.
        """

        def __init__(
            self,
            func: Callable,
            afunc: Optional[Callable[..., Awaitable[Output]]] = None,
        ) -> None:
            wrapped: Optional[Callable[[Input], Output]] = None
            awrapped = self._wrap_async(afunc)
            if is_async(func):
                if awrapped is not None:
                    raise TypeError(
                        "Func was provided as a coroutine function, but afunc was "
                        "also provided. If providing both, func should be a regular "
                        "function to avoid ambiguity."
                    )
                wrapped = cast(Callable[[Input], Output], self._wrap_async(func))
            elif is_traceable_function(func):
                wrapped = cast(Callable[[Input], Output], self._wrap_sync(func))
            if wrapped is None:
                raise ValueError(
                    f"{self.__class__.__name__} expects a function wrapped by"
                    " the LangSmith"
                    f" @traceable decorator. Got {func}"
                )

            super().__init__(
                wrapped,
                cast(
                    Optional[Callable[[Input], Awaitable[Output]]],
                    awrapped,
                ),
            )

        @staticmethod
        def _wrap_sync(
            func: Callable[..., Output],
        ) -> Callable[[Input, RunnableConfig], Output]:
            """Wrap a synchronous function to make it asynchronous."""

            def wrap_traceable(inputs: dict, config: RunnableConfig) -> Any:
                run_tree = run_trees.RunTree.from_runnable_config(cast(dict, config))
                return func(**inputs, langsmith_extra={"run_tree": run_tree})

            return cast(Callable[[Input, RunnableConfig], Output], wrap_traceable)

        @staticmethod
        def _wrap_async(
            afunc: Optional[Callable[..., Awaitable[Output]]],
        ) -> Optional[Callable[[Input, RunnableConfig], Awaitable[Output]]]:
            """Wrap an async function to make it synchronous."""
            if afunc is None:
                return None

            if not is_traceable_function(afunc):
                raise ValueError(
                    "RunnableTraceable expects a function wrapped by the LangSmith"
                    f" @traceable decorator. Got {afunc}"
                )
            afunc_ = cast(Callable[..., Awaitable[Output]], afunc)

            async def awrap_traceable(inputs: dict, config: RunnableConfig) -> Any:
                run_tree = run_trees.RunTree.from_runnable_config(cast(dict, config))
                return await afunc_(**inputs, langsmith_extra={"run_tree": run_tree})

            return cast(
                Callable[[Input, RunnableConfig], Awaitable[Output]], awrap_traceable
            )

    return RunnableTraceable(traceable_fn)


## Private Methods and Objects

_VALID_RUN_TYPES = {
    "tool",
    "chain",
    "llm",
    "retriever",
    "embedding",
    "prompt",
    "parser",
}


class _TraceableContainer(TypedDict, total=False):
    """Typed response when initializing a run a traceable."""

    new_run: Optional[run_trees.RunTree]
    project_name: Optional[str]
    outer_project: Optional[str]
    outer_metadata: Optional[Dict[str, Any]]
    outer_tags: Optional[List[str]]
    on_end: Optional[Callable[[run_trees.RunTree], Any]]
    context: contextvars.Context


class _ContainerInput(TypedDict, total=False):
    """Typed response when initializing a run a traceable."""

    extra_outer: Optional[Dict]
    name: Optional[str]
    metadata: Optional[Dict[str, Any]]
    tags: Optional[List[str]]
    client: Optional[ls_client.Client]
    reduce_fn: Optional[Callable]
    project_name: Optional[str]
    run_type: ls_client.RUN_TYPE_T
    process_inputs: Optional[Callable[[dict], dict]]
    invocation_params_fn: Optional[Callable[[dict], dict]]


def _container_end(
    container: _TraceableContainer,
    outputs: Optional[Any] = None,
    error: Optional[BaseException] = None,
):
    """End the run."""
    run_tree = container.get("new_run")
    if run_tree is None:
        # Tracing enabled
        return
    outputs_ = outputs if isinstance(outputs, dict) else {"output": outputs}
    error_ = None
    if error:
        stacktrace = utils._format_exc()
        error_ = f"{repr(error)}\n\n{stacktrace}"
    run_tree.end(outputs=outputs_, error=error_)
    run_tree.patch()
    if error:
        try:
            LOGGER.info(f"See trace: {run_tree.get_url()}")
        except Exception:
            pass
    on_end = container.get("on_end")
    if on_end is not None and callable(on_end):
        try:
            on_end(run_tree)
        except BaseException as e:
            LOGGER.warning(f"Failed to run on_end function: {e}")


def _collect_extra(extra_outer: dict, langsmith_extra: LangSmithExtra) -> dict:
    run_extra = langsmith_extra.get("run_extra", None)
    if run_extra:
        extra_inner = {**extra_outer, **run_extra}
    else:
        extra_inner = extra_outer
    return extra_inner


def _get_parent_run(
    langsmith_extra: LangSmithExtra,
    config: Optional[dict] = None,
) -> Optional[run_trees.RunTree]:
    parent = langsmith_extra.get("parent")
    if isinstance(parent, run_trees.RunTree):
        return parent
    if isinstance(parent, dict):
        return run_trees.RunTree.from_headers(
            parent, client=langsmith_extra.get("client")
        )
    if isinstance(parent, str):
        return run_trees.RunTree.from_dotted_order(
            parent, client=langsmith_extra.get("client")
        )
    run_tree = langsmith_extra.get("run_tree")
    if run_tree:
        return run_tree
    crt = get_current_run_tree()
    if _runtime_env.get_langchain_core_version() is not None:
        if rt := run_trees.RunTree.from_runnable_config(
            config, client=langsmith_extra.get("client")
        ):
            # Still need to break ties when alternating between traceable and
            # LanChain code.
            # Nesting: LC -> LS -> LS, we want to still use LS as the parent
            # Otherwise would look like LC -> {LS, LS} (siblings)
            if (
                not crt  # Simple LC -> LS
                # Let user override if manually passed in or invoked in a
                # RunnableSequence. This is a naive check.
                or (config is not None and config.get("callbacks"))
                # If the LangChain dotted order is more nested than the LangSmith
                # dotted order, use the LangChain run as the parent.
                # Note that this condition shouldn't be triggered in later
                # versions of core, since we also update the run_tree context
                # vars when updating the RunnableConfig context var.
                or rt.dotted_order > crt.dotted_order
            ):
                return rt
    return crt


def _setup_run(
    func: Callable,
    container_input: _ContainerInput,
    langsmith_extra: Optional[LangSmithExtra] = None,
    args: Any = None,
    kwargs: Any = None,
) -> _TraceableContainer:
    """Create a new run or create_child() if run is passed in kwargs."""
    extra_outer = container_input.get("extra_outer") or {}
    name = container_input.get("name")
    metadata = container_input.get("metadata")
    tags = container_input.get("tags")
    client = container_input.get("client")
    run_type = container_input.get("run_type") or "chain"
    outer_project = _PROJECT_NAME.get()
    langsmith_extra = langsmith_extra or LangSmithExtra()
    client_ = langsmith_extra.get("client", client)
    parent_run_ = _get_parent_run(
        {**langsmith_extra, "client": client_}, kwargs.get("config")
    )
    project_cv = _PROJECT_NAME.get()
    selected_project = (
        project_cv  # From parent trace
        or langsmith_extra.get("project_name")  # at invocation time
        or container_input["project_name"]  # at decorator time
        or utils.get_tracer_project()  # default
    )
    reference_example_id = langsmith_extra.get("reference_example_id")
    id_ = langsmith_extra.get("run_id")
    if (
        not project_cv
        and not reference_example_id
        and not parent_run_
        and not utils.tracing_is_enabled()
    ):
        utils.log_once(
            logging.DEBUG, "LangSmith tracing is enabled, returning original function."
        )
        return _TraceableContainer(
            new_run=None,
            project_name=selected_project,
            outer_project=outer_project,
            outer_metadata=None,
            outer_tags=None,
            on_end=langsmith_extra.get("on_end"),
            context=copy_context(),
        )
    id_ = id_ or str(uuid.uuid4())
    signature = inspect.signature(func)
    name_ = name or func.__name__
    docstring = func.__doc__
    extra_inner = _collect_extra(extra_outer, langsmith_extra)
    outer_metadata = _METADATA.get()
    outer_tags = _TAGS.get()
    context = copy_context()
    metadata_ = {
        **(langsmith_extra.get("metadata") or {}),
        **(outer_metadata or {}),
    }
    context.run(_METADATA.set, metadata_)
    metadata_.update(metadata or {})
    metadata_["ls_method"] = "traceable"
    extra_inner["metadata"] = metadata_
    inputs = _get_inputs_safe(signature, *args, **kwargs)
    invocation_params_fn = container_input.get("invocation_params_fn")
    if invocation_params_fn:
        try:
            invocation_params = {
                k: v for k, v in invocation_params_fn(inputs).items() if v is not None
            }
            if invocation_params and isinstance(invocation_params, dict):
                metadata_.update(invocation_params)
        except BaseException as e:
            LOGGER.error(f"Failed to infer invocation params for {name_}: {e}")
    process_inputs = container_input.get("process_inputs")
    if process_inputs:
        try:
            inputs = process_inputs(inputs)
        except BaseException as e:
            LOGGER.error(f"Failed to filter inputs for {name_}: {e}")
    tags_ = (langsmith_extra.get("tags") or []) + (outer_tags or [])
    context.run(_TAGS.set, tags_)
    tags_ += tags or []
    if parent_run_ is not None:
        new_run = parent_run_.create_child(
            name=name_,
            run_type=run_type,
            serialized={
                "name": name,
                "signature": str(signature),
                "doc": docstring,
            },
            inputs=inputs,
            tags=tags_,
            extra=extra_inner,
            run_id=id_,
        )
    else:
        new_run = run_trees.RunTree(
            id=ls_client._ensure_uuid(id_),
            name=name_,
            serialized={
                "name": name,
                "signature": str(signature),
                "doc": docstring,
            },
            inputs=inputs,
            run_type=run_type,
            reference_example_id=ls_client._ensure_uuid(
                reference_example_id, accept_null=True
            ),
            project_name=selected_project,  # type: ignore[arg-type]
            extra=extra_inner,
            tags=tags_,
            client=client_,  # type: ignore
        )
    try:
        new_run.post()
    except BaseException as e:
        LOGGER.error(f"Failed to post run {new_run.id}: {e}")
    response_container = _TraceableContainer(
        new_run=new_run,
        project_name=selected_project,
        outer_project=outer_project,
        outer_metadata=outer_metadata,
        outer_tags=outer_tags,
        on_end=langsmith_extra.get("on_end"),
        context=context,
    )
    context.run(_PROJECT_NAME.set, response_container["project_name"])
    context.run(_PARENT_RUN_TREE.set, response_container["new_run"])
    return response_container


def _is_traceable_function(func: Callable) -> bool:
    return getattr(func, "__langsmith_traceable__", False)


def _get_inputs(
    signature: inspect.Signature, *args: Any, **kwargs: Any
) -> Dict[str, Any]:
    """Return a dictionary of inputs from the function signature."""
    bound = signature.bind_partial(*args, **kwargs)
    bound.apply_defaults()
    arguments = dict(bound.arguments)
    arguments.pop("self", None)
    arguments.pop("cls", None)
    for param_name, param in signature.parameters.items():
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            # Update with the **kwargs, and remove the original entry
            # This is to help flatten out keyword arguments
            if param_name in arguments:
                arguments.update(arguments[param_name])
                arguments.pop(param_name)

    return arguments


def _get_inputs_safe(
    signature: inspect.Signature, *args: Any, **kwargs: Any
) -> Dict[str, Any]:
    try:
        return _get_inputs(signature, *args, **kwargs)
    except BaseException as e:
        LOGGER.debug(f"Failed to get inputs for {signature}: {e}")
        return {"args": args, "kwargs": kwargs}
