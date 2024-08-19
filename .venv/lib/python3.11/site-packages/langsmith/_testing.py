from __future__ import annotations

import atexit
import concurrent.futures
import datetime
import functools
import inspect
import logging
import threading
import uuid
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Optional, Sequence, Tuple, TypeVar, overload

import orjson
from typing_extensions import TypedDict

from langsmith import client as ls_client
from langsmith import env as ls_env
from langsmith import run_helpers as rh
from langsmith import schemas as ls_schemas
from langsmith import utils as ls_utils

try:
    import pytest  # type: ignore

    SkipException = pytest.skip.Exception
except ImportError:

    class SkipException(Exception):  # type: ignore[no-redef]
        pass


logger = logging.getLogger(__name__)


T = TypeVar("T")
U = TypeVar("U")


@overload
def test(
    func: Callable,
) -> Callable: ...


@overload
def test(
    *,
    id: Optional[uuid.UUID] = None,
    output_keys: Optional[Sequence[str]] = None,
    client: Optional[ls_client.Client] = None,
    test_suite_name: Optional[str] = None,
) -> Callable[[Callable], Callable]: ...


def test(*args: Any, **kwargs: Any) -> Callable:
    """Create a test case in LangSmith.

    This decorator is used to mark a function as a test case for LangSmith. It ensures
    that the necessary example data is created and associated with the test function.
    The decorated function will be executed as a test case, and the results will be
    recorded and reported by LangSmith.

    Args:
        - id (Optional[uuid.UUID]): A unique identifier for the test case. If not
            provided, an ID will be generated based on the test function's module
            and name.
        - output_keys (Optional[Sequence[str]]): A list of keys to be considered as
            the output keys for the test case. These keys will be extracted from the
            test function's inputs and stored as the expected outputs.
        - client (Optional[ls_client.Client]): An instance of the LangSmith client
            to be used for communication with the LangSmith service. If not provided,
            a default client will be used.
        - test_suite_name (Optional[str]): The name of the test suite to which the
            test case belongs. If not provided, the test suite name will be determined
            based on the environment or the package name.

    Returns:
        Callable: The decorated test function.

    Environment:
        - LANGSMITH_TEST_CACHE: If set, API calls will be cached to disk to
            save time and costs during testing. Recommended to commit the
            cache files to your repository for faster CI/CD runs.
            Requires the 'langsmith[vcr]' package to be installed.
        - LANGSMITH_TEST_TRACKING: Set this variable to the path of a directory
            to enable caching of test results. This is useful for re-running tests
             without re-executing the code. Requires the 'langsmith[vcr]' package.

    Example:
        For basic usage, simply decorate a test function with `@test`:

        >>> @test
        ... def test_addition():
        ...     assert 3 + 4 == 7


        Any code that is traced (such as those traced using `@traceable`
        or `wrap_*` functions) will be traced within the test case for
        improved visibility and debugging.

        >>> from langsmith import traceable
        >>> @traceable
        ... def generate_numbers():
        ...     return 3, 4

        >>> @test
        ... def test_nested():
        ...     # Traced code will be included in the test case
        ...     a, b = generate_numbers()
        ...     assert a + b == 7

        LLM calls are expensive! Cache requests by setting
        `LANGSMITH_TEST_CACHE=path/to/cache`. Check in these files to speed up
        CI/CD pipelines, so your results only change when your prompt or requested
        model changes.

        Note that this will require that you install langsmith with the `vcr` extra:

        `pip install -U "langsmith[vcr]"`

        Caching is faster if you install libyaml. See
        https://vcrpy.readthedocs.io/en/latest/installation.html#speed for more details.

        >>> # os.environ["LANGSMITH_TEST_CACHE"] = "tests/cassettes"
        >>> import openai
        >>> from langsmith.wrappers import wrap_openai
        >>> oai_client = wrap_openai(openai.Client())
        >>> @test
        ... def test_openai_says_hello():
        ...     # Traced code will be included in the test case
        ...     response = oai_client.chat.completions.create(
        ...         model="gpt-3.5-turbo",
        ...         messages=[
        ...             {"role": "system", "content": "You are a helpful assistant."},
        ...             {"role": "user", "content": "Say hello!"},
        ...         ],
        ...     )
        ...     assert "hello" in response.choices[0].message.content.lower()

        LLMs are stochastic. Naive assertions are flakey. You can use langsmith's
        `expect` to score and make approximate assertions on your results.

        >>> from langsmith import expect
        >>> @test
        ... def test_output_semantically_close():
        ...     response = oai_client.chat.completions.create(
        ...         model="gpt-3.5-turbo",
        ...         messages=[
        ...             {"role": "system", "content": "You are a helpful assistant."},
        ...             {"role": "user", "content": "Say hello!"},
        ...         ],
        ...     )
        ...     # The embedding_distance call logs the embedding distance to LangSmith
        ...     expect.embedding_distance(
        ...         prediction=response.choices[0].message.content,
        ...         reference="Hello!",
        ...         # The following optional assertion logs a
        ...         # pass/fail score to LangSmith
        ...         # and raises an AssertionError if the assertion fails.
        ...     ).to_be_less_than(1.0)
        ...     # Compute damerau_levenshtein distance
        ...     expect.edit_distance(
        ...         prediction=response.choices[0].message.content,
        ...         reference="Hello!",
        ...         # And then log a pass/fail score to LangSmith
        ...     ).to_be_less_than(1.0)

        The `@test` decorator works natively with pytest fixtures.
        The values will populate the "inputs" of the corresponding example in LangSmith.

        >>> import pytest
        >>> @pytest.fixture
        ... def some_input():
        ...     return "Some input"
        >>>
        >>> @test
        ... def test_with_fixture(some_input: str):
        ...     assert "input" in some_input
        >>>

        You can still use pytest.parametrize() as usual to run multiple test cases
        using the same test function.

        >>> @test(output_keys=["expected"])
        ... @pytest.mark.parametrize(
        ...     "a, b, expected",
        ...     [
        ...         (1, 2, 3),
        ...         (3, 4, 7),
        ...     ],
        ... )
        ... def test_addition_with_multiple_inputs(a: int, b: int, expected: int):
        ...     assert a + b == expected

        By default, each test case will be assigned a consistent, unique identifier
        based on the function name and module. You can also provide a custom identifier
        using the `id` argument:
        >>> @test(id="1a77e4b5-1d38-4081-b829-b0442cf3f145")
        ... def test_multiplication():
        ...     assert 3 * 4 == 12

        By default, all test test inputs are saved as "inputs" to a dataset.
        You can specify the `output_keys` argument to persist those keys
        within the dataset's "outputs" fields.

        >>> @pytest.fixture
        ... def expected_output():
        ...     return "input"
        >>> @test(output_keys=["expected_output"])
        ... def test_with_expected_output(some_input: str, expected_output: str):
        ...     assert expected_output in some_input


        To run these tests, use the pytest CLI. Or directly run the test functions.
        >>> test_output_semantically_close()
        >>> test_addition()
        >>> test_nested()
        >>> test_with_fixture("Some input")
        >>> test_with_expected_output("Some input", "Some")
        >>> test_multiplication()
        >>> test_openai_says_hello()
        >>> test_addition_with_multiple_inputs(1, 2, 3)
    """
    langtest_extra = _UTExtra(
        id=kwargs.pop("id", None),
        output_keys=kwargs.pop("output_keys", None),
        client=kwargs.pop("client", None),
        test_suite_name=kwargs.pop("test_suite_name", None),
        cache=ls_utils.get_cache_dir(kwargs.pop("cache", None)),
    )
    if kwargs:
        warnings.warn(f"Unexpected keyword arguments: {kwargs.keys()}")
    disable_tracking = ls_utils.test_tracking_is_disabled()
    if disable_tracking:
        warnings.warn(
            "LANGSMITH_TEST_TRACKING is set to 'false'."
            " Skipping LangSmith test tracking."
        )

    def decorator(func: Callable) -> Callable:
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*test_args: Any, **test_kwargs: Any):
                if disable_tracking:
                    return await func(*test_args, **test_kwargs)
                await _arun_test(
                    func, *test_args, **test_kwargs, langtest_extra=langtest_extra
                )

            return async_wrapper

        @functools.wraps(func)
        def wrapper(*test_args: Any, **test_kwargs: Any):
            if disable_tracking:
                return func(*test_args, **test_kwargs)
            _run_test(func, *test_args, **test_kwargs, langtest_extra=langtest_extra)

        return wrapper

    if args and callable(args[0]):
        return decorator(args[0])

    return decorator


## Private functions


def _get_experiment_name() -> str:
    # TODO Make more easily configurable
    prefix = ls_utils.get_tracer_project(False) or "TestSuiteResult"
    name = f"{prefix}:{uuid.uuid4().hex[:8]}"
    return name


def _get_test_suite_name(func: Callable) -> str:
    test_suite_name = ls_utils.get_env_var("TEST_SUITE")
    if test_suite_name:
        return test_suite_name
    repo_name = ls_env.get_git_info()["repo_name"]
    try:
        mod = inspect.getmodule(func)
        if mod:
            return f"{repo_name}.{mod.__name__}"
    except BaseException:
        logger.debug("Could not determine test suite name from file path.")

    raise ValueError("Please set the LANGSMITH_TEST_SUITE environment variable.")


def _get_test_suite(
    client: ls_client.Client, test_suite_name: str
) -> ls_schemas.Dataset:
    if client.has_dataset(dataset_name=test_suite_name):
        return client.read_dataset(dataset_name=test_suite_name)
    else:
        repo = ls_env.get_git_info().get("remote_url") or ""
        description = "Test suite"
        if repo:
            description += f" for {repo}"
        return client.create_dataset(
            dataset_name=test_suite_name, description=description
        )


def _start_experiment(
    client: ls_client.Client,
    test_suite: ls_schemas.Dataset,
) -> ls_schemas.TracerSession:
    experiment_name = _get_experiment_name()
    try:
        return client.create_project(
            experiment_name,
            reference_dataset_id=test_suite.id,
            description="Test Suite Results.",
            metadata={
                "revision_id": ls_env.get_langchain_env_var_metadata().get(
                    "revision_id"
                )
            },
        )
    except ls_utils.LangSmithConflictError:
        return client.read_project(project_name=experiment_name)


# Track the number of times a parameter has been used in a test
# This is to ensure that we can uniquely identify each test case
# defined using pytest.mark.parametrize
_param_dict: dict = defaultdict(lambda: defaultdict(int))


def _get_id(func: Callable, inputs: dict, suite_id: uuid.UUID) -> Tuple[uuid.UUID, str]:
    global _param_dict
    try:
        file_path = str(Path(inspect.getfile(func)).relative_to(Path.cwd()))
    except ValueError:
        # Fall back to module name if file path is not available
        file_path = func.__module__
    identifier = f"{suite_id}{file_path}::{func.__name__}"
    input_keys = tuple(sorted(inputs.keys()))
    arg_indices = []
    for key in input_keys:
        _param_dict[identifier][key] += 1
        arg_indices.append(f"{key}{_param_dict[identifier][key]}")
    if arg_indices:
        identifier += f"[{'-'.join(arg_indices)}]"
    return uuid.uuid5(uuid.NAMESPACE_DNS, identifier), identifier[len(str(suite_id)) :]


def _end_tests(
    test_suite: _LangSmithTestSuite,
):
    git_info = ls_env.get_git_info() or {}
    test_suite.client.update_project(
        test_suite.experiment_id,
        end_time=datetime.datetime.now(datetime.timezone.utc),
        metadata={
            **git_info,
            "dataset_version": test_suite.get_version(),
            "revision_id": ls_env.get_langchain_env_var_metadata().get("revision_id"),
        },
    )
    test_suite.wait()


VT = TypeVar("VT", bound=Optional[dict])


def _serde_example_values(values: VT) -> VT:
    if values is None:
        return values
    # Don't try to magically serialize Python objects, just use their REPRs.
    bts = ls_client._dumps_json(values, serialize_py=False)
    return orjson.loads(bts)


class _LangSmithTestSuite:
    _instances: Optional[dict] = None
    _lock = threading.RLock()

    def __init__(
        self,
        client: Optional[ls_client.Client],
        experiment: ls_schemas.TracerSession,
        dataset: ls_schemas.Dataset,
    ):
        self.client = client or ls_client.Client()
        self._experiment = experiment
        self._dataset = dataset
        self._version: Optional[datetime.datetime] = None
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        atexit.register(_end_tests, self)

    @property
    def id(self):
        return self._dataset.id

    @property
    def experiment_id(self):
        return self._experiment.id

    @property
    def experiment(self):
        return self._experiment

    @classmethod
    def from_test(
        cls,
        client: Optional[ls_client.Client],
        func: Callable,
        test_suite_name: Optional[str] = None,
    ) -> _LangSmithTestSuite:
        client = client or ls_client.Client()
        test_suite_name = test_suite_name or _get_test_suite_name(func)
        with cls._lock:
            if not cls._instances:
                cls._instances = {}
            if test_suite_name not in cls._instances:
                test_suite = _get_test_suite(client, test_suite_name)
                experiment = _start_experiment(client, test_suite)
                cls._instances[test_suite_name] = cls(client, experiment, test_suite)
        return cls._instances[test_suite_name]

    @property
    def name(self):
        return self._experiment.name

    def update_version(self, version: datetime.datetime) -> None:
        with self._lock:
            if self._version is None or version > self._version:
                self._version = version

    def get_version(self) -> Optional[datetime.datetime]:
        with self._lock:
            return self._version

    def submit_result(
        self, run_id: uuid.UUID, error: Optional[str] = None, skipped: bool = False
    ) -> None:
        self._executor.submit(self._submit_result, run_id, error, skipped=skipped)

    def _submit_result(
        self, run_id: uuid.UUID, error: Optional[str] = None, skipped: bool = False
    ) -> None:
        if error:
            if skipped:
                self.client.create_feedback(
                    run_id,
                    key="pass",
                    # Don't factor into aggregate score
                    score=None,
                    comment=f"Skipped: {repr(error)}",
                )
            else:
                self.client.create_feedback(
                    run_id, key="pass", score=0, comment=f"Error: {repr(error)}"
                )
        else:
            self.client.create_feedback(
                run_id,
                key="pass",
                score=1,
            )

    def sync_example(
        self, example_id: uuid.UUID, inputs: dict, outputs: dict, metadata: dict
    ) -> None:
        self._executor.submit(
            self._sync_example, example_id, inputs, outputs, metadata.copy()
        )

    def _sync_example(
        self, example_id: uuid.UUID, inputs: dict, outputs: dict, metadata: dict
    ) -> None:
        inputs_ = _serde_example_values(inputs)
        outputs_ = _serde_example_values(outputs)
        try:
            example = self.client.read_example(example_id=example_id)
            if (
                inputs_ != example.inputs
                or outputs_ != example.outputs
                or str(example.dataset_id) != str(self.id)
            ):
                self.client.update_example(
                    example_id=example.id,
                    inputs=inputs_,
                    outputs=outputs_,
                    metadata=metadata,
                    dataset_id=self.id,
                )
        except ls_utils.LangSmithNotFoundError:
            example = self.client.create_example(
                example_id=example_id,
                inputs=inputs_,
                outputs=outputs_,
                dataset_id=self.id,
                metadata=metadata,
                created_at=self._experiment.start_time,
            )
        if example.modified_at:
            self.update_version(example.modified_at)

    def wait(self):
        self._executor.shutdown(wait=True)


class _UTExtra(TypedDict, total=False):
    client: Optional[ls_client.Client]
    id: Optional[uuid.UUID]
    output_keys: Optional[Sequence[str]]
    test_suite_name: Optional[str]
    cache: Optional[str]


def _get_test_repr(func: Callable, sig: inspect.Signature) -> str:
    name = getattr(func, "__name__", None) or ""
    description = getattr(func, "__doc__", None) or ""
    if description:
        description = f" - {description.strip()}"
    return f"{name}{sig}{description}"


def _ensure_example(
    func: Callable, *args: Any, langtest_extra: _UTExtra, **kwargs: Any
) -> Tuple[_LangSmithTestSuite, uuid.UUID]:
    client = langtest_extra["client"] or ls_client.Client()
    output_keys = langtest_extra["output_keys"]
    signature = inspect.signature(func)
    inputs: dict = rh._get_inputs_safe(signature, *args, **kwargs)
    outputs = {}
    if output_keys:
        for k in output_keys:
            outputs[k] = inputs.pop(k, None)
    test_suite = _LangSmithTestSuite.from_test(
        client, func, langtest_extra.get("test_suite_name")
    )
    example_id, example_name = _get_id(func, inputs, test_suite.id)
    example_id = langtest_extra["id"] or example_id
    test_suite.sync_example(
        example_id,
        inputs,
        outputs,
        metadata={"signature": _get_test_repr(func, signature), "name": example_name},
    )
    return test_suite, example_id


def _run_test(
    func: Callable, *test_args: Any, langtest_extra: _UTExtra, **test_kwargs: Any
) -> None:
    test_suite, example_id = _ensure_example(
        func, *test_args, **test_kwargs, langtest_extra=langtest_extra
    )
    run_id = uuid.uuid4()

    def _test():
        func_inputs = rh._get_inputs_safe(
            inspect.signature(func), *test_args, **test_kwargs
        )
        with rh.trace(
            name=getattr(func, "__name__", "Test"),
            run_id=run_id,
            reference_example_id=example_id,
            inputs=func_inputs,
            project_name=test_suite.name,
            exceptions_to_handle=(SkipException,),
        ) as run_tree:
            try:
                result = func(*test_args, **test_kwargs)
                run_tree.end(
                    outputs=(
                        result
                        if result is None or isinstance(result, dict)
                        else {"output": result}
                    )
                )
            except SkipException as e:
                test_suite.submit_result(run_id, error=repr(e), skipped=True)
                run_tree.end(
                    outputs={"skipped_reason": repr(e)},
                )
                raise e
            except BaseException as e:
                test_suite.submit_result(run_id, error=repr(e))
                raise e
            try:
                test_suite.submit_result(run_id, error=None)
            except BaseException as e:
                logger.warning(f"Failed to create feedback for run_id {run_id}: {e}")

    cache_path = (
        Path(langtest_extra["cache"]) / f"{test_suite.id}.yaml"
        if langtest_extra["cache"]
        else None
    )
    current_context = rh.get_tracing_context()
    metadata = {
        **(current_context["metadata"] or {}),
        **{
            "experiment": test_suite.experiment.name,
            "reference_example_id": str(example_id),
        },
    }
    with rh.tracing_context(
        **{**current_context, "metadata": metadata}
    ), ls_utils.with_optional_cache(
        cache_path, ignore_hosts=[test_suite.client.api_url]
    ):
        _test()


async def _arun_test(
    func: Callable, *test_args: Any, langtest_extra: _UTExtra, **test_kwargs: Any
) -> None:
    test_suite, example_id = _ensure_example(
        func, *test_args, **test_kwargs, langtest_extra=langtest_extra
    )
    run_id = uuid.uuid4()

    async def _test():
        func_inputs = rh._get_inputs_safe(
            inspect.signature(func), *test_args, **test_kwargs
        )
        with rh.trace(
            name=getattr(func, "__name__", "Test"),
            run_id=run_id,
            reference_example_id=example_id,
            inputs=func_inputs,
            project_name=test_suite.name,
            exceptions_to_handle=(SkipException,),
        ) as run_tree:
            try:
                result = await func(*test_args, **test_kwargs)
                run_tree.end(
                    outputs=(
                        result
                        if result is None or isinstance(result, dict)
                        else {"output": result}
                    )
                )
            except SkipException as e:
                test_suite.submit_result(run_id, error=repr(e), skipped=True)
                run_tree.end(
                    outputs={"skipped_reason": repr(e)},
                )
                raise e
            except BaseException as e:
                test_suite.submit_result(run_id, error=repr(e))
                raise e
            try:
                test_suite.submit_result(run_id, error=None)
            except BaseException as e:
                logger.warning(f"Failed to create feedback for run_id {run_id}: {e}")

    cache_path = (
        Path(langtest_extra["cache"]) / f"{test_suite.id}.yaml"
        if langtest_extra["cache"]
        else None
    )
    current_context = rh.get_tracing_context()
    metadata = {
        **(current_context["metadata"] or {}),
        **{
            "experiment": test_suite.experiment.name,
            "reference_example_id": str(example_id),
        },
    }
    with rh.tracing_context(
        **{**current_context, "metadata": metadata}
    ), ls_utils.with_optional_cache(
        cache_path, ignore_hosts=[test_suite.client.api_url]
    ):
        await _test()


# For backwards compatibility
unit = test
