"""Make approximate assertions as "expectations" on test results.

This module is designed to be used within test cases decorated with the `@test` decorator
It allows you to log scores about a test case and optionally make assertions that log as
"expectation" feedback to LangSmith.

Example usage:

    from langsmith import expect, test

    @test
    def test_output_semantically_close():
        response = oai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Say hello!"},
            ],
        )
        response_txt = response.choices[0].message.content
        # Intended usage
        expect.embedding_distance(
            prediction=response_txt,
            reference="Hello!",
        ).to_be_less_than(0.9)

        # Score the test case
        matcher = expect.edit_distance(
            prediction=response_txt,
            reference="Hello!",
        )
        # Apply an assertion and log 'expectation' feedback to LangSmith
        matcher.to_be_less_than(1)

        # You can also directly make assertions on values directly
        expect.value(response_txt).to_contain("Hello!")
        # Or using a custom check
        expect.value(response_txt).against(lambda x: "Hello" in x)

        # You can even use this for basic metric logging within tests

        expect.score(0.8)
        expect.score(0.7, key="similarity").to_be_greater_than(0.7)
"""  # noqa: E501

from __future__ import annotations

import atexit
import concurrent.futures
import inspect
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Optional,
    Union,
    overload,
)

from langsmith import client as ls_client
from langsmith import run_helpers as rh
from langsmith import utils as ls_utils

if TYPE_CHECKING:
    from langsmith._internal._edit_distance import EditDistanceConfig
    from langsmith._internal._embedding_distance import EmbeddingConfig


# Sentinel class used until PEP 0661 is accepted
class _NULL_SENTRY:
    """A sentinel singleton class used to distinguish omitted keyword arguments
    from those passed in with the value None (which may have different behavior).
    """  # noqa: D205

    def __bool__(self) -> Literal[False]:
        return False

    def __repr__(self) -> str:
        return "NOT_GIVEN"


NOT_GIVEN = _NULL_SENTRY()


class _Matcher:
    """A class for making assertions on expectation values."""

    def __init__(
        self,
        client: Optional[ls_client.Client],
        key: str,
        value: Any,
        _executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
        run_id: Optional[str] = None,
    ):
        self._client = client
        self.key = key
        self.value = value
        self._executor = _executor or concurrent.futures.ThreadPoolExecutor(
            max_workers=3
        )
        rt = rh.get_current_run_tree()
        self._run_id = rt.trace_id if rt else run_id

    def _submit_feedback(self, score: int, message: Optional[str] = None) -> None:
        if not ls_utils.test_tracking_is_disabled():
            if not self._client:
                self._client = ls_client.Client()
            self._executor.submit(
                self._client.create_feedback,
                run_id=self._run_id,
                key="expectation",
                score=score,
                comment=message,
            )

    def _assert(self, condition: bool, message: str, method_name: str) -> None:
        try:
            assert condition, message
            self._submit_feedback(1, message=f"Success: {self.key}.{method_name}")
        except AssertionError as e:
            self._submit_feedback(0, repr(e))
            raise e from None

    def to_be_less_than(self, value: float) -> None:
        """Assert that the expectation value is less than the given value.

        Args:
            value: The value to compare against.

        Raises:
            AssertionError: If the expectation value is not less than the given value.
        """
        self._assert(
            self.value < value,
            f"Expected {self.key} to be less than {value}, but got {self.value}",
            "to_be_less_than",
        )

    def to_be_greater_than(self, value: float) -> None:
        """Assert that the expectation value is greater than the given value.

        Args:
            value: The value to compare against.

        Raises:
            AssertionError: If the expectation value is not
            greater than the given value.
        """
        self._assert(
            self.value > value,
            f"Expected {self.key} to be greater than {value}, but got {self.value}",
            "to_be_greater_than",
        )

    def to_be_between(self, min_value: float, max_value: float) -> None:
        """Assert that the expectation value is between the given min and max values.

        Args:
            min_value: The minimum value (exclusive).
            max_value: The maximum value (exclusive).

        Raises:
            AssertionError: If the expectation value
                is not between the given min and max.
        """
        self._assert(
            min_value < self.value < max_value,
            f"Expected {self.key} to be between {min_value} and {max_value},"
            f" but got {self.value}",
            "to_be_between",
        )

    def to_be_approximately(self, value: float, precision: int = 2) -> None:
        """Assert that the expectation value is approximately equal to the given value.

        Args:
            value: The value to compare against.
            precision: The number of decimal places to round to for comparison.

        Raises:
            AssertionError: If the rounded expectation value
                does not equal the rounded given value.
        """
        self._assert(
            round(self.value, precision) == round(value, precision),
            f"Expected {self.key} to be approximately {value}, but got {self.value}",
            "to_be_approximately",
        )

    def to_equal(self, value: float) -> None:
        """Assert that the expectation value equals the given value.

        Args:
            value: The value to compare against.

        Raises:
            AssertionError: If the expectation value does
                not exactly equal the given value.
        """
        self._assert(
            self.value == value,
            f"Expected {self.key} to be equal to {value}, but got {self.value}",
            "to_equal",
        )

    def to_be_none(self) -> None:
        """Assert that the expectation value is None.

        Raises:
            AssertionError: If the expectation value is not None.
        """
        self._assert(
            self.value is None,
            f"Expected {self.key} to be None, but got {self.value}",
            "to_be_none",
        )

    def to_contain(self, value: Any) -> None:
        """Assert that the expectation value contains the given value.

        Args:
            value: The value to check for containment.

        Raises:
            AssertionError: If the expectation value does not contain the given value.
        """
        self._assert(
            value in self.value,
            f"Expected {self.key} to contain {value}, but it does not",
            "to_contain",
        )

    # Custom assertions
    def against(self, func: Callable, /) -> None:
        """Assert the expectation value against a custom function.

        Args:
            func: A custom function that takes the expectation value as input.

        Raises:
            AssertionError: If the custom function returns False.
        """
        func_signature = inspect.signature(func)
        self._assert(
            func(self.value),
            f"Assertion {func_signature} failed for {self.key}",
            "against",
        )


class _Expect:
    """A class for setting expectations on test results."""

    def __init__(self, *, client: Optional[ls_client.Client] = None):
        self._client = client
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
        atexit.register(self.executor.shutdown, wait=True)

    def embedding_distance(
        self,
        prediction: str,
        reference: str,
        *,
        config: Optional[EmbeddingConfig] = None,
    ) -> _Matcher:
        """Compute the embedding distance between the prediction and reference.

        This logs the embedding distance to LangSmith and returns a `_Matcher` instance
        for making assertions on the distance value.

        By default, this uses the OpenAI API for computing embeddings.

        Args:
            prediction: The predicted string to compare.
            reference: The reference string to compare against.
            config: Optional configuration for the embedding distance evaluator.
                Supported options:
                - `encoder`: A custom encoder function to encode the list of input
                     strings to embeddings. Defaults to the OpenAI API.
                - `metric`: The distance metric to use for comparison.
                    Supported values: "cosine", "euclidean", "manhattan",
                    "chebyshev", "hamming".

        Returns:
            A `_Matcher` instance for the embedding distance value.


        Examples:
            >>> expect.embedding_distance(
            ...     prediction="hello",
            ...     reference="hi",
            ... ).to_be_less_than(1.0)
        """  # noqa: E501
        from langsmith._internal._embedding_distance import EmbeddingDistance

        config = config or {}
        encoder_func = "custom" if config.get("encoder") else "openai"
        evaluator = EmbeddingDistance(config=config)
        score = evaluator.evaluate(prediction=prediction, reference=reference)
        src_info = {"encoder": encoder_func, "metric": evaluator.distance}
        self._submit_feedback(
            "embedding_distance",
            {
                "score": score,
                "source_info": src_info,
                "comment": f"Using {encoder_func}, Metric: {evaluator.distance}",
            },
        )
        return _Matcher(
            self._client, "embedding_distance", score, _executor=self.executor
        )

    def edit_distance(
        self,
        prediction: str,
        reference: str,
        *,
        config: Optional[EditDistanceConfig] = None,
    ) -> _Matcher:
        """Compute the string distance between the prediction and reference.

        This logs the string distance (Damerau-Levenshtein) to LangSmith and returns
        a `_Matcher` instance for making assertions on the distance value.

        This depends on the `rapidfuzz` package for string distance computation.

        Args:
            prediction: The predicted string to compare.
            reference: The reference string to compare against.
            config: Optional configuration for the string distance evaluator.
                Supported options:
                - `metric`: The distance metric to use for comparison.
                    Supported values: "damerau_levenshtein", "levenshtein",
                    "jaro", "jaro_winkler", "hamming", "indel".
                - `normalize_score`: Whether to normalize the score between 0 and 1.

        Returns:
            A `_Matcher` instance for the string distance value.

        Examples:
            >>> expect.edit_distance("hello", "helo").to_be_less_than(1)
        """
        from langsmith._internal._edit_distance import EditDistance

        config = config or {}
        metric = config.get("metric") or "damerau_levenshtein"
        normalize = config.get("normalize_score", True)
        evaluator = EditDistance(config=config)
        score = evaluator.evaluate(prediction=prediction, reference=reference)
        src_info = {"metric": metric, "normalize": normalize}
        self._submit_feedback(
            "edit_distance",
            {
                "score": score,
                "source_info": src_info,
                "comment": f"Using {metric}, Normalize: {normalize}",
            },
        )
        return _Matcher(
            self._client,
            "edit_distance",
            score,
            _executor=self.executor,
        )

    def value(self, value: Any) -> _Matcher:
        """Create a `_Matcher` instance for making assertions on the given value.

        Args:
            value: The value to make assertions on.

        Returns:
            A `_Matcher` instance for the given value.

        Examples:
           >>> expect.value(10).to_be_less_than(20)
        """
        return _Matcher(self._client, "value", value, _executor=self.executor)

    def score(
        self,
        score: Union[float, int],
        *,
        key: str = "score",
        source_run_id: Optional[ls_client.ID_TYPE] = None,
        comment: Optional[str] = None,
    ) -> _Matcher:
        """Log a numeric score to LangSmith.

        Args:
            score: The score value to log.
            key: The key to use for logging the score. Defaults to "score".

        Examples:
            >>> expect.score(0.8)  # doctest: +ELLIPSIS
            <langsmith._expect._Matcher object at ...>

            >>> expect.score(0.8, key="similarity").to_be_greater_than(0.7)
        """
        self._submit_feedback(
            key,
            {
                "score": score,
                "source_info": {"method": "expect.score"},
                "source_run_id": source_run_id,
                "comment": comment,
            },
        )
        return _Matcher(self._client, key, score, _executor=self.executor)

    ## Private Methods

    @overload
    def __call__(self, value: Any, /) -> _Matcher: ...

    @overload
    def __call__(self, /, *, client: ls_client.Client) -> _Expect: ...

    def __call__(
        self,
        value: Optional[Any] = NOT_GIVEN,
        /,
        client: Optional[ls_client.Client] = None,
    ) -> Union[_Expect, _Matcher]:
        expected = _Expect(client=client)
        if value is not NOT_GIVEN:
            return expected.value(value)
        return expected

    def _submit_feedback(self, key: str, results: dict):
        current_run = rh.get_current_run_tree()
        run_id = current_run.trace_id if current_run else None
        if not ls_utils.test_tracking_is_disabled():
            if not self._client:
                self._client = ls_client.Client()
            self.executor.submit(
                self._client.create_feedback, run_id=run_id, key=key, **results
            )


expect = _Expect()

__all__ = ["expect"]
