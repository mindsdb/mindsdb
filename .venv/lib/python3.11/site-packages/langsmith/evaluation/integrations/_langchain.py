from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Optional, TypedDict, Union

from langsmith.evaluation.evaluator import run_evaluator
from langsmith.run_helpers import traceable
from langsmith.schemas import Example, Run

if TYPE_CHECKING:
    from langchain.evaluation.schema import StringEvaluator

    from langsmith.evaluation.evaluator import RunEvaluator


class SingleEvaluatorInput(TypedDict):
    """The input to a `StringEvaluator`."""

    prediction: str
    """The prediction string."""
    reference: Optional[Any]
    """The reference string."""
    input: Optional[str]
    """The input string."""


class LangChainStringEvaluator:
    r"""A class for wrapping a LangChain StringEvaluator.

    Requires the `langchain` package to be installed.

    Attributes:
        evaluator (StringEvaluator): The underlying StringEvaluator OR the name
            of the evaluator to load.

    Methods:
        as_run_evaluator() -> RunEvaluator:
            Convert the LangChainStringEvaluator to a RunEvaluator.

    Examples:
        Creating a simple LangChainStringEvaluator:

        >>> evaluator = LangChainStringEvaluator("exact_match")

        Converting a LangChainStringEvaluator to a RunEvaluator:

        >>> from langsmith.evaluation import LangChainStringEvaluator
        >>> evaluator = LangChainStringEvaluator(
        ...     "criteria",
        ...     config={
        ...         "criteria": {
        ...             "usefulness": "The prediction is useful if"
        ...             " it is correct and/or asks a useful followup question."
        ...         },
        ...     },
        ... )
        >>> run_evaluator = evaluator.as_run_evaluator()
        >>> run_evaluator  # doctest: +ELLIPSIS
        <DynamicRunEvaluator ...>

        Customizing the LLM model used by the evaluator:

        >>> from langsmith.evaluation import LangChainStringEvaluator
        >>> from langchain_anthropic import ChatAnthropic
        >>> evaluator = LangChainStringEvaluator(
        ...     "criteria",
        ...     config={
        ...         "criteria": {
        ...             "usefulness": "The prediction is useful if"
        ...             " it is correct and/or asks a useful followup question."
        ...         },
        ...         "llm": ChatAnthropic(model="claude-3-opus-20240229"),
        ...     },
        ... )
        >>> run_evaluator = evaluator.as_run_evaluator()
        >>> run_evaluator  # doctest: +ELLIPSIS
        <DynamicRunEvaluator ...>

        Using the `evaluate` API with different evaluators:
        >>> def prepare_data(run: Run, example: Example):
        ...     # Convert the evaluation data into the format expected by the evaluator
        ...     # Only required for datasets with multiple inputs/output keys
        ...     return {
        ...         "prediction": run.outputs["prediction"],
        ...         "reference": example.outputs["answer"],
        ...         "input": str(example.inputs),
        ...     }
        >>> import re
        >>> from langchain_anthropic import ChatAnthropic
        >>> import langsmith
        >>> from langsmith.evaluation import LangChainStringEvaluator, evaluate
        >>> criteria_evaluator = LangChainStringEvaluator(
        ...     "criteria",
        ...     config={
        ...         "criteria": {
        ...             "usefulness": "The prediction is useful if it is correct"
        ...             " and/or asks a useful followup question."
        ...         },
        ...         "llm": ChatAnthropic(model="claude-3-opus-20240229"),
        ...     },
        ...     prepare_data=prepare_data,
        ... )
        >>> embedding_evaluator = LangChainStringEvaluator("embedding_distance")
        >>> exact_match_evaluator = LangChainStringEvaluator("exact_match")
        >>> regex_match_evaluator = LangChainStringEvaluator(
        ...     "regex_match", config={"flags": re.IGNORECASE}, prepare_data=prepare_data
        ... )
        >>> scoring_evaluator = LangChainStringEvaluator(
        ...     "labeled_score_string",
        ...     config={
        ...         "criteria": {
        ...             "accuracy": "Score 1: Completely inaccurate\nScore 5: Somewhat accurate\nScore 10: Completely accurate"
        ...         },
        ...         "normalize_by": 10,
        ...     },
        ...     prepare_data=prepare_data,
        ... )
        >>> string_distance_evaluator = LangChainStringEvaluator(
        ...     "string_distance",
        ...     config={"distance_metric": "levenshtein"},
        ...     prepare_data=prepare_data,
        ... )
        >>> from langsmith import Client
        >>> client = Client()
        >>> results = evaluate(
        ...     lambda inputs: {"prediction": "foo"},
        ...     data=client.list_examples(dataset_name="Evaluate Examples", limit=1),
        ...     evaluators=[
        ...         embedding_evaluator,
        ...         criteria_evaluator,
        ...         exact_match_evaluator,
        ...         regex_match_evaluator,
        ...         scoring_evaluator,
        ...         string_distance_evaluator,
        ...     ],
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...
    """  # noqa: E501

    def __init__(
        self,
        evaluator: Union[StringEvaluator, str],
        *,
        config: Optional[dict] = None,
        prepare_data: Optional[
            Callable[[Run, Optional[Example]], SingleEvaluatorInput]
        ] = None,
    ):
        """Initialize a LangChainStringEvaluator.

        See: https://api.python.langchain.com/en/latest/evaluation/langchain.evaluation.schema.StringEvaluator.html#langchain-evaluation-schema-stringevaluator

        Args:
            evaluator (StringEvaluator): The underlying StringEvaluator.
        """
        from langchain.evaluation.schema import StringEvaluator  # noqa: F811

        if isinstance(evaluator, StringEvaluator):
            self.evaluator = evaluator
        elif isinstance(evaluator, str):
            from langchain.evaluation import load_evaluator  # noqa: F811

            self.evaluator = load_evaluator(evaluator, **(config or {}))  # type: ignore[assignment, arg-type]
        else:
            raise NotImplementedError(f"Unsupported evaluator type: {type(evaluator)}")

        self._prepare_data = prepare_data

    def as_run_evaluator(
        self,
    ) -> RunEvaluator:
        """Convert the LangChainStringEvaluator to a RunEvaluator.

        This is the object used in the LangSmith `evaluate` API.

        Returns:
            RunEvaluator: The converted RunEvaluator.
        """
        input_str = (
            "\n       \"input\": example.inputs['input'],"
            if self.evaluator.requires_input
            else ""
        )
        reference_str = (
            "\n       \"reference\": example.outputs['expected']"
            if self.evaluator.requires_reference
            else ""
        )
        customization_error_str = f"""
def prepare_data(run, example):
    return {{
        "prediction": run.outputs['my_output'],{reference_str}{input_str}
    }}
evaluator = LangChainStringEvaluator(..., prepare_data=prepare_data)
"""

        @traceable
        def prepare_evaluator_inputs(
            run: Run, example: Optional[Example] = None
        ) -> SingleEvaluatorInput:
            if run.outputs and len(run.outputs) > 1:
                raise ValueError(
                    f"Evaluator {self.evaluator} only supports a single prediction "
                    "key. Please ensure that the run has a single output."
                    " Or initialize with a prepare_data:\n"
                    f"{customization_error_str}"
                )
            if (
                self.evaluator.requires_reference
                and example
                and example.outputs
                and len(example.outputs) > 1
            ):
                raise ValueError(
                    f"Evaluator {self.evaluator} nly supports a single reference key. "
                    "Please ensure that the example has a single output."
                    " Or create a custom evaluator yourself:\n"
                    f"{customization_error_str}"
                )
            if (
                self.evaluator.requires_input
                and example
                and example.inputs
                and len(example.inputs) > 1
            ):
                raise ValueError(
                    f"Evaluator {self.evaluator} only supports a single input key. "
                    "Please ensure that the example has a single input."
                    " Or initialize with a prepare_data:\n"
                    f"{customization_error_str}"
                )

            return SingleEvaluatorInput(
                prediction=next(iter(run.outputs.values())),  # type: ignore[union-attr]
                reference=(
                    next(iter(example.outputs.values()))
                    if (
                        self.evaluator.requires_reference
                        and example
                        and example.outputs
                    )
                    else None
                ),
                input=(
                    next(iter(example.inputs.values()))
                    if (self.evaluator.requires_input and example and example.inputs)
                    else None
                ),
            )

        @traceable(name=self.evaluator.evaluation_name)
        def evaluate(run: Run, example: Optional[Example] = None) -> dict:
            eval_inputs = (
                prepare_evaluator_inputs(run, example)
                if self._prepare_data is None
                else self._prepare_data(run, example)
            )
            results = self.evaluator.evaluate_strings(**eval_inputs)
            return {"key": self.evaluator.evaluation_name, **results}

        return run_evaluator(evaluate)
