"""Evaluation Helpers."""

from typing import TYPE_CHECKING, Any, List

if TYPE_CHECKING:
    from typing import List

    from langsmith.evaluation._arunner import (
        aevaluate,
        aevaluate_existing,
    )
    from langsmith.evaluation._runner import (
        evaluate,
        evaluate_comparative,
        evaluate_existing,
    )
    from langsmith.evaluation.evaluator import (
        EvaluationResult,
        EvaluationResults,
        RunEvaluator,
        run_evaluator,
    )
    from langsmith.evaluation.integrations._langchain import LangChainStringEvaluator


def __getattr__(name: str) -> Any:
    # TODO: Use importlib
    if name == "evaluate":
        from langsmith.evaluation._runner import evaluate

        return evaluate
    elif name == "evaluate_existing":
        from langsmith.evaluation._runner import evaluate_existing

        return evaluate_existing
    elif name == "aevaluate":
        from langsmith.evaluation._arunner import aevaluate

        return aevaluate
    elif name == "aevaluate_existing":
        from langsmith.evaluation._arunner import aevaluate_existing

        return aevaluate_existing
    elif name == "evaluate_comparative":
        from langsmith.evaluation._runner import evaluate_comparative

        return evaluate_comparative
    elif name == "EvaluationResult":
        from langsmith.evaluation.evaluator import EvaluationResult

        return EvaluationResult
    elif name == "EvaluationResults":
        from langsmith.evaluation.evaluator import EvaluationResults

        return EvaluationResults
    elif name == "RunEvaluator":
        from langsmith.evaluation.evaluator import RunEvaluator

        return RunEvaluator
    elif name == "run_evaluator":
        from langsmith.evaluation.evaluator import run_evaluator

        return run_evaluator
    elif name == "StringEvaluator":
        from langsmith.evaluation.string_evaluator import StringEvaluator

        return StringEvaluator
    elif name == "LangChainStringEvaluator":
        from langsmith.evaluation.integrations._langchain import (
            LangChainStringEvaluator,
        )

        return LangChainStringEvaluator
    raise AttributeError(f"module {__name__} has no attribute {name}")


__all__ = [
    "run_evaluator",
    "EvaluationResult",
    "EvaluationResults",
    "RunEvaluator",
    "StringEvaluator",
    "aevaluate",
    "aevaluate_existing",
    "evaluate",
    "evaluate_existing",
    "evaluate_comparative",
    "LangChainStringEvaluator",
]


def __dir__() -> List[str]:
    return __all__
