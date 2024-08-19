"""LangSmith Client."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from langsmith._expect import expect
    from langsmith._testing import test, unit
    from langsmith.client import Client
    from langsmith.evaluation import aevaluate, evaluate
    from langsmith.evaluation.evaluator import EvaluationResult, RunEvaluator
    from langsmith.run_helpers import (
        get_current_run_tree,
        get_tracing_context,
        trace,
        traceable,
        tracing_context,
    )
    from langsmith.run_trees import RunTree


def __getattr__(name: str) -> Any:
    if name == "__version__":
        try:
            from importlib import metadata

            return metadata.version(__package__)
        except metadata.PackageNotFoundError:
            return ""
    elif name == "Client":
        from langsmith.client import Client

        return Client
    elif name == "RunTree":
        from langsmith.run_trees import RunTree

        return RunTree
    elif name == "EvaluationResult":
        from langsmith.evaluation.evaluator import EvaluationResult

        return EvaluationResult
    elif name == "RunEvaluator":
        from langsmith.evaluation.evaluator import RunEvaluator

        return RunEvaluator
    elif name == "trace":
        from langsmith.run_helpers import trace

        return trace
    elif name == "traceable":
        from langsmith.run_helpers import traceable

        return traceable

    elif name == "test":
        from langsmith._testing import test

        return test

    elif name == "expect":
        from langsmith._expect import expect

        return expect
    elif name == "evaluate":
        from langsmith.evaluation import evaluate

        return evaluate
    elif name == "aevaluate":
        from langsmith.evaluation import aevaluate

        return aevaluate
    elif name == "tracing_context":
        from langsmith.run_helpers import tracing_context

        return tracing_context

    elif name == "get_tracing_context":
        from langsmith.run_helpers import get_tracing_context

        return get_tracing_context

    elif name == "get_current_run_tree":
        from langsmith.run_helpers import get_current_run_tree

        return get_current_run_tree

    elif name == "unit":
        from langsmith._testing import unit

        return unit

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Client",
    "RunTree",
    "__version__",
    "EvaluationResult",
    "RunEvaluator",
    "anonymizer",
    "traceable",
    "trace",
    "unit",
    "test",
    "expect",
    "evaluate",
    "aevaluate",
    "tracing_context",
    "get_tracing_context",
    "get_current_run_tree",
]
