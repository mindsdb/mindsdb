"""V2 Evaluation Interface."""

from __future__ import annotations

import collections
import concurrent.futures as cf
import datetime
import functools
import itertools
import logging
import pathlib
import random
import threading
import uuid
from contextvars import copy_context
from typing import (
    Awaitable,
    Callable,
    DefaultDict,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from requests import HTTPError
from typing_extensions import TypedDict

import langsmith
from langsmith import env as ls_env
from langsmith import run_helpers as rh
from langsmith import run_trees, schemas
from langsmith import utils as ls_utils
from langsmith.evaluation.evaluator import (
    ComparisonEvaluationResult,
    EvaluationResult,
    EvaluationResults,
    RunEvaluator,
    comparison_evaluator,
    run_evaluator,
)
from langsmith.evaluation.integrations import LangChainStringEvaluator

logger = logging.getLogger(__name__)

TARGET_T = Callable[[dict], dict]
# Data format: dataset-name, dataset_id, or examples
DATA_T = Union[str, uuid.UUID, Iterable[schemas.Example]]
# Summary evaluator runs over the whole dataset
# and reports aggregate metric(s)
SUMMARY_EVALUATOR_T = Callable[
    [Sequence[schemas.Run], Sequence[schemas.Example]],
    Union[EvaluationResult, EvaluationResults],
]
# Row-level evaluator
EVALUATOR_T = Union[
    RunEvaluator,
    Callable[[schemas.Run, Optional[schemas.Example]], EvaluationResult],
]
AEVALUATOR_T = Union[
    Callable[
        [schemas.Run, Optional[schemas.Example]],
        Awaitable[Union[EvaluationResult, EvaluationResults]],
    ],
]


def evaluate(
    target: TARGET_T,
    /,
    data: DATA_T,
    evaluators: Optional[Sequence[EVALUATOR_T]] = None,
    summary_evaluators: Optional[Sequence[SUMMARY_EVALUATOR_T]] = None,
    metadata: Optional[dict] = None,
    experiment_prefix: Optional[str] = None,
    description: Optional[str] = None,
    max_concurrency: Optional[int] = None,
    num_repetitions: int = 1,
    client: Optional[langsmith.Client] = None,
    blocking: bool = True,
) -> ExperimentResults:
    r"""Evaluate a target system or function on a given dataset.

    Args:
        target (TARGET_T): The target system or function to evaluate.
        data (DATA_T): The dataset to evaluate on. Can be a dataset name, a list of
            examples, or a generator of examples.
        evaluators (Optional[Sequence[EVALUATOR_T]]): A list of evaluators to run
            on each example. Defaults to None.
        summary_evaluators (Optional[Sequence[SUMMARY_EVALUATOR_T]]): A list of summary
            evaluators to run on the entire dataset. Defaults to None.
        metadata (Optional[dict]): Metadata to attach to the experiment.
            Defaults to None.
        experiment_prefix (Optional[str]): A prefix to provide for your experiment name.
            Defaults to None.
        description (Optional[str]): A free-form text description for the experiment.
        max_concurrency (Optional[int]): The maximum number of concurrent
            evaluations to run. Defaults to None.
        client (Optional[langsmith.Client]): The LangSmith client to use.
            Defaults to None.
        blocking (bool): Whether to block until the evaluation is complete.
            Defaults to True.
        num_repetitions (int): The number of times to run the evaluation.
            Each item in the dataset will be run and evaluated this many times.
            Defaults to 1.

    Returns:
        ExperimentResults: The results of the evaluation.

    Examples:
        Prepare the dataset:

        >>> from typing import Sequence
        >>> from langsmith import Client
        >>> from langsmith.evaluation import evaluate
        >>> from langsmith.schemas import Example, Run
        >>> client = Client()
        >>> client.clone_public_dataset(
        ...     "https://smith.langchain.com/public/419dcab2-1d66-4b94-8901-0357ead390df/d"
        ... )
        >>> dataset_name = "Evaluate Examples"

        Basic usage:

        >>> def accuracy(run: Run, example: Example):
        ...     # Row-level evaluator for accuracy.
        ...     pred = run.outputs["output"]
        ...     expected = example.outputs["answer"]
        ...     return {"score": expected.lower() == pred.lower()}
        >>> def precision(runs: Sequence[Run], examples: Sequence[Example]):
        ...     # Experiment-level evaluator for precision.
        ...     # TP / (TP + FP)
        ...     predictions = [run.outputs["output"].lower() for run in runs]
        ...     expected = [example.outputs["answer"].lower() for example in examples]
        ...     # yes and no are the only possible answers
        ...     tp = sum([p == e for p, e in zip(predictions, expected) if p == "yes"])
        ...     fp = sum([p == "yes" and e == "no" for p, e in zip(predictions, expected)])
        ...     return {"score": tp / (tp + fp)}
        >>> def predict(inputs: dict) -> dict:
        ...     # This can be any function or just an API call to your app.
        ...     return {"output": "Yes"}
        >>> results = evaluate(
        ...     predict,
        ...     data=dataset_name,
        ...     evaluators=[accuracy],
        ...     summary_evaluators=[precision],
        ...     experiment_prefix="My Experiment",
        ...     description="Evaluating the accuracy of a simple prediction model.",
        ...     metadata={
        ...         "my-prompt-version": "abcd-1234",
        ...     },
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...

        Evaluating over only a subset of the examples

        >>> experiment_name = results.experiment_name
        >>> examples = client.list_examples(dataset_name=dataset_name, limit=5)
        >>> results = evaluate(
        ...     predict,
        ...     data=examples,
        ...     evaluators=[accuracy],
        ...     summary_evaluators=[precision],
        ...     experiment_prefix="My Experiment",
        ...     description="Just testing a subset synchronously.",
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...

        Streaming each prediction to more easily + eagerly debug.

        >>> results = evaluate(
        ...     predict,
        ...     data=dataset_name,
        ...     evaluators=[accuracy],
        ...     summary_evaluators=[precision],
        ...     description="I don't even have to block!",
        ...     blocking=False,
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...
        >>> for i, result in enumerate(results):  # doctest: +ELLIPSIS
        ...     pass

        Using the `evaluate` API with an off-the-shelf LangChain evaluator:

        >>> from langsmith.evaluation import LangChainStringEvaluator
        >>> def prepare_criteria_data(run: Run, example: Example):
        ...     return {
        ...         "prediction": run.outputs["output"],
        ...         "reference": example.outputs["answer"],
        ...         "input": str(example.inputs),
        ...     }
        >>> results = evaluate(
        ...     predict,
        ...     data=dataset_name,
        ...     evaluators=[
        ...         accuracy,
        ...         LangChainStringEvaluator("embedding_distance"),
        ...         LangChainStringEvaluator(
        ...             "labeled_criteria",
        ...             config={
        ...                 "criteria": {
        ...                     "usefulness": "The prediction is useful if it is correct"
        ...                     " and/or asks a useful followup question."
        ...                 },
        ...             },
        ...             prepare_data=prepare_criteria_data,
        ...         ),
        ...     ],
        ...     description="Evaluating with off-the-shelf LangChain evaluators.",
        ...     summary_evaluators=[precision],
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...

        Evaluating a LangChain object:

        >>> from langchain_core.runnables import chain as as_runnable
        >>> @as_runnable
        ... def nested_predict(inputs):
        ...     return {"output": "Yes"}
        >>> @as_runnable
        ... def lc_predict(inputs):
        ...     return nested_predict.invoke(inputs)
        >>> results = evaluate(
        ...     lc_predict.invoke,
        ...     data=dataset_name,
        ...     evaluators=[accuracy],
        ...     description="This time we're evaluating a LangChain object.",
        ...     summary_evaluators=[precision],
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...
    """  # noqa: E501
    return _evaluate(
        target,
        data=data,
        evaluators=evaluators,
        summary_evaluators=summary_evaluators,
        metadata=metadata,
        experiment_prefix=experiment_prefix,
        description=description,
        max_concurrency=max_concurrency,
        num_repetitions=num_repetitions,
        client=client,
        blocking=blocking,
    )


def evaluate_existing(
    experiment: Union[str, uuid.UUID],
    /,
    evaluators: Optional[Sequence[EVALUATOR_T]] = None,
    summary_evaluators: Optional[Sequence[SUMMARY_EVALUATOR_T]] = None,
    metadata: Optional[dict] = None,
    max_concurrency: Optional[int] = None,
    client: Optional[langsmith.Client] = None,
    load_nested: bool = False,
    blocking: bool = True,
) -> ExperimentResults:
    r"""Evaluate existing experiment runs.

    Args:
        experiment (Union[str, uuid.UUID]): The identifier of the experiment to evaluate.
        data (DATA_T): The data to use for evaluation.
        evaluators (Optional[Sequence[EVALUATOR_T]]): Optional sequence of evaluators to use for individual run evaluation.
        summary_evaluators (Optional[Sequence[SUMMARY_EVALUATOR_T]]): Optional sequence of evaluators
            to apply over the entire dataset.
        metadata (Optional[dict]): Optional metadata to include in the evaluation results.
        max_concurrency (Optional[int]): Optional maximum number of concurrent evaluations.
        client (Optional[langsmith.Client]): Optional Langsmith client to use for evaluation.
        load_nested: Whether to load all child runs for the experiment.
            Default is to only load the top-level root runs.
        blocking (bool): Whether to block until evaluation is complete.

    Returns:
        ExperimentResults: The evaluation results.

    Environment:
        - LANGSMITH_TEST_CACHE: If set, API calls will be cached to disk to save time and
            cost during testing. Recommended to commit the cache files to your repository
            for faster CI/CD runs.
            Requires the 'langsmith[vcr]' package to be installed.

    Examples:
        >>> from langsmith.evaluation import evaluate, evaluate_existing
        >>> dataset_name = "Evaluate Examples"
        >>> def predict(inputs: dict) -> dict:
        ...     # This can be any function or just an API call to your app.
        ...     return {"output": "Yes"}
        >>> # First run inference on the dataset
        ... results = evaluate(
        ...     predict,
        ...     data=dataset_name,
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...
        >>> # Then apply evaluators to the experiment
        ... def accuracy(run: Run, example: Example):
        ...     # Row-level evaluator for accuracy.
        ...     pred = run.outputs["output"]
        ...     expected = example.outputs["answer"]
        ...     return {"score": expected.lower() == pred.lower()}
        >>> def precision(runs: Sequence[Run], examples: Sequence[Example]):
        ...     # Experiment-level evaluator for precision.
        ...     # TP / (TP + FP)
        ...     predictions = [run.outputs["output"].lower() for run in runs]
        ...     expected = [example.outputs["answer"].lower() for example in examples]
        ...     # yes and no are the only possible answers
        ...     tp = sum([p == e for p, e in zip(predictions, expected) if p == "yes"])
        ...     fp = sum([p == "yes" and e == "no" for p, e in zip(predictions, expected)])
        ...     return {"score": tp / (tp + fp)}
        >>> experiment_name = (
        ...     results.experiment_name
        ... )  # Can use the returned experiment name
        >>> experiment_name = "My Experiment:64e6e91"  # Or manually specify
        >>> results = evaluate_existing(
        ...     experiment_name,
        ...     summary_evaluators=[precision],
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...
    """  # noqa: E501
    client = client or langsmith.Client()
    project = _load_experiment(experiment, client)
    runs = _load_traces(experiment, client, load_nested=load_nested)
    data = list(
        client.list_examples(
            dataset_id=project.reference_dataset_id,
            as_of=project.metadata.get("dataset_version"),
        )
    )
    runs = sorted(runs, key=lambda r: str(r.reference_example_id))
    data = sorted(data, key=lambda d: str(d.id))
    return _evaluate(
        runs,
        data=data,
        evaluators=evaluators,
        summary_evaluators=summary_evaluators,
        metadata=metadata,
        max_concurrency=max_concurrency,
        client=client,
        blocking=blocking,
    )


class ExperimentResultRow(TypedDict):
    run: schemas.Run
    example: schemas.Example
    evaluation_results: EvaluationResults


class ExperimentResults:
    """Represents the results of an evaluate() call.

    This class provides an iterator interface to iterate over the experiment results
    as they become available. It also provides methods to access the experiment name,
    the number of results, and to wait for the results to be processed.

    Methods:
        experiment_name() -> str: Returns the name of the experiment.
        wait() -> None: Waits for the experiment data to be processed.
    """

    def __init__(
        self,
        experiment_manager: _ExperimentManager,
    ):
        self._manager = experiment_manager
        self._results: List[ExperimentResultRow] = []
        self._lock = threading.RLock()
        self._thread = threading.Thread(
            target=lambda: self._process_data(self._manager)
        )
        self._thread.start()

    @property
    def experiment_name(self) -> str:
        return self._manager.experiment_name

    def __iter__(self) -> Iterator[ExperimentResultRow]:
        processed_count = 0
        while True:
            with self._lock:
                if processed_count < len(self._results):
                    yield self._results[processed_count]
                    processed_count += 1
                elif not self._thread.is_alive():
                    break

    def _process_data(self, manager: _ExperimentManager) -> None:
        tqdm = _load_tqdm()
        results = manager.get_results()
        for item in tqdm(results):
            with self._lock:
                self._results.append(item)
        summary_scores = manager.get_summary_scores()
        with self._lock:
            self._summary_results = summary_scores

    def __len__(self) -> int:
        return len(self._results)

    def __repr__(self) -> str:
        return f"<ExperimentResults {self.experiment_name}>"

    def wait(self) -> None:
        """Wait for the evaluation runner to complete.

        This method blocks the current thread until the evaluation runner has
        finished its execution.
        """
        self._thread.join()


## Public API for Comparison Experiments

# Row-level evaluator
COMPARATIVE_EVALUATOR_T = Callable[
    [Sequence[schemas.Run], Optional[schemas.Example]],
    Union[
        Union[ComparisonEvaluationResult, dict],
        Awaitable[Union[ComparisonEvaluationResult, dict]],
    ],
]


def evaluate_comparative(
    experiments: Tuple[Union[str, uuid.UUID], Union[str, uuid.UUID]],
    /,
    evaluators: Sequence[COMPARATIVE_EVALUATOR_T],
    experiment_prefix: Optional[str] = None,
    description: Optional[str] = None,
    max_concurrency: int = 5,
    client: Optional[langsmith.Client] = None,
    metadata: Optional[dict] = None,
    load_nested: bool = False,
    randomize_order: bool = False,
) -> ComparativeExperimentResults:
    r"""Evaluate existing experiment runs against each other.

    This lets you use pairwise preference scoring to generate more
    reliable feedback in your experiments.

    Args:
        experiments (Tuple[Union[str, uuid.UUID], Union[str, uuid.UUID]]):
            The identifiers of the experiments to compare.
        evaluators (Sequence[COMPARATIVE_EVALUATOR_T]):
            A list of evaluators to run on each example.
        experiment_prefix (Optional[str]): A prefix to provide for your experiment name.
            Defaults to None.
        description (Optional[str]): A free-form text description for the experiment.
        max_concurrency (int): The maximum number of concurrent evaluations to run.
            Defaults to 5.
        client (Optional[langsmith.Client]): The LangSmith client to use.
            Defaults to None.
        metadata (Optional[dict]): Metadata to attach to the experiment.
            Defaults to None.
        load_nested (bool): Whether to load all child runs for the experiment.
            Default is to only load the top-level root runs.
        randomize_order (bool): Whether to randomize the order of the outputs for each evaluation.
            Default is False.

    Returns:
        ComparativeExperimentResults: The results of the comparative evaluation.

    Examples:
        Suppose you want to compare two prompts to see which one is more effective.
        You would first prepare your dataset:

        >>> from typing import Sequence
        >>> from langsmith import Client
        >>> from langsmith.evaluation import evaluate
        >>> from langsmith.schemas import Example, Run
        >>> client = Client()
        >>> client.clone_public_dataset(
        ...     "https://smith.langchain.com/public/419dcab2-1d66-4b94-8901-0357ead390df/d"
        ... )
        >>> dataset_name = "Evaluate Examples"

        Then you would run your different prompts:
        >>> import functools
        >>> import openai
        >>> from langsmith.evaluation import evaluate
        >>> from langsmith.wrappers import wrap_openai
        >>> oai_client = openai.Client()
        >>> wrapped_client = wrap_openai(oai_client)
        >>> prompt_1 = "You are a helpful assistant."
        >>> prompt_2 = "You are an exceedingly helpful assistant."
        >>> def predict(inputs: dict, prompt: str) -> dict:
        ...     completion = wrapped_client.chat.completions.create(
        ...         model="gpt-3.5-turbo",
        ...         messages=[
        ...             {"role": "system", "content": prompt},
        ...             {
        ...                 "role": "user",
        ...                 "content": f"Context: {inputs['context']}"
        ...                 f"\n\ninputs['question']",
        ...             },
        ...         ],
        ...     )
        ...     return {"output": completion.choices[0].message.content}
        >>> results_1 = evaluate(
        ...     functools.partial(predict, prompt=prompt_1),
        ...     data=dataset_name,
        ...     description="Evaluating our basic system prompt.",
        ...     blocking=False,  # Run these experiments in parallel
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...
        >>> results_2 = evaluate(
        ...     functools.partial(predict, prompt=prompt_2),
        ...     data=dataset_name,
        ...     description="Evaluating our advanced system prompt.",
        ...     blocking=False,
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...
        >>> results_1.wait()
        >>> results_2.wait()
        >>> import time
        >>> time.sleep(10)  # Wait for the traces to be fully processed

            Finally, you would compare the two prompts directly:
        >>> import json
        >>> from langsmith.evaluation import evaluate_comparative
        >>> def score_preferences(runs: list, example):
        ...     pred_a = runs[0].outputs["output"]
        ...     pred_b = runs[1].outputs["output"]
        ...     ground_truth = example.outputs["answer"]
        ...     tools = [
        ...         {
        ...             "type": "function",
        ...             "function": {
        ...                 "name": "rank_preferences",
        ...                 "description": "Saves the prefered response ('A' or 'B')",
        ...                 "parameters": {
        ...                     "type": "object",
        ...                     "properties": {
        ...                         "reasoning": {
        ...                             "type": "string",
        ...                             "description": "The reasoning behind the choice.",
        ...                         },
        ...                         "preferred_option": {
        ...                             "type": "string",
        ...                             "enum": ["A", "B"],
        ...                             "description": "The preferred option, either 'A' or 'B'",
        ...                         },
        ...                     },
        ...                     "required": ["preferred_option"],
        ...                 },
        ...             },
        ...         }
        ...     ]
        ...     completion = openai.Client().chat.completions.create(
        ...         model="gpt-3.5-turbo",
        ...         messages=[
        ...             {"role": "system", "content": "Select the better response."},
        ...             {
        ...                 "role": "user",
        ...                 "content": f"Option A: {pred_a}"
        ...                 f"\n\nOption B: {pred_b}"
        ...                 f"\n\nGround Truth: {ground_truth}",
        ...             },
        ...         ],
        ...         tools=tools,
        ...         tool_choice={
        ...             "type": "function",
        ...             "function": {"name": "rank_preferences"},
        ...         },
        ...     )
        ...     tool_args = completion.choices[0].message.tool_calls[0].function.arguments
        ...     preference = json.loads(tool_args)["preferred_option"]
        ...     if preference == "A":
        ...         return {
        ...             "key": "ranked_preference",
        ...             "scores": {runs[0].id: 1, runs[1].id: 0},
        ...         }
        ...     else:
        ...         return {
        ...             "key": "ranked_preference",
        ...             "scores": {runs[0].id: 0, runs[1].id: 1},
        ...         }
        >>> results = evaluate_comparative(
        ...     [results_1.experiment_name, results_2.experiment_name],
        ...     evaluators=[score_preferences],
        ...     client=client,
        ... )  # doctest: +ELLIPSIS
        View the pairwise evaluation results at:...
    """  # noqa: E501
    if len(experiments) < 2:
        raise ValueError("Comparative evaluation requires at least 2 experiments.")
    if not evaluators:
        raise ValueError(
            "At least one evaluator is required for comparative evaluation."
        )
    if max_concurrency < 0:
        raise ValueError("max_concurrency must be a positive integer.")
    client = client or langsmith.Client()

    # TODO: Add information about comparison experiments
    projects = [_load_experiment(experiment, client) for experiment in experiments]
    ref_datasets_ = [str(p.reference_dataset_id) for p in projects]
    if not len(set(ref_datasets_)) == 1:
        raise ValueError("All experiments must have the same reference dataset.")
    experiment_ids = [p.id for p in projects]
    if experiment_prefix is None:
        experiment_names = [p.name for p in projects if p.name is not None]
        experiment_name = (
            " vs. ".join(experiment_names) + "-" + str(uuid.uuid4().hex[:4])
        )
    else:
        experiment_name = experiment_prefix + "-" + str(uuid.uuid4().hex[:8])
    comparative_experiment_id = uuid.uuid4()
    comparative_experiment = client.create_comparative_experiment(
        experiment_name,
        experiments=experiment_ids,
        description=description,
        metadata=metadata,
        id=comparative_experiment_id,
    )
    _print_comparative_experiment_start(
        cast(
            Tuple[schemas.TracerSessionResult, schemas.TracerSessionResult],
            tuple(projects),
        ),
        comparative_experiment,
    )
    runs = [
        _load_traces(experiment, client, load_nested=load_nested)
        for experiment in experiments
    ]
    # Only check intersections for the experiments
    examples_intersection = None
    for runs_list in runs:
        example_ids_set = {run.reference_example_id for run in runs_list}
        if examples_intersection is None:
            examples_intersection = example_ids_set
        else:
            examples_intersection &= example_ids_set
    example_ids_nullable = (
        list(examples_intersection) if examples_intersection is not None else []
    )
    example_ids = [eid for eid in example_ids_nullable if eid is not None]
    # TODO: Warn if different dataset versions, etc. are used in the different
    # experiments. We aren't providing any training wheels here.
    batch_size = 99
    data = {}
    for i in range(0, len(example_ids), batch_size):
        example_ids_batch = example_ids[i : i + batch_size]
        for e in client.list_examples(
            dataset_id=projects[0].reference_dataset_id,
            as_of=projects[0].metadata.get("dataset_version"),
            example_ids=example_ids_batch,
        ):
            data[e.id] = e
    runs_dict: Dict[uuid.UUID, List[schemas.Run]] = collections.defaultdict(list)
    for runs_list in runs:
        for run in runs_list:
            if run.reference_example_id in data:
                runs_dict[cast(uuid.UUID, run.reference_example_id)].append(run)

    comparators = [comparison_evaluator(evaluator) for evaluator in evaluators or []]
    results: dict = {}

    def evaluate_and_submit_feedback(
        runs_list: list[schemas.Run], example: schemas.Example, executor: cf.Executor
    ) -> ComparisonEvaluationResult:
        feedback_group_id = uuid.uuid4()
        if randomize_order:
            random.shuffle(runs_list)
        result = comparator.compare_runs(runs_list, example)
        if client is None:
            raise ValueError("Client is required to submit feedback.")
        for run_id, score in result.scores.items():
            executor.submit(
                client.create_feedback,
                run_id=run_id,
                key=result.key,
                score=score,
                comparative_experiment_id=comparative_experiment.id,
                source_run_id=result.source_run_id,
                feedback_group_id=feedback_group_id,
            )
        return result

    tqdm = _load_tqdm()
    with cf.ThreadPoolExecutor(max_workers=max_concurrency or 1) as executor:
        futures = []
        for example_id, runs_list in tqdm(runs_dict.items()):
            results[example_id] = {
                "runs": runs_list,
            }
            for comparator in comparators:
                if max_concurrency > 1:
                    future = executor.submit(
                        evaluate_and_submit_feedback,
                        runs_list,
                        data[example_id],
                        executor,
                    )
                    futures.append(future)
                else:
                    result = evaluate_and_submit_feedback(
                        runs_list, data[example_id], executor
                    )
                    results[example_id][f"feedback.{result.key}"] = result
            if futures:
                cf.wait(futures)
                for future in futures:
                    result = future.result()
                    results[example_id][f"feedback.{result.key}"] = result

    return ComparativeExperimentResults(results)


class ComparativeExperimentResults:
    """Represents the results of an evaluate_comparative() call.

    This class provides an iterator interface to iterate over the experiment results
    as they become available. It also provides methods to access the experiment name,
    the number of results, and to wait for the results to be processed.

    Methods:
        experiment_name() -> str: Returns the name of the experiment.
        wait() -> None: Waits for the experiment data to be processed.
    """

    def __init__(
        self,
        results: dict,
    ):
        self._results = results

    def __getitem__(self, key):
        """Return the result associated with the given key."""
        return self._results[key]


## Private API


def _print_comparative_experiment_start(
    experiments: Tuple[schemas.TracerSession, schemas.TracerSession],
    comparative_experiment: schemas.ComparativeExperiment,
) -> None:
    url = experiments[0].url or experiments[1].url
    if url:
        project_url = url.split("?")[0]
        dataset_id = comparative_experiment.reference_dataset_id
        base_url = project_url.split("/projects/p/")[0]
        comparison_url = (
            f"{base_url}/datasets/{dataset_id}/compare?"
            f"selectedSessions={'%2C'.join([str(e.id) for e in experiments])}"
            f"&comparativeExperiment={comparative_experiment.id}"
        )
        print(  # noqa: T201
            f"View the pairwise evaluation results at:\n{comparison_url}\n\n"
        )


def _is_callable(target: Union[TARGET_T, Iterable[schemas.Run]]) -> bool:
    return callable(target) or (hasattr(target, "invoke") and callable(target.invoke))


def _evaluate(
    target: Union[TARGET_T, Iterable[schemas.Run]],
    /,
    data: DATA_T,
    evaluators: Optional[Sequence[EVALUATOR_T]] = None,
    summary_evaluators: Optional[Sequence[SUMMARY_EVALUATOR_T]] = None,
    metadata: Optional[dict] = None,
    experiment_prefix: Optional[str] = None,
    description: Optional[str] = None,
    max_concurrency: Optional[int] = None,
    num_repetitions: int = 1,
    client: Optional[langsmith.Client] = None,
    blocking: bool = True,
    experiment: Optional[schemas.TracerSession] = None,
) -> ExperimentResults:
    # Initialize the experiment manager.
    client = client or langsmith.Client()
    runs = None if _is_callable(target) else cast(Iterable[schemas.Run], target)
    experiment_, runs = _resolve_experiment(
        experiment,
        runs,
        client,
    )

    manager = _ExperimentManager(
        data,
        client=client,
        metadata=metadata,
        experiment=experiment_ or experiment_prefix,
        description=description,
        num_repetitions=num_repetitions,
        # If provided, we don't need to create a new experiment.
        runs=runs,
        # Create or resolve the experiment.
    ).start()
    cache_dir = ls_utils.get_cache_dir(None)
    cache_path = (
        pathlib.Path(cache_dir) / f"{manager.dataset_id}.yaml" if cache_dir else None
    )
    with ls_utils.with_optional_cache(cache_path, ignore_hosts=[client.api_url]):
        if _is_callable(target):
            # Add predictions to the experiment.
            manager = manager.with_predictions(
                cast(TARGET_T, target), max_concurrency=max_concurrency
            )
        if evaluators:
            # Apply evaluators to the predictions.
            manager = manager.with_evaluators(
                evaluators, max_concurrency=max_concurrency
            )
        if summary_evaluators:
            # Apply the experiment-level summary evaluators.
            manager = manager.with_summary_evaluators(summary_evaluators)
        # Start consuming the results.
        results = ExperimentResults(manager)
        if blocking:
            # Wait for the evaluation to complete.
            results.wait()
        return results


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False


def _load_experiment(
    project: Union[str, uuid.UUID], client: langsmith.Client
) -> schemas.TracerSessionResult:
    if isinstance(project, uuid.UUID) or _is_uuid(project):
        return client.read_project(project_id=project)
    return client.read_project(project_name=project)


def _load_traces(
    project: Union[str, uuid.UUID], client: langsmith.Client, load_nested: bool = False
) -> List[schemas.Run]:
    """Load nested traces for a given project."""
    execution_order = None if load_nested else 1
    if isinstance(project, uuid.UUID) or _is_uuid(project):
        runs = client.list_runs(project_id=project, execution_order=execution_order)
    else:
        runs = client.list_runs(project_name=project, execution_order=execution_order)
    if not load_nested:
        return list(runs)

    treemap: DefaultDict[uuid.UUID, List[schemas.Run]] = collections.defaultdict(list)
    results = []
    all_runs = {}
    for run in runs:
        if run.parent_run_id is not None:
            treemap[run.parent_run_id].append(run)
        else:
            results.append(run)
        all_runs[run.id] = run
    for run_id, child_runs in treemap.items():
        all_runs[run_id].child_runs = sorted(child_runs, key=lambda r: r.dotted_order)
    return results


IT = TypeVar("IT")


def _load_tqdm() -> Callable[[IT], IT]:
    try:
        from tqdm.auto import tqdm
    except ImportError:
        return lambda x: x
    return tqdm  # type: ignore[return-value]


ET = TypeVar("ET", bound="_ExperimentManagerMixin")


class _ExperimentManagerMixin:
    def __init__(
        self,
        /,
        experiment: Optional[Union[schemas.TracerSession, str]],
        metadata: Optional[dict] = None,
        client: Optional[langsmith.Client] = None,
        description: Optional[str] = None,
    ):
        self.client = client or langsmith.Client()
        self._experiment: Optional[schemas.TracerSession] = None
        if experiment is None:
            self._experiment_name = _get_random_name()
        elif isinstance(experiment, str):
            self._experiment_name = experiment + "-" + str(uuid.uuid4().hex[:8])
        else:
            self._experiment_name = cast(str, experiment.name)
            self._experiment = experiment

        metadata = metadata or {}
        if not metadata.get("revision_id"):
            metadata = {
                "revision_id": ls_env.get_langchain_env_var_metadata().get(
                    "revision_id"
                ),
                **metadata,
            }
        self._metadata = metadata or {}
        self._description = description

    @property
    def experiment_name(self) -> str:
        if self._experiment_name is not None:
            return self._experiment_name
        raise ValueError(
            "Experiment name not provided, and experiment not yet started."
        )

    def _get_experiment(self) -> schemas.TracerSession:
        if self._experiment is None:
            raise ValueError("Experiment not started yet.")
        return self._experiment

    def _get_experiment_metadata(self):
        project_metadata = self._metadata or {}
        git_info = ls_env.get_git_info()
        if git_info:
            project_metadata = {
                **project_metadata,
                "git": git_info,
            }
        if self._experiment:
            project_metadata = {
                **self._experiment.metadata,
                **project_metadata,
            }
        return project_metadata

    def _get_project(self, first_example: schemas.Example) -> schemas.TracerSession:
        if self._experiment is None:
            try:
                project_metadata = self._get_experiment_metadata()
                project = self.client.create_project(
                    self.experiment_name,
                    description=self._description,
                    reference_dataset_id=first_example.dataset_id,
                    metadata=project_metadata,
                )
            except (HTTPError, ValueError, ls_utils.LangSmithError) as e:
                if "already exists " not in str(e):
                    raise e
                raise ValueError(
                    # TODO: Better error
                    f"Experiment {self.experiment_name} already exists."
                    " Please use a different name."
                )
        else:
            project = self._experiment
        return project

    def _print_experiment_start(
        self, project: schemas.TracerSession, first_example: schemas.Example
    ) -> None:
        if project.url:
            # TODO: Make this a public API
            project_url = project.url.split("?")[0]
            dataset_id = first_example.dataset_id
            base_url = project_url.split("/projects/p/")[0]
            comparison_url = (
                f"{base_url}/datasets/{dataset_id}/compare?"
                f"selectedSessions={project.id}"
            )
            print(  # noqa: T201
                f"View the evaluation results for experiment: '{self.experiment_name}'"
                f" at:\n{comparison_url}\n\n"
            )
        else:
            # HACKHACK
            print("Starting evaluation of experiment: %s", self.experiment_name)


class _ExperimentManager(_ExperimentManagerMixin):
    """Manage the execution of experiments.

    Supports lazily running predictions and evaluations in parallel to facilitate
    result streaming and early debugging.

    Args:
        data (DATA_T): The data used for the experiment. Can be a dataset name or ID OR
            a generator of examples.
        num_repetitions (int): The number of times to run over the data.
        runs (Optional[Iterable[schemas.Run]]): The runs associated with the experiment
            predictions.
        experiment (Optional[schemas.TracerSession]): The tracer session
            associated with the experiment.
        experiment_prefix (Optional[str]): The prefix for the experiment name.
        metadata (Optional[dict]): Additional metadata for the experiment.
        client (Optional[langsmith.Client]): The Langsmith client used for
             the experiment.
        evaluation_results (Optional[Iterable[EvaluationResults]]): The evaluation
            sresults for the experiment.
        summary_results (Optional[Iterable[EvaluationResults]]): The aggregate results
            for the experiment.
    """

    def __init__(
        self,
        data: DATA_T,
        /,
        experiment: Optional[Union[schemas.TracerSession, str]],
        metadata: Optional[dict] = None,
        client: Optional[langsmith.Client] = None,
        runs: Optional[Iterable[schemas.Run]] = None,
        evaluation_results: Optional[Iterable[EvaluationResults]] = None,
        summary_results: Optional[Iterable[EvaluationResults]] = None,
        description: Optional[str] = None,
        num_repetitions: int = 1,
    ):
        super().__init__(
            experiment=experiment,
            metadata=metadata,
            client=client,
            description=description,
        )
        self._data = data
        self._examples: Optional[Iterable[schemas.Example]] = None
        self._runs = runs
        self._evaluation_results = evaluation_results
        self._summary_results = summary_results
        self._num_repetitions = num_repetitions

    @property
    def examples(self) -> Iterable[schemas.Example]:
        if self._examples is None:
            self._examples = _resolve_data(self._data, client=self.client)
            if self._num_repetitions > 1:
                self._examples = itertools.chain.from_iterable(
                    itertools.tee(self._examples, self._num_repetitions)
                )
        self._examples, examples_iter = itertools.tee(self._examples)
        return examples_iter

    @property
    def dataset_id(self) -> str:
        if self._experiment is None or not getattr(
            self._experiment, "reference_dataset_id", None
        ):
            example = next(iter(self.examples))
            return str(example.dataset_id)
        return str(
            cast(schemas.TracerSessionResult, self._experiment).reference_dataset_id
        )

    @property
    def evaluation_results(self) -> Iterable[EvaluationResults]:
        if self._evaluation_results is None:
            return [{"results": []} for _ in self.examples]
        return self._evaluation_results

    @property
    def runs(self) -> Iterable[schemas.Run]:
        if self._runs is None:
            raise ValueError(
                "Runs not provided in this experiment." " Please predict first."
            )
        self._runs, runs_iter = itertools.tee(self._runs)
        return runs_iter

    def start(self) -> _ExperimentManager:
        first_example = next(itertools.islice(self.examples, 1))
        project = self._get_project(first_example)
        self._print_experiment_start(project, first_example)
        self._metadata["num_repetitions"] = self._num_repetitions
        return self.__class__(
            self.examples,
            experiment=project,
            metadata=self._metadata,
            client=self.client,
            runs=self._runs,
            evaluation_results=self._evaluation_results,
        )

    def with_predictions(
        self,
        target: TARGET_T,
        /,
        max_concurrency: Optional[int] = None,
    ) -> _ExperimentManager:
        """Lazily apply the target function to the experiment."""
        context = copy_context()
        _experiment_results = context.run(
            self._predict, target, max_concurrency=max_concurrency
        )
        r1, r2 = itertools.tee(_experiment_results, 2)
        return _ExperimentManager(
            (pred["example"] for pred in r1),
            experiment=self._experiment,
            metadata=self._metadata,
            client=self.client,
            runs=(pred["run"] for pred in r2),
            # TODO: Can't do multiple prediction rounds rn.
        )

    def with_evaluators(
        self,
        evaluators: Sequence[
            Union[
                EVALUATOR_T,
                RunEvaluator,
            ]
        ],
        *,
        max_concurrency: Optional[int] = None,
    ) -> _ExperimentManager:
        """Lazily apply the provided evaluators to the experiment."""
        evaluators = _resolve_evaluators(evaluators)
        context = copy_context()
        experiment_results = context.run(
            self._score, evaluators, max_concurrency=max_concurrency
        )
        # Split the generator into three so the manager
        # can consume each value individually.
        r1, r2, r3 = itertools.tee(experiment_results, 3)
        return _ExperimentManager(
            (result["example"] for result in r1),
            experiment=self._experiment,
            metadata=self._metadata,
            client=self.client,
            runs=(result["run"] for result in r2),
            evaluation_results=(result["evaluation_results"] for result in r3),
            summary_results=self._summary_results,
        )

    def with_summary_evaluators(
        self,
        summary_evaluators: Sequence[SUMMARY_EVALUATOR_T],
    ) -> _ExperimentManager:
        """Lazily apply the provided summary evaluators to the experiment."""
        wrapped_evaluators = _wrap_summary_evaluators(summary_evaluators)
        context = copy_context()
        aggregate_feedback_gen = context.run(
            self._apply_summary_evaluators, wrapped_evaluators
        )
        return _ExperimentManager(
            self.examples,
            experiment=self._experiment,
            metadata=self._metadata,
            client=self.client,
            runs=self.runs,
            evaluation_results=self._evaluation_results,
            summary_results=aggregate_feedback_gen,
        )

    def get_results(self) -> Iterable[ExperimentResultRow]:
        """Return the traces, evaluation results, and associated examples."""
        for run, example, evaluation_results in zip(
            self.runs, self.examples, self.evaluation_results
        ):
            yield ExperimentResultRow(
                run=run,
                example=example,
                evaluation_results=evaluation_results,
            )

    def get_summary_scores(self) -> Dict[str, List[dict]]:
        """If summary_evaluators were applied, consume and return the results."""
        if self._summary_results is None:
            return {"results": []}
        # Consume the generator
        return {
            "results": [
                res  # type: ignore[misc]
                for results in self._summary_results
                for res in results["results"]
            ]
        }

    # Private methods

    def _predict(
        self, target: TARGET_T, /, max_concurrency: Optional[int] = None
    ) -> Generator[_ForwardResults, None, None]:
        """Run the target function on the examples."""
        fn = _ensure_traceable(target)
        if max_concurrency == 0:
            for example in self.examples:
                yield _forward(
                    fn, example, self.experiment_name, self._metadata, self.client
                )

        else:
            with cf.ThreadPoolExecutor(max_concurrency) as executor:
                futures = [
                    executor.submit(
                        _forward,
                        fn,
                        example,
                        self.experiment_name,
                        self._metadata,
                        self.client,
                    )
                    for example in self.examples
                ]
                for future in cf.as_completed(futures):
                    yield future.result()
        # Close out the project.
        self._end()

    def _run_evaluators(
        self,
        evaluators: Sequence[RunEvaluator],
        current_results: ExperimentResultRow,
    ) -> ExperimentResultRow:
        current_context = rh.get_tracing_context()
        metadata = {
            **(current_context["metadata"] or {}),
            **{
                "experiment": self.experiment_name,
                "reference_example_id": current_results["example"].id,
                "reference_run_id": current_results["run"].id,
            },
        }
        with rh.tracing_context(
            **{**current_context, "project_name": "evaluators", "metadata": metadata}
        ):
            run = current_results["run"]
            example = current_results["example"]
            eval_results = current_results["evaluation_results"]
            for evaluator in evaluators:
                try:
                    evaluator_response = evaluator.evaluate_run(
                        run=run,
                        example=example,
                    )
                    eval_results["results"].extend(
                        # TODO: This is a hack
                        self.client._log_evaluation_feedback(
                            evaluator_response,
                            run=run,
                        )
                    )
                except Exception as e:
                    logger.error(
                        f"Error running evaluator {repr(evaluator)} on"
                        f" run {run.id}: {repr(e)}",
                        exc_info=True,
                    )
            return ExperimentResultRow(
                run=run,
                example=example,
                evaluation_results=eval_results,
            )

    def _score(
        self,
        evaluators: Sequence[RunEvaluator],
        max_concurrency: Optional[int] = None,
    ) -> Iterable[ExperimentResultRow]:
        """Run the evaluators on the prediction stream.

        Expects runs to be available in the manager.
        (e.g. from a previous prediction step)
        """
        if max_concurrency == 0:
            for current_results in self.get_results():
                yield self._run_evaluators(evaluators, current_results)
        else:
            with cf.ThreadPoolExecutor(max_workers=max_concurrency) as executor:
                futures = []
                for current_results in self.get_results():
                    futures.append(
                        executor.submit(
                            self._run_evaluators,
                            evaluators,
                            current_results,
                        )
                    )
                for future in cf.as_completed(futures):
                    result = future.result()
                    yield result

    def _apply_summary_evaluators(
        self, summary_evaluators: Sequence[SUMMARY_EVALUATOR_T]
    ) -> Generator[EvaluationResults, None, None]:
        runs, examples = [], []
        for run, example in zip(self.runs, self.examples):
            runs.append(run)
            examples.append(example)
        aggregate_feedback = []
        with cf.ThreadPoolExecutor() as executor:
            project_id = self._get_experiment().id
            current_context = rh.get_tracing_context()
            metadata = {
                **(current_context["metadata"] or {}),
                **{
                    "experiment": self.experiment_name,
                    "experiment_id": project_id,
                },
            }
            with rh.tracing_context(
                **{
                    **current_context,
                    "project_name": "evaluators",
                    "metadata": metadata,
                }
            ):
                for evaluator in summary_evaluators:
                    try:
                        summary_eval_result = evaluator(runs, examples)
                        # TODO: Expose public API for this.
                        flattened_results = self.client._select_eval_results(
                            summary_eval_result,
                            fn_name=evaluator.__name__,
                        )
                        aggregate_feedback.extend(flattened_results)
                        for result in flattened_results:
                            feedback = result.dict(exclude={"target_run_id"})
                            evaluator_info = feedback.pop("evaluator_info", None)
                            executor.submit(
                                self.client.create_feedback,
                                **feedback,
                                run_id=None,
                                project_id=project_id,
                                source_info=evaluator_info,
                            )
                    except Exception as e:
                        logger.error(
                            f"Error running summary evaluator {repr(evaluator)}: {e}"
                        )
        yield {"results": aggregate_feedback}

    def _get_dataset_version(self) -> Optional[str]:
        examples = list(self.examples)
        modified_at = [ex.modified_at for ex in examples if ex.modified_at]
        # Should always be defined in practice when fetched,
        # but the typing permits None
        max_modified_at = max(modified_at) if modified_at else None
        return max_modified_at.isoformat() if max_modified_at else None

    def _get_dataset_splits(self) -> Optional[list[str]]:
        examples = list(self.examples)
        splits = set()
        for example in examples:
            if (
                example.metadata
                and example.metadata.get("dataset_split")
                and isinstance(example.metadata["dataset_split"], list)
            ):
                for split in example.metadata["dataset_split"]:
                    if isinstance(split, str):
                        splits.add(split)
            else:
                splits.add("base")

        return list(splits)

    def _end(self) -> None:
        experiment = self._experiment
        if experiment is None:
            raise ValueError("Experiment not started yet.")

        project_metadata = self._get_experiment_metadata()
        project_metadata["dataset_version"] = self._get_dataset_version()
        project_metadata["dataset_splits"] = self._get_dataset_splits()
        self.client.update_project(
            experiment.id,
            end_time=datetime.datetime.now(datetime.timezone.utc),
            metadata=project_metadata,
        )


def _resolve_evaluators(
    evaluators: Sequence[Union[EVALUATOR_T, RunEvaluator, AEVALUATOR_T]],
) -> Sequence[RunEvaluator]:
    results = []
    for evaluator in evaluators:
        if isinstance(evaluator, RunEvaluator):
            results.append(evaluator)
        elif isinstance(evaluator, LangChainStringEvaluator):
            results.append(evaluator.as_run_evaluator())
        else:
            results.append(run_evaluator(evaluator))
    return results


def _wrap_summary_evaluators(
    evaluators: Sequence[SUMMARY_EVALUATOR_T],
) -> List[SUMMARY_EVALUATOR_T]:
    def _wrap(evaluator: SUMMARY_EVALUATOR_T) -> SUMMARY_EVALUATOR_T:
        eval_name = getattr(evaluator, "__name__", "BatchEvaluator")

        @functools.wraps(evaluator)
        def _wrapper_inner(
            runs: Sequence[schemas.Run], examples: Sequence[schemas.Example]
        ) -> Union[EvaluationResult, EvaluationResults]:
            @rh.traceable(name=eval_name)
            def _wrapper_super_inner(
                runs_: str, examples_: str
            ) -> Union[EvaluationResult, EvaluationResults]:
                return evaluator(runs, examples)

            return _wrapper_super_inner(
                f"Runs[] (Length={len(runs)})", f"Examples[] (Length={len(examples)})"
            )

        return _wrapper_inner

    results = []
    for evaluator in evaluators:
        results.append(_wrap(evaluator))
    return results


class _ForwardResults(TypedDict):
    run: schemas.Run
    example: schemas.Example


def _forward(
    fn: rh.SupportsLangsmithExtra,
    example: schemas.Example,
    experiment_name: str,
    metadata: dict,
    client: langsmith.Client,
) -> _ForwardResults:
    run: Optional[schemas.RunBase] = None

    def _get_run(r: run_trees.RunTree) -> None:
        nonlocal run
        run = r

    try:
        fn(
            example.inputs,
            langsmith_extra=rh.LangSmithExtra(
                reference_example_id=example.id,
                on_end=_get_run,
                project_name=experiment_name,
                metadata={
                    **metadata,
                    "example_version": (
                        example.modified_at.isoformat()
                        if example.modified_at
                        else example.created_at.isoformat()
                    ),
                },
                client=client,
            ),
        )
    except Exception as e:
        logger.error(f"Error running target function: {e}")
    return _ForwardResults(
        run=cast(schemas.Run, run),
        example=example,
    )


def _resolve_data(
    data: DATA_T, *, client: langsmith.Client
) -> Iterable[schemas.Example]:
    """Return the examples for the given dataset."""
    if isinstance(data, str):
        return client.list_examples(dataset_name=data)
    elif isinstance(data, uuid.UUID):
        return client.list_examples(dataset_id=data)
    return data


def _ensure_traceable(
    target: TARGET_T | rh.SupportsLangsmithExtra[[dict], dict],
) -> rh.SupportsLangsmithExtra[[dict], dict]:
    """Ensure the target function is traceable."""
    if not callable(target):
        raise ValueError("Target must be a callable function.")
    if rh.is_traceable_function(target):
        fn = target
    else:
        fn = rh.traceable(name="Target")(target)
    return fn


def _resolve_experiment(
    experiment: Optional[schemas.TracerSession],
    runs: Optional[Iterable[schemas.Run]],
    client: langsmith.Client,
) -> Tuple[
    Optional[Union[schemas.TracerSession, str]], Optional[Iterable[schemas.Run]]
]:
    # TODO: Remove this, handle outside the manager
    if experiment is not None:
        if not experiment.name:
            raise ValueError("Experiment name must be defined if provided.")
        return experiment, None
    # If we have runs, that means the experiment was already started.
    if runs is not None:
        if runs is not None:
            runs_, runs = itertools.tee(runs)
            first_run = next(runs_)
        experiment = client.read_project(project_id=first_run.session_id)
        if not experiment.name:
            raise ValueError("Experiment name not found for provided runs.")
        return experiment, runs
    return None, None


def _get_random_name() -> str:
    from langsmith.evaluation._name_generation import random_name  # noqa: F401

    return random_name()
