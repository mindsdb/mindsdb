"""V2 Evaluation Interface."""

from __future__ import annotations

import asyncio
import datetime
import logging
import pathlib
import uuid
from typing import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
)

import langsmith
from langsmith import run_helpers as rh
from langsmith import run_trees, schemas
from langsmith import utils as ls_utils
from langsmith._internal import _aiter as aitertools
from langsmith.beta import warn_beta
from langsmith.evaluation._runner import (
    AEVALUATOR_T,
    DATA_T,
    EVALUATOR_T,
    SUMMARY_EVALUATOR_T,
    ExperimentResultRow,
    _ExperimentManagerMixin,
    _ForwardResults,
    _load_experiment,
    _load_tqdm,
    _load_traces,
    _resolve_data,
    _resolve_evaluators,
    _resolve_experiment,
    _wrap_summary_evaluators,
)
from langsmith.evaluation.evaluator import EvaluationResults, RunEvaluator

logger = logging.getLogger(__name__)

ATARGET_T = Callable[[dict], Awaitable[dict]]


@warn_beta
async def aevaluate(
    target: Union[ATARGET_T, AsyncIterable[dict]],
    /,
    data: Union[DATA_T, AsyncIterable[schemas.Example], Iterable[schemas.Example]],
    evaluators: Optional[Sequence[Union[EVALUATOR_T, AEVALUATOR_T]]] = None,
    summary_evaluators: Optional[Sequence[SUMMARY_EVALUATOR_T]] = None,
    metadata: Optional[dict] = None,
    experiment_prefix: Optional[str] = None,
    description: Optional[str] = None,
    max_concurrency: Optional[int] = None,
    num_repetitions: int = 1,
    client: Optional[langsmith.Client] = None,
    blocking: bool = True,
) -> AsyncExperimentResults:
    r"""Evaluate an async target system or function on a given dataset.

    Args:
        target (Union[AsyncCallable[[dict], dict], AsyncIterable[dict]]): The async target system or function to evaluate.
        data (Union[DATA_T, AsyncIterable[schemas.Example]]): The dataset to evaluate on. Can be a dataset name, a list of
            examples, an async generator of examples, or an async iterable of examples.
        evaluators (Optional[Sequence[EVALUATOR_T]]): A list of evaluators to run
            on each example. Defaults to None.
        summary_evaluators (Optional[Sequence[SUMMARY_EVALUATOR_T]]): A list of summary
            evaluators to run on the entire dataset. Defaults to None.
        metadata (Optional[dict]): Metadata to attach to the experiment.
            Defaults to None.
        experiment_prefix (Optional[str]): A prefix to provide for your experiment name.
            Defaults to None.
        description (Optional[str]): A description of the experiment.
        max_concurrency (Optional[int]): The maximum number of concurrent
            evaluations to run. Defaults to None.
        num_repetitions (int): The number of times to run the evaluation.
            Each item in the dataset will be run and evaluated this many times.
            Defaults to 1.
        client (Optional[langsmith.Client]): The LangSmith client to use.
            Defaults to None.
        blocking (bool): Whether to block until the evaluation is complete.
            Defaults to True.

    Returns:
        AsyncIterator[ExperimentResultRow]: An async iterator over the experiment results.

    Environment:
        - LANGSMITH_TEST_CACHE: If set, API calls will be cached to disk to save time and
            cost during testing. Recommended to commit the cache files to your repository
            for faster CI/CD runs.
            Requires the 'langsmith[vcr]' package to be installed.

    Examples:
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

        >>> import asyncio
        >>> async def apredict(inputs: dict) -> dict:
        ...     # This can be any async function or just an API call to your app.
        ...     await asyncio.sleep(0.1)
        ...     return {"output": "Yes"}
        >>> results = asyncio.run(
        ...     aevaluate(
        ...         apredict,
        ...         data=dataset_name,
        ...         evaluators=[accuracy],
        ...         summary_evaluators=[precision],
        ...         experiment_prefix="My Experiment",
        ...         description="Evaluate the accuracy of the model asynchronously.",
        ...         metadata={
        ...             "my-prompt-version": "abcd-1234",
        ...         },
        ...     )
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...

        Evaluating over only a subset of the examples using an async generator:

        >>> async def example_generator():
        ...     examples = client.list_examples(dataset_name=dataset_name, limit=5)
        ...     for example in examples:
        ...         yield example
        >>> results = asyncio.run(
        ...     aevaluate(
        ...         apredict,
        ...         data=example_generator(),
        ...         evaluators=[accuracy],
        ...         summary_evaluators=[precision],
        ...         experiment_prefix="My Subset Experiment",
        ...         description="Evaluate a subset of examples asynchronously.",
        ...     )
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...

        Streaming each prediction to more easily + eagerly debug.

        >>> results = asyncio.run(
        ...     aevaluate(
        ...         apredict,
        ...         data=dataset_name,
        ...         evaluators=[accuracy],
        ...         summary_evaluators=[precision],
        ...         experiment_prefix="My Streaming Experiment",
        ...         description="Streaming predictions for debugging.",
        ...         blocking=False,
        ...     )
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...

        >>> async def aenumerate(iterable):
        ...     async for elem in iterable:
        ...         print(elem)
        >>> asyncio.run(aenumerate(results))

        Running without concurrency:

        >>> results = asyncio.run(
        ...     aevaluate(
        ...         apredict,
        ...         data=dataset_name,
        ...         evaluators=[accuracy],
        ...         summary_evaluators=[precision],
        ...         experiment_prefix="My Experiment Without Concurrency",
        ...         description="This was run without concurrency.",
        ...         max_concurrency=0,
        ...     )
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...

        Using Async evaluators:

        >>> async def helpfulness(run: Run, example: Example):
        ...     # Row-level evaluator for helpfulness.
        ...     await asyncio.sleep(0.1)  # Replace with your LLM API call
        ...     return {"score": run.outputs["output"] == "Yes"}

        >>> results = asyncio.run(
        ...     aevaluate(
        ...         apredict,
        ...         data=dataset_name,
        ...         evaluators=[helpfulness],
        ...         summary_evaluators=[precision],
        ...         experiment_prefix="My Helpful Experiment",
        ...         description="Applying async evaluators example.",
        ...     )
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...
    """  # noqa: E501
    return await _aevaluate(
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


@warn_beta
async def aevaluate_existing(
    experiment: Union[str, uuid.UUID],
    /,
    evaluators: Optional[Sequence[Union[EVALUATOR_T, AEVALUATOR_T]]] = None,
    summary_evaluators: Optional[Sequence[SUMMARY_EVALUATOR_T]] = None,
    metadata: Optional[dict] = None,
    max_concurrency: Optional[int] = None,
    client: Optional[langsmith.Client] = None,
    load_nested: bool = False,
    blocking: bool = True,
) -> AsyncIterator[ExperimentResultRow]:
    r"""Evaluate existing experiment runs asynchronously.

    Args:
        experiment (Union[str, uuid.UUID]): The identifier of the experiment to evaluate.
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
        AsyncIterator[ExperimentResultRow]: An async iterator over the experiment results.

    Examples:
        Define your evaluators

        >>> from typing import Sequence
        >>> from langsmith.schemas import Example, Run
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

        Load the experiment and run the evaluation.

        >>> from langsmith.evaluation import aevaluate, aevaluate_existing
        >>> dataset_name = "Evaluate Examples"
        >>> async def apredict(inputs: dict) -> dict:
        ...     # This can be any async function or just an API call to your app.
        ...     await asyncio.sleep(0.1)
        ...     return {"output": "Yes"}
        >>> # First run inference on the dataset
        ... results = asyncio.run(
        ...     aevaluate(
        ...         apredict,
        ...         data=dataset_name,
        ...     )
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...

        Then evaluate the results
        >>> experiment_name = "My Experiment:64e6e91"  # Or manually specify
        >>> results = asyncio.run(
        ...     aevaluate_existing(
        ...         experiment_name,
        ...         evaluators=[accuracy],
        ...         summary_evaluators=[precision],
        ...     )
        ... )  # doctest: +ELLIPSIS
        View the evaluation results for experiment:...


    """  # noqa: E501
    client = client or langsmith.Client()
    project = _load_experiment(experiment, client)
    runs = _load_traces(experiment, client, load_nested=load_nested)
    data = [
        example
        for example in client.list_examples(
            dataset_id=project.reference_dataset_id,
            as_of=project.metadata.get("dataset_version"),
        )
    ]
    runs = sorted(runs, key=lambda r: str(r.reference_example_id))
    data = sorted(data, key=lambda d: str(d.id))
    return await _aevaluate(
        runs,
        data=data,
        evaluators=evaluators,
        summary_evaluators=summary_evaluators,
        metadata=metadata,
        max_concurrency=max_concurrency,
        client=client,
        blocking=blocking,
    )


async def _aevaluate(
    target: Union[ATARGET_T, AsyncIterable[dict], Iterable[schemas.Run]],
    /,
    data: Union[DATA_T, AsyncIterable[schemas.Example]],
    evaluators: Optional[Sequence[Union[EVALUATOR_T, AEVALUATOR_T]]] = None,
    summary_evaluators: Optional[Sequence[SUMMARY_EVALUATOR_T]] = None,
    metadata: Optional[dict] = None,
    experiment_prefix: Optional[str] = None,
    description: Optional[str] = None,
    max_concurrency: Optional[int] = None,
    num_repetitions: int = 1,
    client: Optional[langsmith.Client] = None,
    blocking: bool = True,
    experiment: Optional[schemas.TracerSession] = None,
) -> AsyncExperimentResults:
    is_async_target = asyncio.iscoroutinefunction(target) or (
        hasattr(target, "__aiter__") and asyncio.iscoroutine(target.__aiter__())
    )
    client = client or langsmith.Client()
    runs = None if is_async_target else cast(Iterable[schemas.Run], target)
    experiment_, runs = _resolve_experiment(
        experiment,
        runs,
        client,
    )
    manager = await _AsyncExperimentManager(
        data,
        client=client,
        metadata=metadata,
        experiment=experiment_ or experiment_prefix,
        description=description,
        num_repetitions=num_repetitions,
        runs=runs,
    ).astart()
    cache_dir = ls_utils.get_cache_dir(None)
    if cache_dir is not None:
        dsid = await manager.get_dataset_id()
        cache_path = pathlib.Path(cache_dir) / f"{dsid}.yaml"
    else:
        cache_path = None
    with ls_utils.with_optional_cache(cache_path, ignore_hosts=[client.api_url]):
        if is_async_target:
            manager = await manager.awith_predictions(
                cast(ATARGET_T, target), max_concurrency=max_concurrency
            )
        if evaluators:
            manager = await manager.awith_evaluators(
                evaluators, max_concurrency=max_concurrency
            )
        if summary_evaluators:
            manager = await manager.awith_summary_evaluators(summary_evaluators)
        results = AsyncExperimentResults(manager)
        if blocking:
            await results.wait()
        return results


class _AsyncExperimentManager(_ExperimentManagerMixin):
    """Manage the execution of experiments asynchronously.

    Supports lazily running predictions and evaluations in parallel to facilitate
    result streaming and early debugging.

    Args:
        data (DATA_T): The data used for the experiment. Can be a dataset name or ID OR
            a generator of examples.
        runs (Optional[Iterable[schemas.Run]]): The runs associated with the experiment
            predictions.
        experiment (Optional[schemas.TracerSession]): The tracer session
            associated with the experiment.
        experiment_prefix (Optional[str]): The prefix for the experiment name.
        description (Optional[str]): The description for the experiment.
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
        data: Union[DATA_T, AsyncIterable[schemas.Example]],
        /,
        experiment: Optional[Union[schemas.TracerSession, str]] = None,
        metadata: Optional[dict] = None,
        runs: Optional[Union[Iterable[schemas.Run], AsyncIterable[schemas.Run]]] = None,
        client: Optional[langsmith.Client] = None,
        evaluation_results: Optional[AsyncIterable[EvaluationResults]] = None,
        summary_results: Optional[AsyncIterable[EvaluationResults]] = None,
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
        self._examples: Optional[AsyncIterable[schemas.Example]] = None
        self._runs = (
            aitertools.ensure_async_iterator(runs) if runs is not None else None
        )
        self._evaluation_results = evaluation_results
        self._summary_results = summary_results
        self._num_repetitions = num_repetitions

    async def aget_examples(self) -> AsyncIterator[schemas.Example]:
        if self._examples is None:
            self._examples = _aresolve_data(self._data, client=self.client)
            if self._num_repetitions > 1:
                self._examples = async_chain_from_iterable(
                    aitertools.atee(self._examples, self._num_repetitions)
                )

        self._examples, examples_iter = aitertools.atee(
            aitertools.ensure_async_iterator(self._examples), 2, lock=asyncio.Lock()
        )
        return examples_iter

    async def get_dataset_id(self) -> str:
        if self._experiment is None or not getattr(
            self._experiment, "reference_dataset_id", None
        ):
            example = await aitertools.py_anext(await self.aget_examples())
            if example is None:
                raise ValueError("No examples found in the dataset.")
            return str(example.dataset_id)
        return str(self._experiment.reference_dataset_id)

    async def aget_runs(self) -> AsyncIterator[schemas.Run]:
        if self._runs is None:
            raise ValueError("Runs not loaded yet.")
        self._runs, runs = aitertools.atee(
            aitertools.ensure_async_iterator(self._runs), 2, lock=asyncio.Lock()
        )
        async for run in runs:
            yield run

    async def aget_evaluation_results(self) -> AsyncIterator[EvaluationResults]:
        if self._evaluation_results is None:
            async for _ in await self.aget_examples():
                yield {"results": []}
        else:
            self._evaluation_results, evaluation_results = aitertools.atee(
                aitertools.ensure_async_iterator(self._evaluation_results),
                2,
                lock=asyncio.Lock(),
            )
            async for result in evaluation_results:
                yield result

    async def astart(self) -> _AsyncExperimentManager:
        first_example = await aitertools.py_anext(await self.aget_examples())
        if not first_example:
            raise ValueError("No examples found in the dataset.")
        project = self._get_project(first_example)
        self._print_experiment_start(project, first_example)
        self._metadata["num_repetitions"] = self._num_repetitions
        return self.__class__(
            await self.aget_examples(),
            experiment=project,
            metadata=self._metadata,
            client=self.client,
            runs=self._runs,
            evaluation_results=self._evaluation_results,
        )

    async def awith_predictions(
        self,
        target: ATARGET_T,
        /,
        max_concurrency: Optional[int] = None,
    ) -> _AsyncExperimentManager:
        _experiment_results = self._apredict(target, max_concurrency=max_concurrency)
        r1, r2 = aitertools.atee(_experiment_results, 2, lock=asyncio.Lock())
        return _AsyncExperimentManager(
            (pred["example"] async for pred in r1),
            experiment=self._experiment,
            metadata=self._metadata,
            client=self.client,
            runs=(pred["run"] async for pred in r2),
        )

    async def awith_evaluators(
        self,
        evaluators: Sequence[Union[EVALUATOR_T, AEVALUATOR_T]],
        *,
        max_concurrency: Optional[int] = None,
    ) -> _AsyncExperimentManager:
        evaluators = _resolve_evaluators(evaluators)
        experiment_results = self._ascore(evaluators, max_concurrency=max_concurrency)
        r1, r2, r3 = aitertools.atee(experiment_results, 3, lock=asyncio.Lock())
        return _AsyncExperimentManager(
            (result["example"] async for result in r1),
            experiment=self._experiment,
            metadata=self._metadata,
            client=self.client,
            runs=(result["run"] async for result in r2),
            evaluation_results=(result["evaluation_results"] async for result in r3),
            summary_results=self._summary_results,
        )

    async def awith_summary_evaluators(
        self,
        summary_evaluators: Sequence[SUMMARY_EVALUATOR_T],
    ) -> _AsyncExperimentManager:
        wrapped_evaluators = _wrap_summary_evaluators(summary_evaluators)
        aggregate_feedback_gen = self._aapply_summary_evaluators(wrapped_evaluators)
        return _AsyncExperimentManager(
            await self.aget_examples(),
            experiment=self._experiment,
            metadata=self._metadata,
            client=self.client,
            runs=self.aget_runs(),
            evaluation_results=self._evaluation_results,
            summary_results=aggregate_feedback_gen,
        )

    async def aget_results(self) -> AsyncIterator[ExperimentResultRow]:
        async for run, example, evaluation_results in aitertools.async_zip(
            self.aget_runs(), await self.aget_examples(), self.aget_evaluation_results()
        ):
            yield ExperimentResultRow(
                run=run,
                example=example,
                evaluation_results=evaluation_results,
            )

    async def aget_summary_scores(self) -> Dict[str, List[dict]]:
        if self._summary_results is None:
            return {"results": []}
        return {
            "results": [
                res  # type: ignore[misc]
                async for results in self._summary_results
                for res in results["results"]
            ]
        }

    ## Private methods

    async def _apredict(
        self, target: ATARGET_T, /, max_concurrency: Optional[int] = None
    ) -> AsyncIterator[_ForwardResults]:
        fn = _ensure_async_traceable(target)

        async def predict_all():
            async for example in await self.aget_examples():
                # Yield the coroutine to be awaited later
                yield _aforward(
                    fn, example, self.experiment_name, self._metadata, self.client
                )

        async for result in aitertools.aiter_with_concurrency(
            max_concurrency, predict_all()
        ):
            yield result

        await self._aend()

    async def _ascore(
        self,
        evaluators: Sequence[RunEvaluator],
        max_concurrency: Optional[int] = None,
    ) -> AsyncIterator[ExperimentResultRow]:
        async def score_all():
            async for current_results in self.aget_results():
                # Yield the coroutine to be awaited later in aiter_with_concurrency
                yield self._arun_evaluators(evaluators, current_results)

        async for result in aitertools.aiter_with_concurrency(
            max_concurrency, score_all()
        ):
            yield result

    async def _arun_evaluators(
        self,
        evaluators: Sequence[RunEvaluator],
        current_results: ExperimentResultRow,
    ) -> ExperimentResultRow:
        current_context = rh.get_tracing_context()
        metadata = {
            **(current_context["metadata"] or {}),
            **{"experiment": self.experiment_name},
        }
        with rh.tracing_context(
            **{**current_context, "project_name": "evaluators", "metadata": metadata}
        ):
            run = current_results["run"]
            example = current_results["example"]
            eval_results = current_results["evaluation_results"]
            for evaluator in evaluators:
                try:
                    evaluator_response = await evaluator.aevaluate_run(
                        run=run,
                        example=example,
                    )
                    eval_results["results"].extend(
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

    async def _aapply_summary_evaluators(
        self, summary_evaluators: Sequence[SUMMARY_EVALUATOR_T]
    ) -> AsyncIterator[EvaluationResults]:
        runs, examples = [], []
        async_examples = aitertools.ensure_async_iterator(await self.aget_examples())
        async for run, example in aitertools.async_zip(
            self.aget_runs(), async_examples
        ):
            runs.append(run)
            examples.append(example)
        aggregate_feedback = []
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
                    # TODO: Support async evaluators
                    summary_eval_result = evaluator(runs, examples)
                    flattened_results = self.client._select_eval_results(
                        summary_eval_result,
                        fn_name=evaluator.__name__,
                    )
                    aggregate_feedback.extend(flattened_results)
                    for result in flattened_results:
                        feedback = result.dict(exclude={"target_run_id"})
                        evaluator_info = feedback.pop("evaluator_info", None)
                        self.client.create_feedback(
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

    async def _get_dataset_version(self) -> Optional[str]:
        modified_at = []
        async for example in await self.aget_examples():
            if example.modified_at:
                # Should always be defined in practice when fetched,
                # but the typing permits None
                modified_at.append(example.modified_at)

        max_modified_at = max(modified_at) if modified_at else None
        return max_modified_at.isoformat() if max_modified_at else None

    async def _get_dataset_splits(self) -> Optional[list[str]]:
        splits = set()
        async for example in await self.aget_examples():
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

    async def _aend(self) -> None:
        experiment = self._experiment
        if experiment is None:
            raise ValueError("Experiment not started yet.")

        project_metadata = self._get_experiment_metadata()
        project_metadata["dataset_version"] = await self._get_dataset_version()
        project_metadata["dataset_splits"] = await self._get_dataset_splits()
        self.client.update_project(
            experiment.id,
            end_time=datetime.datetime.now(datetime.timezone.utc),
            metadata=project_metadata,
        )


class AsyncExperimentResults:
    def __init__(
        self,
        experiment_manager: _AsyncExperimentManager,
    ):
        self._manager = experiment_manager
        self._results: List[ExperimentResultRow] = []
        self._lock = asyncio.Lock()
        self._task = asyncio.create_task(self._process_data(self._manager))
        self._processed_count = 0

    @property
    def experiment_name(self) -> str:
        return self._manager.experiment_name

    def __aiter__(self) -> AsyncIterator[ExperimentResultRow]:
        return self

    async def __anext__(self) -> ExperimentResultRow:
        while True:
            async with self._lock:
                if self._processed_count < len(self._results):
                    result = self._results[self._processed_count]
                    self._processed_count += 1
                    return result
                elif self._task.done():
                    raise StopAsyncIteration
            await asyncio.shield(
                asyncio.wait([self._task], return_when=asyncio.FIRST_COMPLETED)
            )

    async def _process_data(self, manager: _AsyncExperimentManager) -> None:
        tqdm = _load_tqdm()
        async for item in tqdm(manager.aget_results()):
            async with self._lock:
                self._results.append(item)
        summary_scores = await manager.aget_summary_scores()
        async with self._lock:
            self._summary_results = summary_scores

    def __len__(self) -> int:
        return len(self._results)

    def __repr__(self) -> str:
        return f"<AsyncExperimentResults {self.experiment_name}>"

    async def wait(self) -> None:
        await self._task


async def _aforward(
    fn: rh.SupportsLangsmithExtra[[dict], Awaitable],
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
        await fn(
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


def _ensure_async_traceable(
    target: ATARGET_T,
) -> rh.SupportsLangsmithExtra[[dict], Awaitable]:
    if not asyncio.iscoroutinefunction(target):
        raise ValueError(
            "Target must be an async function. For sync functions, use evaluate."
        )
    if rh.is_traceable_function(target):
        return target  # type: ignore
    return rh.traceable(name="AsyncTarget")(target)


def _aresolve_data(
    data: Union[DATA_T, AsyncIterable[schemas.Example]], *, client: langsmith.Client
) -> AsyncIterator[schemas.Example]:
    """Return the examples for the given dataset."""
    if isinstance(data, AsyncIterable):
        return aitertools.ensure_async_iterator(data)
    return aitertools.ensure_async_iterator(_resolve_data(data, client=client))


T = TypeVar("T")


async def async_chain_from_iterable(
    iterable: Iterable[AsyncIterable[T]],
) -> AsyncIterator[T]:
    """Chain multiple async iterables."""
    for sub_iterable in iterable:
        async for item in sub_iterable:
            yield item
