"""Schemas for the LangSmith API."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, cast
from uuid import UUID, uuid4

try:
    from pydantic.v1 import Field, root_validator, validator  # type: ignore[import]
except ImportError:
    from pydantic import (  # type: ignore[assignment, no-redef]
        Field,
        root_validator,
        validator,
    )

import threading
import urllib.parse

from langsmith import schemas as ls_schemas
from langsmith import utils
from langsmith.client import ID_TYPE, RUN_TYPE_T, Client, _dumps_json, _ensure_uuid

logger = logging.getLogger(__name__)

LANGSMITH_PREFIX = "langsmith-"
LANGSMITH_DOTTED_ORDER = f"{LANGSMITH_PREFIX}trace"
_CLIENT: Optional[Client] = None
_LOCK = threading.Lock()


def _get_client() -> Client:
    global _CLIENT
    if _CLIENT is None:
        with _LOCK:
            if _CLIENT is None:
                _CLIENT = Client()
    return _CLIENT


class RunTree(ls_schemas.RunBase):
    """Run Schema with back-references for posting runs."""

    name: str
    id: UUID = Field(default_factory=uuid4)
    run_type: str = Field(default="chain")
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    parent_run: Optional[RunTree] = Field(default=None, exclude=True)
    child_runs: List[RunTree] = Field(
        default_factory=list,
        exclude={"__all__": {"parent_run_id"}},
    )
    session_name: str = Field(
        default_factory=lambda: utils.get_tracer_project() or "default",
        alias="project_name",
    )
    session_id: Optional[UUID] = Field(default=None, alias="project_id")
    extra: Dict = Field(default_factory=dict)
    client: Client = Field(default_factory=_get_client, exclude=True)
    dotted_order: str = Field(
        default="", description="The order of the run in the tree."
    )
    trace_id: UUID = Field(default="", description="The trace id of the run.")  # type: ignore

    class Config:
        """Pydantic model configuration."""

        arbitrary_types_allowed = True
        allow_population_by_field_name = True
        extra = "allow"

    @validator("client", pre=True)
    def validate_client(cls, v: Optional[Client]) -> Client:
        """Ensure the client is specified."""
        if v is None:
            return _get_client()
        return v

    @root_validator(pre=True)
    def infer_defaults(cls, values: dict) -> dict:
        """Assign name to the run."""
        if "serialized" not in values:
            values["serialized"] = {"name": values["name"]}
        if values.get("parent_run") is not None:
            values["parent_run_id"] = values["parent_run"].id
        if "id" not in values:
            values["id"] = uuid4()
        if "trace_id" not in values:
            if "parent_run" in values:
                values["trace_id"] = values["parent_run"].trace_id
            else:
                values["trace_id"] = values["id"]
        cast(dict, values.setdefault("extra", {}))
        if values.get("events") is None:
            values["events"] = []
        if values.get("tags") is None:
            values["tags"] = []
        return values

    @root_validator(pre=False)
    def ensure_dotted_order(cls, values: dict) -> dict:
        """Ensure the dotted order of the run."""
        current_dotted_order = values.get("dotted_order")
        if current_dotted_order and current_dotted_order.strip():
            return values
        current_dotted_order = _create_current_dotted_order(
            values["start_time"], values["id"]
        )
        if values["parent_run"]:
            values["dotted_order"] = (
                values["parent_run"].dotted_order + "." + current_dotted_order
            )
        else:
            values["dotted_order"] = current_dotted_order
        return values

    def add_tags(self, tags: Union[Sequence[str], str]) -> None:
        """Add tags to the run."""
        if isinstance(tags, str):
            tags = [tags]
        if self.tags is None:
            self.tags = []
        self.tags.extend(tags)

    def add_metadata(self, metadata: Dict[str, Any]) -> None:
        """Add metadata to the run."""
        if self.extra is None:
            self.extra = {}
        metadata_: dict = self.extra.setdefault("metadata", {})
        metadata_.update(metadata)

    def add_outputs(self, outputs: Dict[str, Any]) -> None:
        """Upsert the given outputs into the run.

        Args:
            outputs (Dict[str, Any]): A dictionary containing the outputs to be added.

        Returns:
            None
        """
        if self.outputs is None:
            self.outputs = {}
        self.outputs.update(outputs)

    def add_event(
        self,
        events: Union[
            ls_schemas.RunEvent,
            Sequence[ls_schemas.RunEvent],
            Sequence[dict],
            dict,
            str,
        ],
    ) -> None:
        """Add an event to the list of events.

        Args:
            events (Union[ls_schemas.RunEvent, Sequence[ls_schemas.RunEvent],
                    Sequence[dict], dict, str]):
                The event(s) to be added. It can be a single event, a sequence
                    of events,
                a sequence of dictionaries, a dictionary, or a string.

        Returns:
            None
        """
        if self.events is None:
            self.events = []
        if isinstance(events, dict):
            self.events.append(events)  # type: ignore[arg-type]
        elif isinstance(events, str):
            self.events.append(
                {
                    "name": "event",
                    "time": datetime.now(timezone.utc).isoformat(),
                    "message": events,
                }
            )
        else:
            self.events.extend(events)  # type: ignore[arg-type]

    def end(
        self,
        *,
        outputs: Optional[Dict] = None,
        error: Optional[str] = None,
        end_time: Optional[datetime] = None,
        events: Optional[Sequence[ls_schemas.RunEvent]] = None,
    ) -> None:
        """Set the end time of the run and all child runs."""
        self.end_time = end_time or datetime.now(timezone.utc)
        if outputs is not None:
            if not self.outputs:
                self.outputs = outputs
            else:
                self.outputs.update(outputs)
        if error is not None:
            self.error = error
        if events is not None:
            self.add_event(events)

    def create_child(
        self,
        name: str,
        run_type: RUN_TYPE_T = "chain",
        *,
        run_id: Optional[ID_TYPE] = None,
        serialized: Optional[Dict] = None,
        inputs: Optional[Dict] = None,
        outputs: Optional[Dict] = None,
        error: Optional[str] = None,
        reference_example_id: Optional[UUID] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        tags: Optional[List[str]] = None,
        extra: Optional[Dict] = None,
    ) -> RunTree:
        """Add a child run to the run tree."""
        serialized_ = serialized or {"name": name}
        run = RunTree(
            name=name,
            id=_ensure_uuid(run_id),
            serialized=serialized_,
            inputs=inputs or {},
            outputs=outputs or {},
            error=error,
            run_type=run_type,
            reference_example_id=reference_example_id,
            start_time=start_time or datetime.now(timezone.utc),
            end_time=end_time,
            extra=extra or {},
            parent_run=self,
            project_name=self.session_name,
            client=self.client,
            tags=tags,
        )
        self.child_runs.append(run)
        return run

    def _get_dicts_safe(self):
        # Things like generators cannot be copied
        self_dict = self.dict(
            exclude={"child_runs", "inputs", "outputs"}, exclude_none=True
        )
        if self.inputs is not None:
            # shallow copy. deep copying will occur in the client
            self_dict["inputs"] = self.inputs.copy()
        if self.outputs is not None:
            # shallow copy; deep copying will occur in the client
            self_dict["outputs"] = self.outputs.copy()
        return self_dict

    def post(self, exclude_child_runs: bool = True) -> None:
        """Post the run tree to the API asynchronously."""
        kwargs = self._get_dicts_safe()
        self.client.create_run(**kwargs)
        if not exclude_child_runs:
            for child_run in self.child_runs:
                child_run.post(exclude_child_runs=False)

    def patch(self) -> None:
        """Patch the run tree to the API in a background thread."""
        if not self.end_time:
            self.end()
        self.client.update_run(
            run_id=self.id,
            outputs=self.outputs.copy() if self.outputs else None,
            error=self.error,
            parent_run_id=self.parent_run_id,
            reference_example_id=self.reference_example_id,
            end_time=self.end_time,
            dotted_order=self.dotted_order,
            trace_id=self.trace_id,
            events=self.events,
            tags=self.tags,
            extra=self.extra,
        )

    def wait(self) -> None:
        """Wait for all _futures to complete."""
        pass

    def get_url(self) -> str:
        """Return the URL of the run."""
        return self.client.get_run_url(run=self)

    @classmethod
    def from_dotted_order(
        cls,
        dotted_order: str,
        **kwargs: Any,
    ) -> RunTree:
        """Create a new 'child' span from the provided dotted order.

        Returns:
            RunTree: The new span.
        """
        headers = {
            f"{LANGSMITH_DOTTED_ORDER}": dotted_order,
        }
        return cast(RunTree, cls.from_headers(headers, **kwargs))

    @classmethod
    def from_runnable_config(
        cls,
        config: Optional[dict],
        **kwargs: Any,
    ) -> Optional[RunTree]:
        """Create a new 'child' span from the provided runnable config.

        Requires langchain to be installed.

        Returns:
            Optional[RunTree]: The new span or None if
                no parent span information is found.
        """
        try:
            from langchain_core.callbacks.manager import (
                AsyncCallbackManager,
                CallbackManager,
            )
            from langchain_core.runnables import RunnableConfig, ensure_config
            from langchain_core.tracers.langchain import LangChainTracer
        except ImportError as e:
            raise ImportError(
                "RunTree.from_runnable_config requires langchain-core to be installed. "
                "You can install it with `pip install langchain-core`."
            ) from e
        config_ = ensure_config(
            cast(RunnableConfig, config) if isinstance(config, dict) else None
        )
        if (
            (cb := config_.get("callbacks"))
            and isinstance(cb, (CallbackManager, AsyncCallbackManager))
            and cb.parent_run_id
            and (
                tracer := next(
                    (t for t in cb.handlers if isinstance(t, LangChainTracer)),
                    None,
                )
            )
        ):
            if (run := tracer.run_map.get(str(cb.parent_run_id))) and run.dotted_order:
                dotted_order = run.dotted_order
                kwargs["run_type"] = run.run_type
                kwargs["inputs"] = run.inputs
                kwargs["outputs"] = run.outputs
                kwargs["start_time"] = run.start_time
                kwargs["end_time"] = run.end_time
            elif hasattr(tracer, "order_map") and cb.parent_run_id in tracer.order_map:
                dotted_order = tracer.order_map[cb.parent_run_id][1]
            else:
                return None
            kwargs["client"] = tracer.client
            kwargs["project_name"] = tracer.project_name
            return RunTree.from_dotted_order(dotted_order, **kwargs)
        return None

    @classmethod
    def from_headers(cls, headers: Dict[str, str], **kwargs: Any) -> Optional[RunTree]:
        """Create a new 'parent' span from the provided headers.

        Extracts parent span information from the headers and creates a new span.
        Metadata and tags are extracted from the baggage header.
        The dotted order and trace id are extracted from the trace header.

        Returns:
            Optional[RunTree]: The new span or None if
                no parent span information is found.
        """
        init_args = kwargs.copy()

        langsmith_trace = headers.get(f"{LANGSMITH_DOTTED_ORDER}")
        if not langsmith_trace:
            return  # type: ignore[return-value]

        parent_dotted_order = langsmith_trace.strip()
        parsed_dotted_order = _parse_dotted_order(parent_dotted_order)
        trace_id = parsed_dotted_order[0][1]
        init_args["trace_id"] = trace_id
        init_args["id"] = parsed_dotted_order[-1][1]
        init_args["dotted_order"] = parent_dotted_order
        if len(parsed_dotted_order) >= 2:
            # Has a parent
            init_args["parent_run_id"] = parsed_dotted_order[-2][1]
        # All placeholders. We assume the source process
        # handles the life-cycle of the run.
        init_args["start_time"] = init_args.get("start_time") or datetime.now(
            timezone.utc
        )
        init_args["run_type"] = init_args.get("run_type") or "chain"
        init_args["name"] = init_args.get("name") or "parent"

        baggage = _Baggage.from_header(headers.get("baggage"))
        if baggage.metadata or baggage.tags:
            init_args["extra"] = init_args.setdefault("extra", {})
            init_args["extra"]["metadata"] = init_args["extra"].setdefault(
                "metadata", {}
            )
            metadata = {**baggage.metadata, **init_args["extra"]["metadata"]}
            init_args["extra"]["metadata"] = metadata
            tags = sorted(set(baggage.tags + init_args.get("tags", [])))
            init_args["tags"] = tags

        return RunTree(**init_args)

    def to_headers(self) -> Dict[str, str]:
        """Return the RunTree as a dictionary of headers."""
        headers = {}
        if self.trace_id:
            headers[f"{LANGSMITH_DOTTED_ORDER}"] = self.dotted_order
        baggage = _Baggage(
            metadata=self.extra.get("metadata", {}),
            tags=self.tags,
        )
        headers["baggage"] = baggage.to_header()
        return headers


class _Baggage:
    """Baggage header information."""

    def __init__(
        self,
        metadata: Optional[Dict[str, str]] = None,
        tags: Optional[List[str]] = None,
    ):
        """Initialize the Baggage object."""
        self.metadata = metadata or {}
        self.tags = tags or []

    @classmethod
    def from_header(cls, header_value: Optional[str]) -> _Baggage:
        """Create a Baggage object from the given header value."""
        if not header_value:
            return cls()
        metadata = {}
        tags = []
        try:
            for item in header_value.split(","):
                key, value = item.split("=", 1)
                if key == f"{LANGSMITH_PREFIX}metadata":
                    metadata = json.loads(urllib.parse.unquote(value))
                elif key == f"{LANGSMITH_PREFIX}tags":
                    tags = urllib.parse.unquote(value).split(",")
        except Exception as e:
            logger.warning(f"Error parsing baggage header: {e}")

        return cls(metadata=metadata, tags=tags)

    def to_header(self) -> str:
        """Return the Baggage object as a header value."""
        items = []
        if self.metadata:
            serialized_metadata = _dumps_json(self.metadata)
            items.append(
                f"{LANGSMITH_PREFIX}metadata={urllib.parse.quote(serialized_metadata)}"
            )
        if self.tags:
            serialized_tags = ",".join(self.tags)
            items.append(
                f"{LANGSMITH_PREFIX}tags={urllib.parse.quote(serialized_tags)}"
            )
        return ",".join(items)


def _parse_dotted_order(dotted_order: str) -> List[Tuple[datetime, UUID]]:
    """Parse the dotted order string."""
    parts = dotted_order.split(".")
    return [
        (datetime.strptime(part[:-36], "%Y%m%dT%H%M%S%fZ"), UUID(part[-36:]))
        for part in parts
    ]


def _create_current_dotted_order(
    start_time: Optional[datetime], run_id: Optional[UUID]
) -> str:
    """Create the current dotted order."""
    st = start_time or datetime.now(timezone.utc)
    id_ = run_id or uuid4()
    return st.strftime("%Y%m%dT%H%M%S%fZ") + str(id_)


__all__ = ["RunTree", "RunTree"]
