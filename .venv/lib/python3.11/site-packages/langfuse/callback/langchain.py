import httpx
import logging
import typing

import pydantic

try:  # Test that langchain is installed before proceeding
    import langchain  # noqa
except ImportError as e:
    log = logging.getLogger("langfuse")
    log.error(
        f"Could not import langchain. The langchain integration will not work. {e}"
    )
from typing import Any, Dict, List, Optional, Sequence, Union
from uuid import UUID, uuid4
from langfuse.api.resources.ingestion.types.sdk_log_body import SdkLogBody
from langfuse.client import (
    StatefulSpanClient,
    StatefulTraceClient,
)
from langfuse.extract_model import _extract_model_name
from langfuse.utils import _get_timestamp
from langfuse.utils.base_callback_handler import LangfuseBaseCallbackHandler

try:
    from langchain.callbacks.base import (
        BaseCallbackHandler as LangchainBaseCallbackHandler,
    )
    from langchain.schema.agent import AgentAction, AgentFinish
    from langchain.schema.document import Document
    from langchain_core.outputs import (
        ChatGeneration,
        LLMResult,
    )
    from langchain_core.messages import (
        AIMessage,
        BaseMessage,
        ChatMessage,
        HumanMessage,
        SystemMessage,
        ToolMessage,
        FunctionMessage,
    )
except ImportError:
    raise ModuleNotFoundError(
        "Please install langchain to use the Langfuse langchain integration: 'pip install langchain'"
    )


class LangchainCallbackHandler(
    LangchainBaseCallbackHandler, LangfuseBaseCallbackHandler
):
    log = logging.getLogger("langfuse")
    next_span_id: Optional[str] = None

    def __init__(
        self,
        public_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        host: Optional[str] = None,
        debug: bool = False,
        stateful_client: Optional[
            Union[StatefulTraceClient, StatefulSpanClient]
        ] = None,
        update_stateful_client: bool = False,
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
        trace_name: Optional[str] = None,
        release: Optional[str] = None,
        version: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        threads: Optional[int] = None,
        flush_at: Optional[int] = None,
        flush_interval: Optional[int] = None,
        max_retries: Optional[int] = None,
        timeout: Optional[int] = None,
        enabled: Optional[bool] = None,
        httpx_client: Optional[httpx.Client] = None,
        sdk_integration: Optional[str] = None,
    ) -> None:
        LangfuseBaseCallbackHandler.__init__(
            self,
            public_key=public_key,
            secret_key=secret_key,
            host=host,
            debug=debug,
            stateful_client=stateful_client,
            update_stateful_client=update_stateful_client,
            session_id=session_id,
            user_id=user_id,
            trace_name=trace_name,
            release=release,
            version=version,
            metadata=metadata,
            tags=tags,
            threads=threads,
            flush_at=flush_at,
            flush_interval=flush_interval,
            max_retries=max_retries,
            timeout=timeout,
            enabled=enabled,
            httpx_client=httpx_client,
            sdk_integration=sdk_integration or "langchain",
        )

        self.runs = {}

        if stateful_client and isinstance(stateful_client, StatefulSpanClient):
            self.runs[stateful_client.id] = stateful_client

    def setNextSpan(self, id: str):
        self.next_span_id = id

    def on_llm_new_token(
        self,
        token: str,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> Any:
        """Run on new LLM token. Only available when streaming is enabled."""
        # Nothing needs to happen here for langfuse. Once the streaming is done,
        self.log.debug(
            f"on llm new token: run_id: {run_id} parent_run_id: {parent_run_id}"
        )

    def get_langchain_run_name(self, serialized: Dict[str, Any], **kwargs: Any) -> str:
        """Retrieves the 'run_name' for an entity based on Langchain convention, prioritizing the 'name'
        key in 'kwargs' or falling back to the 'name' or 'id' in 'serialized'. Defaults to "<unknown>"
        if none are available.

        Args:
            serialized (Dict[str, Any]): A dictionary containing the entity's serialized data.
            **kwargs (Any): Additional keyword arguments, potentially including the 'name' override.

        Returns:
            str: The determined Langchain run name for the entity.
        """
        # Check if 'name' is in kwargs and not None, otherwise use default fallback logic
        if "name" in kwargs and kwargs["name"] is not None:
            return kwargs["name"]

        # Fallback to serialized 'name', 'id', or "<unknown>"
        return serialized.get("name", serialized.get("id", ["<unknown>"])[-1])

    def on_retriever_error(
        self,
        error: Union[Exception, KeyboardInterrupt],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> Any:
        """Run when Retriever errors."""
        try:
            self._log_debug_event(
                "on_retriever_error", run_id, parent_run_id, error=error
            )

            if run_id is None or run_id not in self.runs:
                raise Exception("run not found")

            self.runs[run_id] = self.runs[run_id].end(
                level="ERROR",
                status_message=str(error),
                version=self.version,
                input=kwargs.get("inputs"),
            )
        except Exception as e:
            self.log.exception(e)

    def on_chain_start(
        self,
        serialized: Dict[str, Any],
        inputs: Dict[str, Any],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        try:
            self._log_debug_event(
                "on_chain_start", run_id, parent_run_id, inputs=inputs
            )
            self.__generate_trace_and_parent(
                serialized=serialized,
                inputs=inputs,
                run_id=run_id,
                parent_run_id=parent_run_id,
                tags=tags,
                metadata=metadata,
                version=self.version,
                **kwargs,
            )

            content = {
                "id": self.next_span_id,
                "trace_id": self.trace.id,
                "name": self.get_langchain_run_name(serialized, **kwargs),
                "metadata": self.__join_tags_and_metadata(tags, metadata),
                "input": inputs,
                "version": self.version,
            }
            if parent_run_id is None:
                if self.root_span is None:
                    self.runs[run_id] = self.trace.span(**content)
                else:
                    self.runs[run_id] = self.root_span.span(**content)
            if parent_run_id is not None:
                self.runs[run_id] = self.runs[parent_run_id].span(**content)

        except Exception as e:
            self.log.exception(e)

    def __generate_trace_and_parent(
        self,
        serialized: Dict[str, Any],
        inputs: Union[Dict[str, Any], List[str], str, None],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        try:
            class_name = self.get_langchain_run_name(serialized, **kwargs)

            # on a new invocation, and not user provided root, we want to initialise a new traceo
            # parent_run_id is None when we are at the root of a langchain execution
            if (
                self.trace is not None
                and parent_run_id is None
                and self.langfuse is not None
            ):
                self.trace = None

            if (
                self.trace is not None
                and parent_run_id is None  # We are at the root of a langchain execution
                and self.langfuse is None  # StatefulClient was provided by user
                and self.update_stateful_client
            ):
                params = {
                    "name": self.trace_name
                    if self.trace_name is not None
                    else class_name,
                    "metadata": self.__join_tags_and_metadata(
                        tags, metadata, trace_metadata=self.metadata
                    ),
                    "version": self.version,
                    "session_id": self.session_id,
                    "user_id": self.user_id,
                    "tags": self.tags,
                    "input": inputs,
                }

                if self.root_span:
                    self.root_span.update(**params)
                else:
                    self.trace.update(**params)

            # if we are at a root, but langfuse exists, it means we do not have a
            # root provided by a user. Initialise it by creating a trace and root span.
            if self.trace is None and self.langfuse is not None:
                trace = self.langfuse.trace(
                    id=str(run_id),
                    name=self.trace_name if self.trace_name is not None else class_name,
                    metadata=self.__join_tags_and_metadata(
                        tags, metadata, trace_metadata=self.metadata
                    ),
                    version=self.version,
                    session_id=self.session_id,
                    user_id=self.user_id,
                    tags=self.tags,
                    input=inputs,
                )

                self.trace = trace

                if parent_run_id is not None and parent_run_id in self.runs:
                    self.runs[run_id] = self.trace.span(
                        id=self.next_span_id,
                        trace_id=self.trace.id,
                        name=class_name,
                        metadata=self.__join_tags_and_metadata(tags, metadata),
                        input=inputs,
                        version=self.version,
                    )

                return

        except Exception as e:
            self.log.exception(e)

    def on_agent_action(
        self,
        action: AgentAction,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> Any:
        """Run on agent action."""
        try:
            self._log_debug_event(
                "on_agent_action", run_id, parent_run_id, action=action
            )

            if run_id not in self.runs:
                raise Exception("run not found")

            self.runs[run_id] = self.runs[run_id].end(
                output=action,
                version=self.version,
                input=kwargs.get("inputs"),
            )

        except Exception as e:
            self.log.exception(e)

    def on_agent_finish(
        self,
        finish: AgentFinish,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> Any:
        try:
            self._log_debug_event(
                "on_agent_finish", run_id, parent_run_id, finish=finish
            )
            if run_id not in self.runs:
                raise Exception("run not found")

            self.runs[run_id] = self.runs[run_id].end(
                output=finish,
                version=self.version,
                input=kwargs.get("inputs"),
            )

            # langchain sends same run_id for agent_finish and chain_end for the same agent interaction.
            # Hence, we only delete at chain_end and not here.
            self._update_trace_and_remove_state(
                run_id, parent_run_id, finish, keep_state=True
            )

        except Exception as e:
            self.log.exception(e)

    def on_chain_end(
        self,
        outputs: Dict[str, Any],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> Any:
        try:
            self._log_debug_event(
                "on_chain_end", run_id, parent_run_id, outputs=outputs
            )

            if run_id not in self.runs:
                raise Exception("run not found")

            self.runs[run_id] = self.runs[run_id].end(
                output=outputs,
                version=self.version,
                input=kwargs.get("inputs"),
            )
            self._update_trace_and_remove_state(
                run_id, parent_run_id, outputs, input=kwargs.get("inputs")
            )
        except Exception as e:
            self.log.exception(e)

    def on_chain_error(
        self,
        error: Union[Exception, KeyboardInterrupt],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> None:
        try:
            self._log_debug_event("on_chain_error", run_id, parent_run_id, error=error)
            self.runs[run_id] = self.runs[run_id].end(
                level="ERROR",
                status_message=str(error),
                version=self.version,
                input=kwargs.get("inputs"),
            )

            self._update_trace_and_remove_state(
                run_id, parent_run_id, error, input=kwargs.get("inputs")
            )

        except Exception as e:
            self.log.exception(e)

    def on_chat_model_start(
        self,
        serialized: Dict[str, Any],
        messages: List[List[BaseMessage]],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        try:
            self._log_debug_event(
                "on_chat_model_start", run_id, parent_run_id, messages=messages
            )
            self.__on_llm_action(
                serialized,
                run_id,
                _flatten_comprehension(
                    [self._create_message_dicts(m) for m in messages]
                ),
                parent_run_id,
                tags=tags,
                metadata=metadata,
                **kwargs,
            )
        except Exception as e:
            self.log.exception(e)

    def on_llm_start(
        self,
        serialized: Dict[str, Any],
        prompts: List[str],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        try:
            self._log_debug_event(
                "on_llm_start", run_id, parent_run_id, prompts=prompts
            )
            self.__on_llm_action(
                serialized,
                run_id,
                prompts[0] if len(prompts) == 1 else prompts,
                parent_run_id,
                tags=tags,
                metadata=metadata,
                **kwargs,
            )
        except Exception as e:
            self.log.exception(e)

    def on_tool_start(
        self,
        serialized: Dict[str, Any],
        input_str: str,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        try:
            self._log_debug_event(
                "on_tool_start", run_id, parent_run_id, input_str=input_str
            )

            if parent_run_id is None or parent_run_id not in self.runs:
                raise Exception("parent run not found")
            meta = self.__join_tags_and_metadata(tags, metadata)

            if not meta:
                meta = {}

            meta.update(
                {key: value for key, value in kwargs.items() if value is not None}
            )

            self.runs[run_id] = self.runs[parent_run_id].span(
                id=self.next_span_id,
                name=self.get_langchain_run_name(serialized, **kwargs),
                input=input_str,
                metadata=meta,
                version=self.version,
            )
            self.next_span_id = None
        except Exception as e:
            self.log.exception(e)

    def on_retriever_start(
        self,
        serialized: Dict[str, Any],
        query: str,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        try:
            self._log_debug_event(
                "on_retriever_start", run_id, parent_run_id, query=query
            )
            if parent_run_id is None or parent_run_id not in self.runs:
                raise Exception("parent run not found")

            self.runs[run_id] = self.runs[parent_run_id].span(
                id=self.next_span_id,
                name=self.get_langchain_run_name(serialized, **kwargs),
                input=query,
                metadata=self.__join_tags_and_metadata(tags, metadata),
                version=self.version,
            )
            self.next_span_id = None
        except Exception as e:
            self.log.exception(e)

    def on_retriever_end(
        self,
        documents: Sequence[Document],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> Any:
        try:
            self._log_debug_event(
                "on_retriever_end", run_id, parent_run_id, documents=documents
            )
            if run_id is None or run_id not in self.runs:
                raise Exception("run not found")

            self.runs[run_id] = self.runs[run_id].end(
                output=documents,
                version=self.version,
                input=kwargs.get("inputs"),
            )

            self._update_trace_and_remove_state(run_id, parent_run_id, documents)

        except Exception as e:
            self.log.exception(e)

    def on_tool_end(
        self,
        output: str,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> Any:
        try:
            self._log_debug_event("on_tool_end", run_id, parent_run_id, output=output)
            if run_id is None or run_id not in self.runs:
                raise Exception("run not found")

            self.runs[run_id] = self.runs[run_id].end(
                output=output,
                version=self.version,
                input=kwargs.get("inputs"),
            )

            self._update_trace_and_remove_state(run_id, parent_run_id, output)

        except Exception as e:
            self.log.exception(e)

    def on_tool_error(
        self,
        error: Union[Exception, KeyboardInterrupt],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> Any:
        try:
            self._log_debug_event("on_tool_error", run_id, parent_run_id, error=error)
            if run_id is None or run_id not in self.runs:
                raise Exception("run not found")

            self.runs[run_id] = self.runs[run_id].end(
                status_message=str(error),
                level="ERROR",
                version=self.version,
                input=kwargs.get("inputs"),
            )

            self._update_trace_and_remove_state(run_id, parent_run_id, error)

        except Exception as e:
            self.log.exception(e)

    def __on_llm_action(
        self,
        serialized: Dict[str, Any],
        run_id: UUID,
        prompts: List[str],
        parent_run_id: Optional[UUID] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        try:
            self.__generate_trace_and_parent(
                serialized,
                inputs=prompts[0] if len(prompts) == 1 else prompts,
                run_id=run_id,
                parent_run_id=parent_run_id,
                tags=tags,
                metadata=metadata,
                version=self.version,
                kwargs=kwargs,
            )

            model_name = None

            model_name = self._parse_model_and_log_errors(serialized, kwargs)

            content = {
                "name": self.get_langchain_run_name(serialized, **kwargs),
                "input": prompts,
                "metadata": self.__join_tags_and_metadata(tags, metadata),
                "model": model_name,
                "model_parameters": {
                    key: value
                    for key, value in {
                        "temperature": kwargs["invocation_params"].get("temperature"),
                        "max_tokens": kwargs["invocation_params"].get("max_tokens"),
                        "top_p": kwargs["invocation_params"].get("top_p"),
                        "frequency_penalty": kwargs["invocation_params"].get(
                            "frequency_penalty"
                        ),
                        "presence_penalty": kwargs["invocation_params"].get(
                            "presence_penalty"
                        ),
                        "request_timeout": kwargs["invocation_params"].get(
                            "request_timeout"
                        ),
                    }.items()
                    if value is not None
                },
                "version": self.version,
            }

            if parent_run_id in self.runs:
                self.runs[run_id] = self.runs[parent_run_id].generation(**content)
            elif self.root_span is not None and parent_run_id is None:
                self.runs[run_id] = self.root_span.generation(**content)
            else:
                self.runs[run_id] = self.trace.generation(**content)

        except Exception as e:
            self.log.exception(e)

    def _parse_model_and_log_errors(self, serialized, kwargs):
        """Parse the model name from the serialized object or kwargs. If it fails, send the error log to the server and return None."""
        try:
            model_name = _extract_model_name(serialized, **kwargs)
            if model_name:
                return model_name

            if model_name is None:
                self.log.warning(
                    "Langfuse was not able to parse the LLM model. The LLM call will be recorded without model name. Please create an issue so we can fix your integration: https://github.com/langfuse/langfuse/issues/new/choose"
                )
                self._report_error(
                    {
                        "log": "unable to parse model name",
                        "kwargs": str(kwargs),
                        "serialized": str(serialized),
                    }
                )
        except Exception as e:
            self.log.exception(e)
            self.log.warning(
                "Langfuse was not able to parse the LLM model. The LLM call will be recorded without model name. Please create an issue so we can fix your integration: https://github.com/langfuse/langfuse/issues/new/choose"
            )
            self._report_error(
                {
                    "log": "unable to parse model name",
                    "kwargs": str(kwargs),
                    "serialized": str(serialized),
                    "exception": str(e),
                }
            )

            return None

    def on_llm_end(
        self,
        response: LLMResult,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> Any:
        try:
            self._log_debug_event(
                "on_llm_end", run_id, parent_run_id, response=response, kwargs=kwargs
            )
            if run_id not in self.runs:
                raise Exception("Run not found, see docs what to do in this case.")
            else:
                generation = response.generations[-1][-1]
                extracted_response = (
                    self._convert_message_to_dict(generation.message)
                    if isinstance(generation, ChatGeneration)
                    else _extract_raw_esponse(generation)
                )

                llm_usage = _parse_usage(response)
                self.runs[run_id] = self.runs[run_id].end(
                    output=extracted_response,
                    usage=llm_usage,
                    version=self.version,
                    input=kwargs.get("inputs"),
                )

                self._update_trace_and_remove_state(
                    run_id, parent_run_id, extracted_response
                )

        except Exception as e:
            self.log.exception(e)

    def on_llm_error(
        self,
        error: Union[Exception, KeyboardInterrupt],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> Any:
        try:
            self._log_debug_event("on_llm_error", run_id, parent_run_id, error=error)
            self.runs[run_id] = self.runs[run_id].end(
                status_message=str(error),
                level="ERROR",
                version=self.version,
                input=kwargs.get("inputs"),
            )
            self._update_trace_and_remove_state(run_id, parent_run_id, error)

        except Exception as e:
            self.log.exception(e)

    def __join_tags_and_metadata(
        self,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        trace_metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        final_dict = {}
        if tags is not None and len(tags) > 0:
            final_dict["tags"] = tags
        if metadata is not None:
            final_dict.update(metadata)
        if trace_metadata is not None:
            final_dict.update(trace_metadata)
        return final_dict if final_dict != {} else None

    def _report_error(self, error: dict):
        event = SdkLogBody(log=error)

        self._task_manager.add_task(
            {
                "id": str(uuid4()),
                "type": "sdk-log",
                "timestamp": _get_timestamp(),
                "body": event.dict(),
            }
        )

    def _update_trace_and_remove_state(
        self,
        run_id: str,
        parent_run_id: Optional[str],
        output: any,
        *,
        keep_state: bool = False,
        **kwargs: Any,
    ):
        """Update the trace with the output of the current run. Called at every finish callback event."""
        if (
            parent_run_id
            is None  # If we are at the root of the langchain execution -> reached the end of the root
            and self.trace is not None  # We do have a trace available
            and self.trace.id
            == str(run_id)  # The trace was generated by langchain and not by the user
        ):
            self.trace = self.trace.update(output=output, **kwargs)

        elif (
            parent_run_id is None
            and self.trace is not None  # We have a user-provided parent
            and self.update_stateful_client
        ):
            if self.root_span is not None:
                self.root_span = self.root_span.update(output=output, **kwargs)
            else:
                self.trace = self.trace.update(output=output, **kwargs)

        if not keep_state:
            del self.runs[run_id]

    def _convert_message_to_dict(self, message: BaseMessage) -> Dict[str, Any]:
        # assistant message
        if isinstance(message, HumanMessage):
            message_dict = {"role": "user", "content": message.content}
        elif isinstance(message, AIMessage):
            message_dict = {"role": "assistant", "content": message.content}
        elif isinstance(message, SystemMessage):
            message_dict = {"role": "system", "content": message.content}
        elif isinstance(message, ToolMessage):
            message_dict = {"role": "tool", "content": message.content}
        elif isinstance(message, FunctionMessage):
            message_dict = {"role": "function", "content": message.content}
        elif isinstance(message, ChatMessage):
            message_dict = {"role": message.role, "content": message.content}
        else:
            raise ValueError(f"Got unknown type {message}")
        if "name" in message.additional_kwargs:
            message_dict["name"] = message.additional_kwargs["name"]

        if message.additional_kwargs:
            message_dict["additional_kwargs"] = message.additional_kwargs

        return message_dict

    def _create_message_dicts(
        self, messages: List[BaseMessage]
    ) -> List[Dict[str, Any]]:
        return [self._convert_message_to_dict(m) for m in messages]

    def _log_debug_event(
        self,
        event_name: str,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs,
    ):
        kwargs_log = (
            ", " + ", ".join([f"{key}: {value}" for key, value in kwargs.items()])
            if len(kwargs) > 0
            else ""
        )
        self.log.debug(
            f"Event: {event_name}, run_id: {str(run_id)[:5]}, parent_run_id: {str(parent_run_id)[:5]}"
            + kwargs_log
        )


def _extract_raw_esponse(last_response):
    """Extract the response from the last response of the LLM call."""
    # We return the text of the response if not empty
    if last_response.text is not None and last_response.text.strip() != "":
        return last_response.text.strip()
    elif hasattr(last_response, "message"):
        # Additional kwargs contains the response in case of tool usage
        return last_response.message.additional_kwargs
    else:
        # Not tool usage, some LLM responses can be simply empty
        return ""


def _flatten_comprehension(matrix):
    return [item for row in matrix for item in row]


def _parse_usage_model(usage: typing.Union[pydantic.BaseModel, dict]):
    # maintains a list of key translations. For each key, the usage model is checked
    # and a new object will be created with the new key if the key exists in the usage model
    # All non matched keys will remain on the object.

    if hasattr(usage, "__dict__"):
        usage = usage.__dict__

    conversion_list = [
        # https://pypi.org/project/langchain-anthropic/
        ("input_tokens", "input"),
        ("output_tokens", "output"),
        # https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/get-token-count
        ("prompt_token_count", "input"),
        ("candidates_token_count", "output"),
    ]

    usage_model = usage.copy()  # Copy all existing key-value pairs
    for key, value in conversion_list:
        if key in usage_model:
            usage_model[value] = usage_model.pop(
                key
            )  # Translate key and keep the value

    return usage_model if usage_model else None


def _parse_usage(response: LLMResult):
    # langchain-anthropic uses the usage field
    llm_usage_keys = ["token_usage", "usage"]
    llm_usage = None
    if response.llm_output is not None:
        for key in llm_usage_keys:
            if key in response.llm_output and response.llm_output[key]:
                llm_usage = _parse_usage_model(response.llm_output[key])
                break

    if hasattr(response, "generations"):
        for generation in response.generations:
            for generation_chunk in generation:
                if generation_chunk.generation_info and (
                    "usage_metadata" in generation_chunk.generation_info
                ):
                    llm_usage = _parse_usage_model(
                        generation_chunk.generation_info["usage_metadata"]
                    )
                    break

    return llm_usage
