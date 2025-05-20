from typing import AsyncIterable
from common.types import (
    SendTaskRequest,
    TaskSendParams,
    Message,
    TaskStatus,
    Artifact,
    TaskStatusUpdateEvent,
    TaskArtifactUpdateEvent,
    TaskState,
    Task,
    SendTaskResponse,
    InternalError,
    JSONRPCResponse,
    SendTaskStreamingRequest,
    SendTaskStreamingResponse,
)
from common.server.task_manager import InMemoryTaskManager
from agent import MindsDBAgent
import common.server.utils as utils
from typing import Union
import logging

logger = logging.getLogger(__name__)


class AgentTaskManager(InMemoryTaskManager):

    def __init__(self, project_name: str, mindsdb_host: str, mindsdb_port: int):
        super().__init__()
        self.project_name = project_name
        self.mindsdb_host = mindsdb_host
        self.mindsdb_port = mindsdb_port

    def _create_agent(self, agent_name: str) -> MindsDBAgent:
        """Create a new MindsDBAgent instance for the given agent name."""
        return MindsDBAgent(
            agent_name=agent_name,
            project_name=self.project_name,
            host=self.mindsdb_host,
            port=self.mindsdb_port,
        )

    async def _stream_generator(
        self, request: SendTaskStreamingRequest
    ) -> AsyncIterable[SendTaskStreamingResponse] | JSONRPCResponse:
        task_send_params: TaskSendParams = request.params
        query = self._get_user_query(task_send_params)
        params = self._get_task_params(task_send_params)
        agent_name = params["agent_name"]
        streaming = params["streaming"]
        agent = self._create_agent(agent_name)

        if not streaming:
            # If streaming is disabled, use invoke and return a single response
            try:
                result = agent.invoke(query, task_send_params.sessionId)

                # Use the parts from the agent response if available, or create them
                if "parts" in result:
                    parts = result["parts"]
                else:
                    result_text = result.get("content", "No response from MindsDB")
                    parts = [{"type": "text", "text": result_text}]

                    # Check if we have structured data
                    if "data" in result and result["data"]:
                        parts.append(
                            {
                                "type": "data",
                                "data": result["data"],
                                "metadata": {"subtype": "json"},
                            }
                        )

                # Create and yield the final response
                task_state = TaskState.COMPLETED
                artifact = Artifact(parts=parts, index=0, append=False)
                task_status = TaskStatus(state=task_state)

                # Update the task store
                await self._update_store(task_send_params.id, task_status, [artifact])

                # Yield the artifact update
                yield SendTaskStreamingResponse(
                    id=request.id,
                    result=TaskArtifactUpdateEvent(
                        id=task_send_params.id, artifact=artifact
                    ),
                )

                # Yield the final status update
                yield SendTaskStreamingResponse(
                    id=request.id,
                    result=TaskStatusUpdateEvent(
                        id=task_send_params.id,
                        status=TaskStatus(state=task_status.state),
                        final=True,
                    ),
                )
                return

            except Exception as e:
                logger.error(f"Error invoking agent: {e}")
                yield JSONRPCResponse(
                    id=request.id,
                    error=InternalError(message=f"Error invoking agent: {str(e)}"),
                )
                return

        # If streaming is enabled (default), use the streaming implementation
        try:
            # Track the chunks we've seen to avoid duplicates
            seen_chunks = set()

            async for item in agent.stream(query, task_send_params.sessionId):
                # Ensure item has the required fields or provide defaults
                is_task_complete = item.get("is_task_complete", False)

                # Extract parts from the chunk
                parts = []

                # Handle different chunk formats to extract text content
                if "parts" in item and item["parts"]:
                    parts = item["parts"]
                elif "content" in item:
                    parts = [{"type": "text", "text": item["content"]}]
                elif "output" in item:
                    parts = [{"type": "text", "text": item["output"]}]
                elif "actions" in item:
                    # Extract thought process from actions
                    for action in item.get("actions", []):
                        if "log" in action:
                            parts.append({"type": "text", "text": action["log"]})
                        if "tool_input" in action:
                            # Include SQL queries
                            tool_input = action.get("tool_input", "")
                            if "$START$" in tool_input and "$STOP$" in tool_input:
                                sql = tool_input.replace("$START$", "").replace("$STOP$", "")
                                parts.append({"type": "text", "text": sql})
                elif "steps" in item:
                    # Extract observations from steps
                    for step in item.get("steps", []):
                        if "observation" in step:
                            parts.append({"type": "text", "text": step["observation"]})
                elif "messages" in item:
                    # Extract content from messages
                    for message in item.get("messages", []):
                        if "content" in message:
                            parts.append({"type": "text", "text": message["content"]})

                # Skip if we have no parts to send
                if not parts:
                    continue

                # Generate a unique key for this chunk to avoid duplicates
                chunk_key = str(parts)
                if chunk_key in seen_chunks:
                    continue
                seen_chunks.add(chunk_key)

                # Ensure metadata exists
                metadata = item.get("metadata", {})

                # Handle error field if present
                if "error" in item and not is_task_complete:
                    logger.warning(f"Error in streaming response: {item['error']}")
                    # Mark as complete if there's an error
                    is_task_complete = True

                if not is_task_complete:
                    task_state = TaskState.WORKING
                    message = Message(role="agent", parts=parts, metadata=metadata)
                    task_status = TaskStatus(state=task_state, message=message)
                    await self._update_store(task_send_params.id, task_status, [])
                    task_update_event = TaskStatusUpdateEvent(
                        id=task_send_params.id,
                        status=task_status,
                        final=False,
                    )
                    yield SendTaskStreamingResponse(
                        id=request.id, result=task_update_event
                    )
                else:
                    task_state = TaskState.COMPLETED
                    artifact = Artifact(parts=parts, index=0, append=False)
                    task_status = TaskStatus(state=task_state)
                    yield SendTaskStreamingResponse(
                        id=request.id,
                        result=TaskArtifactUpdateEvent(
                            id=task_send_params.id, artifact=artifact
                        ),
                    )
                    await self._update_store(
                        task_send_params.id, task_status, [artifact]
                    )
                    yield SendTaskStreamingResponse(
                        id=request.id,
                        result=TaskStatusUpdateEvent(
                            id=task_send_params.id,
                            status=TaskStatus(
                                state=task_status.state,
                            ),
                            final=True,
                        ),
                    )

        except Exception as e:
            logger.error(f"An error occurred while streaming the response: {e}")
            error_text = f"An error occurred while streaming the response: {str(e)}"
            parts = [{"type": "text", "text": error_text}]

            # First send the error as an artifact
            artifact = Artifact(parts=parts, index=0, append=False)
            yield SendTaskStreamingResponse(
                id=request.id,
                result=TaskArtifactUpdateEvent(
                    id=task_send_params.id, artifact=artifact
                ),
            )

            # Then mark the task as completed with an error
            task_state = TaskState.FAILED
            task_status = TaskStatus(state=task_state)
            await self._update_store(
                task_send_params.id, task_status, [artifact]
            )

            # Send the final status update
            yield SendTaskStreamingResponse(
                id=request.id,
                result=TaskStatusUpdateEvent(
                    id=task_send_params.id,
                    status=task_status,
                    final=True,
                ),
            )

    def _validate_request(
        self, request: Union[SendTaskRequest, SendTaskStreamingRequest]
    ) -> None:
        task_send_params: TaskSendParams = request.params
        if not utils.are_modalities_compatible(
            task_send_params.acceptedOutputModes, MindsDBAgent.SUPPORTED_CONTENT_TYPES
        ):
            logger.warning(
                "Unsupported output mode. Received %s, Support %s",
                task_send_params.acceptedOutputModes,
                MindsDBAgent.SUPPORTED_CONTENT_TYPES,
            )
            return utils.new_incompatible_types_error(request.id)

    async def on_send_task(self, request: SendTaskRequest) -> SendTaskResponse:
        error = self._validate_request(request)
        if error:
            return error
        await self.upsert_task(request.params)
        return await self._invoke(request)

    async def on_send_task_subscribe(
        self, request: SendTaskStreamingRequest
    ) -> AsyncIterable[SendTaskStreamingResponse] | JSONRPCResponse:
        error = self._validate_request(request)
        if error:
            return error
        await self.upsert_task(request.params)
        return self._stream_generator(request)

    async def _update_store(
        self, task_id: str, status: TaskStatus, artifacts: list[Artifact]
    ) -> Task:
        async with self.lock:
            try:
                task = self.tasks[task_id]
            except KeyError:
                logger.error(f"Task {task_id} not found for updating the task")
                raise ValueError(f"Task {task_id} not found")
            task.status = status
            if artifacts is not None:
                if task.artifacts is None:
                    task.artifacts = []
                task.artifacts.extend(artifacts)
            return task

    def _get_user_query(self, task_send_params: TaskSendParams) -> str:
        """Extract the user query from the task parameters."""
        message = task_send_params.message
        if not message.parts:
            return ""

        # Find the first text part
        for part in message.parts:
            if part.type == "text":
                return part.text

        # If no text part found, return empty string
        return ""

    def _get_task_params(self, task_send_params: TaskSendParams) -> dict:
        """Extract common parameters from task metadata."""
        metadata = task_send_params.message.metadata or {}
        return {
            "agent_name": metadata.get("agent_name", "my_agent"),
            "streaming": metadata.get("streaming", True),
            "session_id": task_send_params.sessionId,
        }

    async def _invoke(self, request: SendTaskRequest) -> SendTaskResponse:
        task_send_params: TaskSendParams = request.params
        query = self._get_user_query(task_send_params)
        params = self._get_task_params(task_send_params)
        agent_name = params["agent_name"]
        streaming = params["streaming"]
        agent = self._create_agent(agent_name)

        try:
            # Always use streaming internally, but handle the response differently based on the streaming parameter
            all_parts = []
            final_metadata = {}

            # Create a streaming generator
            stream_gen = agent.stream(query, task_send_params.sessionId)

            if streaming:
                # For streaming mode, we'll use the streaming endpoint instead
                # Just create a minimal response to acknowledge the request
                task_state = TaskState.WORKING
                task = await self._update_store(
                    task_send_params.id,
                    TaskStatus(state=task_state),
                    []
                )
                return SendTaskResponse(id=request.id, result=task)
            else:
                # For non-streaming mode, collect all chunks into a single response
                async for chunk in stream_gen:
                    # Extract parts if they exist
                    if "parts" in chunk and chunk["parts"]:
                        all_parts.extend(chunk["parts"])
                    elif "content" in chunk:
                        all_parts.append({"type": "text", "text": chunk["content"]})

                    # Extract metadata if it exists
                    if "metadata" in chunk:
                        final_metadata.update(chunk["metadata"])

                # If we didn't get any parts, create a default part
                if not all_parts:
                    all_parts = [{"type": "text", "text": "No response from MindsDB"}]

                # Create the final response
                task_state = TaskState.COMPLETED
                task = await self._update_store(
                    task_send_params.id,
                    TaskStatus(state=task_state, message=Message(role="agent", parts=all_parts, metadata=final_metadata)),
                    [Artifact(parts=all_parts)],
                )
                return SendTaskResponse(id=request.id, result=task)
        except Exception as e:
            logger.error(f"Error invoking agent: {e}")
            result_text = f"Error invoking agent: {e}"
            parts = [{"type": "text", "text": result_text}]

            task_state = TaskState.FAILED
            task = await self._update_store(
                task_send_params.id,
                TaskStatus(state=task_state, message=Message(role="agent", parts=parts)),
                [Artifact(parts=parts)],
            )
            return SendTaskResponse(id=request.id, result=task)
