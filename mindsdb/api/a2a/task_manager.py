from typing import AsyncIterable
from mindsdb.api.a2a.common.types import (
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
    InvalidRequestError,
)
from mindsdb.api.a2a.common.server.task_manager import InMemoryTaskManager
from mindsdb.api.a2a.agent import MindsDBAgent

from typing import Union
import logging
import asyncio

logger = logging.getLogger(__name__)


class AgentTaskManager(InMemoryTaskManager):
    def __init__(
        self,
        project_name: str,
        mindsdb_host: str,
        mindsdb_port: int,
        agent_name: str = None,
    ):
        super().__init__()
        self.project_name = project_name
        self.mindsdb_host = mindsdb_host
        self.mindsdb_port = mindsdb_port
        self.agent_name = agent_name
        self.tasks = {}  # Task storage
        self.lock = asyncio.Lock()  # Lock for task operations

    def _create_agent(self, agent_name: str = None) -> MindsDBAgent:
        """Create a new MindsDBAgent instance for the given agent name."""
        if not agent_name:
            raise ValueError("Agent name is required but was not provided in the request")

        return MindsDBAgent(
            agent_name=agent_name,
            project_name=self.project_name,
            host=self.mindsdb_host,
            port=self.mindsdb_port,
        )

    async def _stream_generator(self, request: SendTaskStreamingRequest) -> AsyncIterable[SendTaskStreamingResponse]:
        task_send_params: TaskSendParams = request.params
        query = self._get_user_query(task_send_params)
        params = self._get_task_params(task_send_params)
        agent_name = params["agent_name"]
        streaming = params["streaming"]

        # Create and store the task first to ensure it exists
        try:
            task = await self.upsert_task(task_send_params)
            logger.info(f"Task created/updated with history length: {len(task.history) if task.history else 0}")
        except Exception as e:
            logger.error(f"Error creating task: {str(e)}")
            yield SendTaskStreamingResponse(
                id=request.id,
                error=InternalError(message=f"Error creating task: {str(e)}"),
            )
            return  # Early return from generator

        agent = self._create_agent(agent_name)

        # Get the history from the task
        history = task.history if task and task.history else []
        logger.info(f"Using history with length {len(history)} for request")

        # Log the history for debugging
        logger.info(f"Conversation history for task {task_send_params.id}:")
        for idx, msg in enumerate(history):
            # Convert Message object to dict if needed
            msg_dict = msg.dict() if hasattr(msg, "dict") else msg
            role = msg_dict.get("role", "unknown")
            text = ""
            for part in msg_dict.get("parts", []):
                if part.get("type") == "text":
                    text = part.get("text", "")
                    break
            logger.info(f"Message {idx + 1} ({role}): {text[:100]}...")

        if not streaming:
            # If streaming is disabled, use invoke and return a single response
            try:
                result = agent.invoke(query, task_send_params.sessionId, history=history)

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
                    result=TaskArtifactUpdateEvent(id=task_send_params.id, artifact=artifact),
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

            async for item in agent.stream(query, task_send_params.sessionId, history=history):
                # Ensure item has the required fields or provide defaults
                is_task_complete = item.get("is_task_complete", False)

                # Create a structured thought dictionary to encapsulate the agent's thought process
                thought_dict = {}
                parts = []

                # Handle different chunk formats to extract text content
                if "actions" in item:
                    # Extract thought process from actions
                    thought_dict["type"] = "thought"
                    thought_dict["actions"] = item["actions"]

                    for action in item.get("actions", []):
                        if "log" in action:
                            # Use "text" type for all parts, but add a thought_type in metadata
                            parts.append(
                                {
                                    "type": "text",
                                    "text": action["log"],
                                    "metadata": {"thought_type": "thought"},
                                }
                            )
                        if "tool_input" in action:
                            # Include SQL queries
                            tool_input = action.get("tool_input", "")
                            if "$START$" in tool_input and "$STOP$" in tool_input:
                                sql = tool_input.replace("$START$", "").replace("$STOP$", "")
                                parts.append(
                                    {
                                        "type": "text",
                                        "text": sql,
                                        "metadata": {"thought_type": "sql"},
                                    }
                                )

                elif "steps" in item:
                    # Extract observations from steps
                    thought_dict["type"] = "observation"
                    thought_dict["steps"] = item["steps"]

                    for step in item.get("steps", []):
                        if "observation" in step:
                            parts.append(
                                {
                                    "type": "text",
                                    "text": step["observation"],
                                    "metadata": {"thought_type": "observation"},
                                }
                            )
                        if "action" in step and "log" in step["action"]:
                            parts.append(
                                {
                                    "type": "text",
                                    "text": step["action"]["log"],
                                    "metadata": {"thought_type": "thought"},
                                }
                            )

                elif "output" in item:
                    # Final answer
                    thought_dict["type"] = "answer"
                    thought_dict["output"] = item["output"]
                    parts.append({"type": "text", "text": item["output"]})

                elif "parts" in item and item["parts"]:
                    # Use existing parts, but ensure they have valid types
                    for part in item["parts"]:
                        if part.get("type") in ["text", "file", "data"]:
                            # Valid type, use as is
                            parts.append(part)
                        else:
                            # Invalid type, convert to text
                            text_content = part.get("text", "")
                            if not text_content and "content" in part:
                                text_content = part["content"]

                            new_part = {"type": "text", "text": text_content}

                            # Preserve metadata if it exists
                            if "metadata" in part:
                                new_part["metadata"] = part["metadata"]
                            else:
                                new_part["metadata"] = {"thought_type": part.get("type", "text")}

                            parts.append(new_part)

                    # Try to determine the type from parts for the thought dictionary
                    for part in item["parts"]:
                        if part.get("type") == "text" and part.get("text", "").startswith("$START$"):
                            thought_dict["type"] = "sql"
                            thought_dict["query"] = part.get("text")
                        else:
                            thought_dict["type"] = "text"

                elif "content" in item:
                    # Simple content
                    thought_dict["type"] = "text"
                    thought_dict["content"] = item["content"]
                    parts.append({"type": "text", "text": item["content"]})

                elif "messages" in item:
                    # Extract content from messages
                    thought_dict["type"] = "message"
                    thought_dict["messages"] = item["messages"]

                    for message in item.get("messages", []):
                        if "content" in message:
                            parts.append(
                                {
                                    "type": "text",
                                    "text": message["content"],
                                    "metadata": {"thought_type": "message"},
                                }
                            )

                # Skip if we have no parts to send
                if not parts:
                    continue

                # Process each part individually to ensure true streaming
                for part in parts:
                    # Generate a unique key for this part to avoid duplicates
                    part_key = str(part)
                    if part_key in seen_chunks:
                        continue
                    seen_chunks.add(part_key)

                    # Ensure metadata exists
                    metadata = item.get("metadata", {})

                    # Add the thought dictionary to metadata for frontend parsing
                    if thought_dict:
                        metadata["thought_process"] = thought_dict

                    # Handle error field if present
                    if "error" in item and not is_task_complete:
                        logger.warning(f"Error in streaming response: {item['error']}")
                        # Mark as complete if there's an error
                        is_task_complete = True

                    if not is_task_complete:
                        # Create a message with just this part and send it immediately
                        task_state = TaskState.WORKING
                        message = Message(role="agent", parts=[part], metadata=metadata)
                        task_status = TaskStatus(state=task_state, message=message)
                        await self._update_store(task_send_params.id, task_status, [])
                        task_update_event = TaskStatusUpdateEvent(
                            id=task_send_params.id,
                            status=task_status,
                            final=False,
                        )
                        yield SendTaskStreamingResponse(id=request.id, result=task_update_event)

                # If this is the final chunk, send a completion message
                if is_task_complete:
                    task_state = TaskState.COMPLETED
                    artifact = Artifact(parts=parts, index=0, append=False)
                    task_status = TaskStatus(state=task_state)
                    yield SendTaskStreamingResponse(
                        id=request.id,
                        result=TaskArtifactUpdateEvent(id=task_send_params.id, artifact=artifact),
                    )
                    await self._update_store(task_send_params.id, task_status, [artifact])
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
                result=TaskArtifactUpdateEvent(id=task_send_params.id, artifact=artifact),
            )

            # Then mark the task as completed with an error
            task_state = TaskState.FAILED
            task_status = TaskStatus(state=task_state)
            await self._update_store(task_send_params.id, task_status, [artifact])

            # Send the final status update
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

    async def upsert_task(self, task_send_params: TaskSendParams) -> Task:
        """Create or update a task in the task store.

        Args:
            task_send_params: The parameters for the task.

        Returns:
            The created or updated task.
        """
        logger.info(f"Upserting task {task_send_params.id}")
        async with self.lock:
            task = self.tasks.get(task_send_params.id)
            if task is None:
                # Convert the message to a dict if it's not already one
                message = task_send_params.message
                message_dict = message.dict() if hasattr(message, "dict") else message

                # Get history from request if available
                history = []
                if hasattr(task_send_params, "history") and task_send_params.history:
                    # Convert each history item to dict if needed and ensure proper role
                    for item in task_send_params.history:
                        item_dict = item.dict() if hasattr(item, "dict") else item
                        # Ensure the role is properly set
                        if "role" not in item_dict:
                            item_dict["role"] = "assistant" if "answer" in item_dict else "user"
                        history.append(item_dict)

                # Add current message to history
                history.append(message_dict)

                # Create a new task
                task = Task(
                    id=task_send_params.id,
                    sessionId=task_send_params.sessionId,
                    status=TaskStatus(state=TaskState.SUBMITTED),
                    history=history,
                    artifacts=[],
                )
                self.tasks[task_send_params.id] = task
            else:
                # Convert the message to a dict if it's not already one
                message = task_send_params.message
                message_dict = message.dict() if hasattr(message, "dict") else message

                # Update the existing task
                if task.history is None:
                    task.history = []

                # If we have new history from the request, use it
                if hasattr(task_send_params, "history") and task_send_params.history:
                    # Convert each history item to dict if needed and ensure proper role
                    history = []
                    for item in task_send_params.history:
                        item_dict = item.dict() if hasattr(item, "dict") else item
                        # Ensure the role is properly set
                        if "role" not in item_dict:
                            item_dict["role"] = "assistant" if "answer" in item_dict else "user"
                        history.append(item_dict)
                    task.history = history

                # Add current message to history
                task.history.append(message_dict)
            return task

    def _validate_request(
        self, request: Union[SendTaskRequest, SendTaskStreamingRequest]
    ) -> Union[None, JSONRPCResponse]:
        """Validate the request and return an error response if invalid."""
        # Check if the request has the required parameters
        if not hasattr(request, "params") or not request.params:
            return JSONRPCResponse(
                id=request.id,
                error=InvalidRequestError(message="Missing params"),
            )

        # Check if the request has a message
        if not hasattr(request.params, "message") or not request.params.message:
            return JSONRPCResponse(
                id=request.id,
                error=InvalidRequestError(message="Missing message in params"),
            )

        # Check if the message has metadata
        if not hasattr(request.params.message, "metadata") or not request.params.message.metadata:
            return JSONRPCResponse(
                id=request.id,
                error=InvalidRequestError(message="Missing metadata in message"),
            )

        # Check if the agent name is provided in the metadata
        metadata = request.params.message.metadata
        agent_name = metadata.get("agent_name", metadata.get("agentName"))
        if not agent_name:
            return JSONRPCResponse(
                id=request.id,
                error=InvalidRequestError(
                    message="Agent name is required but was not provided in the request metadata"
                ),
            )

        return None

    async def on_send_task(self, request: SendTaskRequest) -> SendTaskResponse:
        error = self._validate_request(request)
        if error:
            return error

        return await self._invoke(request)

    async def on_send_task_subscribe(
        self, request: SendTaskStreamingRequest
    ) -> AsyncIterable[SendTaskStreamingResponse]:
        error = self._validate_request(request)
        if error:
            # Convert JSONRPCResponse to SendTaskStreamingResponse
            yield SendTaskStreamingResponse(id=request.id, error=error.error)
            return

        # We can't await an async generator directly, so we need to use it as is
        try:
            async for response in self._stream_generator(request):
                yield response
        except Exception as e:
            # If an error occurs, yield an error response
            logger.error(f"Error in on_send_task_subscribe: {str(e)}")
            yield SendTaskStreamingResponse(
                id=request.id,
                error=InternalError(message=f"Error processing streaming request: {str(e)}"),
            )

    async def _update_store(self, task_id: str, status: TaskStatus, artifacts: list[Artifact]) -> Task:
        async with self.lock:
            try:
                task = self.tasks[task_id]
            except KeyError:
                logger.error(f"Task {task_id} not found for updating the task")
                # Create a new task with the provided ID if it doesn't exist
                # This ensures we don't fail when a task is not found
                task = Task(
                    id=task_id,
                    sessionId="recovery-session",  # Use a placeholder session ID
                    messages=[],  # No messages available
                    status=status,  # Use the provided status
                    history=[],  # No history available
                )
                self.tasks[task_id] = task

            task.status = status

            # Store assistant's response in history if we have a message
            if status.message and status.message.role == "agent":
                if task.history is None:
                    task.history = []
                # Convert message to dict if needed
                message_dict = status.message.dict() if hasattr(status.message, "dict") else status.message
                # Ensure role is set to assistant
                message_dict["role"] = "assistant"
                task.history.append(message_dict)

            if artifacts is not None:
                for artifact in artifacts:
                    if artifact.append and len(task.artifacts) > 0:
                        # Append to the last artifact
                        last_artifact = task.artifacts[-1]
                        for part in artifact.parts:
                            last_artifact.parts.append(part)
                    else:
                        # Add as a new artifact
                        task.artifacts.append(artifact)
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
        # Check for both agent_name and agentName in the metadata
        agent_name = metadata.get("agent_name", metadata.get("agentName"))
        return {
            "agent_name": agent_name,
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
            # Get the history from the task
            task = self.tasks.get(task_send_params.id)
            history = task.history if task and task.history else []

            # Always use streaming internally, but handle the response differently based on the streaming parameter
            all_parts = []
            final_metadata = {}

            # Create a streaming generator
            stream_gen = agent.stream(query, task_send_params.sessionId, history=history)

            if streaming:
                # For streaming mode, we'll use the streaming endpoint instead
                # Just create a minimal response to acknowledge the request
                task_state = TaskState.WORKING
                task = await self._update_store(task_send_params.id, TaskStatus(state=task_state), [])
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
                    TaskStatus(
                        state=task_state,
                        message=Message(role="agent", parts=all_parts, metadata=final_metadata),
                    ),
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
