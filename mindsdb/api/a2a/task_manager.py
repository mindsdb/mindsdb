from typing import AsyncIterable
from common.types import (
    SendTaskRequest,
    TaskSendParams,
    Message,
    TaskStatus,
    Artifact,
    TaskStatusUpdateEvent,
    TaskArtifactUpdateEvent,
    TextPart,
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
        agent_name = task_send_params.message.metadata.get('agent_name', 'my_agent')
        agent = self._create_agent(agent_name)
        
        try:
            async for item in agent.stream(query, task_send_params.sessionId):
                is_task_complete = item["is_task_complete"]
                parts = item["parts"]

                if not is_task_complete:
                    task_state = TaskState.WORKING
                    metadata = item["metadata"]
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
            yield JSONRPCResponse(
                id=request.id,
                error=InternalError(
                    message=f"An error occurred while streaming the response: {str(e)}"
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

    async def _invoke(self, request: SendTaskRequest) -> SendTaskResponse:
        task_send_params: TaskSendParams = request.params
        query = self._get_user_query(task_send_params)
        agent_name = task_send_params.message.metadata.get('agent_name', 'my_agent')
        agent = self._create_agent(agent_name)
        
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
        except Exception as e:
            logger.error(f"Error invoking agent: {e}")
            result_text = f"Error invoking agent: {e}"
            parts = [{"type": "text", "text": result_text}]

        task_state = TaskState.COMPLETED
        task = await self._update_store(
            task_send_params.id,
            TaskStatus(state=task_state, message=Message(role="agent", parts=parts)),
            [Artifact(parts=parts)],
        )
        return SendTaskResponse(id=request.id, result=task)

    def _get_user_query(self, task_send_params: TaskSendParams) -> str:
        part = task_send_params.message.parts[0]
        if not isinstance(part, TextPart):
            raise ValueError("Only text parts are supported")
        return part.text
