import json
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
from agent import ReimbursementAgent
import common.server.utils as utils
from typing import Union
import logging
logger = logging.getLogger(__name__)

class AgentTaskManager(InMemoryTaskManager):

    def __init__(self, agent: ReimbursementAgent):
        super().__init__()
        self.agent = agent

    async def _stream_generator(
        self, request: SendTaskStreamingRequest
    ) -> AsyncIterable[SendTaskStreamingResponse] | JSONRPCResponse:
        task_send_params: TaskSendParams = request.params
        query = self._get_user_query(task_send_params)
        try:
          async for item in self.agent.stream(query, task_send_params.sessionId):
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
                yield SendTaskStreamingResponse(id=request.id, result=task_update_event)
            else:
                #task_state = TaskState.INPUT_REQUIRED
                task_state = TaskState.COMPLETED
                artifact = Artifact(parts=parts, index=0, append=False)
                task_status = TaskStatus(state=task_state)
                yield SendTaskStreamingResponse(
                  id=request.id,
                  result=TaskArtifactUpdateEvent(
                      id=task_send_params.id,
                      artifact=artifact,
                  )
                )
                await self._update_store(task_send_params.id, task_status, [artifact])
                yield SendTaskStreamingResponse(
                    id=request.id,
                    result=TaskStatusUpdateEvent(
                        id=task_send_params.id,
                        status=TaskStatus(
                            state=task_status.state,
                        ),
                        final=True
                    )
                )
           
        except Exception as e:
            logger.error(f"An error occurred while streaming the response: {e}")
            yield JSONRPCResponse(
                id=request.id,
                error=InternalError(
                    message="An error occurred while streaming the response"
                ),
            )
    def _validate_request(
        self, request: Union[SendTaskRequest, SendTaskStreamingRequest]
    ) -> None:
        task_send_params: TaskSendParams = request.params
        if not utils.are_modalities_compatible(
            task_send_params.acceptedOutputModes, ReimbursementAgent.SUPPORTED_CONTENT_TYPES
        ):
            logger.warning(
                "Unsupported output mode. Received %s, Support %s",
                task_send_params.acceptedOutputModes,
                ReimbursementAgent.SUPPORTED_CONTENT_TYPES,
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
            #if status.message is not None:
            #    self.task_messages[task_id].append(status.message)
            if artifacts is not None:
                if task.artifacts is None:
                    task.artifacts = []
                task.artifacts.extend(artifacts)
            return task
    async def _invoke(self, request: SendTaskRequest) -> SendTaskResponse:
        task_send_params: TaskSendParams = request.params
        query = self._get_user_query(task_send_params)
        try:
            result = self.agent.invoke(query, task_send_params.sessionId)
        except Exception as e:
            logger.error(f"Error invoking agent: {e}")
            raise ValueError(f"Error invoking agent: {e}")
        parts = [{"type": "text", "text": result}]
        task_state = TaskState.INPUT_REQUIRED if "MISSING_INFO:" in result else TaskState.COMPLETED
        task = await self._update_store(
            task_send_params.id,
            TaskStatus(
                state=task_state, message=Message(role="agent", parts=parts)
            ),
            [Artifact(parts=parts)],
        )
        return SendTaskResponse(id=request.id, result=task)
    
    def _get_user_query(self, task_send_params: TaskSendParams) -> str:
        part = task_send_params.message.parts[0]
        if not isinstance(part, TextPart):
            raise ValueError("Only text parts are supported")
        return part.text

