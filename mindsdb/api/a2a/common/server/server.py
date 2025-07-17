import json
import time
from typing import AsyncIterable, Any, Dict

from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse
from starlette.requests import Request
from ...common.types import (
    A2ARequest,
    JSONRPCResponse,
    InvalidRequestError,
    JSONParseError,
    GetTaskRequest,
    CancelTaskRequest,
    SendTaskRequest,
    SetTaskPushNotificationRequest,
    GetTaskPushNotificationRequest,
    InternalError,
    AgentCard,
    TaskResubscriptionRequest,
    SendTaskStreamingRequest,
)
from pydantic import ValidationError
from ...common.server.task_manager import TaskManager

from mindsdb.utilities import log
from mindsdb.utilities.log import get_uvicorn_logging_config, get_mindsdb_log_level

logger = log.getLogger(__name__)


class A2AServer:
    def __init__(
        self,
        host="0.0.0.0",
        port=5000,
        endpoint="/",
        agent_card: AgentCard = None,
        task_manager: TaskManager = None,
    ):
        self.host = host
        self.port = port
        self.endpoint = endpoint
        self.task_manager = task_manager
        self.agent_card = agent_card
        self.app = Starlette()
        self.app.add_route(self.endpoint, self._process_request, methods=["POST"])
        self.app.add_route("/.well-known/agent.json", self._get_agent_card, methods=["GET"])
        # Add status endpoint
        self.app.add_route("/status", self._get_status, methods=["GET"])
        # TODO: Remove this when we have a proper CORS policy
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        self.start_time = time.time()

    def start(self):
        if self.agent_card is None:
            raise ValueError("agent_card is not defined")

        if self.task_manager is None:
            raise ValueError("request_handler is not defined")

        import uvicorn

        # Configure uvicorn with optimized settings for streaming
        uvicorn.run(
            self.app,
            host=self.host,
            port=self.port,
            http="h11",
            timeout_keep_alive=65,
            log_level=get_mindsdb_log_level(),
            log_config=get_uvicorn_logging_config("uvicorn_a2a"),
        )

    def _get_agent_card(self, request: Request) -> JSONResponse:
        return JSONResponse(self.agent_card.model_dump(exclude_none=True))

    def _get_status(self, request: Request) -> JSONResponse:
        """
        Status endpoint that returns basic server information.
        This endpoint can be used by the frontend to check if the A2A server is running.
        """
        uptime_seconds = time.time() - self.start_time

        status_info: Dict[str, Any] = {
            "status": "ok",
            "service": "mindsdb-a2a",
            "uptime_seconds": round(uptime_seconds, 2),
            "host": self.host,
            "port": self.port,
            "agent_name": self.agent_card.name if self.agent_card else None,
            "version": self.agent_card.version if self.agent_card else "unknown",
        }

        return JSONResponse(status_info)

    async def _process_request(self, request: Request):
        try:
            body = await request.json()
            json_rpc_request = A2ARequest.validate_python(body)

            if isinstance(json_rpc_request, GetTaskRequest):
                result = await self.task_manager.on_get_task(json_rpc_request)
            elif isinstance(json_rpc_request, SendTaskRequest):
                result = await self.task_manager.on_send_task(json_rpc_request)
            elif isinstance(json_rpc_request, SendTaskStreamingRequest):
                # Don't await the async generator, just pass it to _create_response
                result = self.task_manager.on_send_task_subscribe(json_rpc_request)
            elif isinstance(json_rpc_request, CancelTaskRequest):
                result = await self.task_manager.on_cancel_task(json_rpc_request)
            elif isinstance(json_rpc_request, SetTaskPushNotificationRequest):
                result = await self.task_manager.on_set_task_push_notification(json_rpc_request)
            elif isinstance(json_rpc_request, GetTaskPushNotificationRequest):
                result = await self.task_manager.on_get_task_push_notification(json_rpc_request)
            elif isinstance(json_rpc_request, TaskResubscriptionRequest):
                result = await self.task_manager.on_resubscribe_to_task(json_rpc_request)
            else:
                logger.warning(f"Unexpected request type: {type(json_rpc_request)}")
                raise ValueError(f"Unexpected request type: {type(request)}")

            return self._create_response(result)

        except Exception as e:
            return self._handle_exception(e)

    def _handle_exception(self, e: Exception) -> JSONResponse:
        if isinstance(e, json.decoder.JSONDecodeError):
            json_rpc_error = JSONParseError()
        elif isinstance(e, ValidationError):
            json_rpc_error = InvalidRequestError(data=json.loads(e.json()))
        else:
            logger.error(f"Unhandled exception: {e}")
            json_rpc_error = InternalError()

        response = JSONRPCResponse(id=None, error=json_rpc_error)
        return JSONResponse(response.model_dump(exclude_none=True), status_code=400)

    def _create_response(self, result: Any) -> JSONResponse | EventSourceResponse:
        if isinstance(result, AsyncIterable):
            # Step 2: Yield actual serialized event as JSON, with timing logs
            async def event_generator(result):
                async for item in result:
                    t0 = time.time()
                    logger.debug(f"[A2AServer] STEP2 serializing item at {t0}: {str(item)[:120]}")
                    try:
                        if hasattr(item, "model_dump_json"):
                            data = item.model_dump_json(exclude_none=True)
                        else:
                            data = json.dumps(item)
                    except Exception as e:
                        logger.error(f"Serialization error in SSE stream: {e}")
                        data = json.dumps({"error": f"Serialization error: {str(e)}"})
                    yield {"data": data}

            # Add robust SSE headers for compatibility
            sse_headers = {
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
                "Transfer-Encoding": "chunked",
            }
            return EventSourceResponse(event_generator(result), headers=sse_headers)
        elif isinstance(result, JSONRPCResponse):
            return JSONResponse(result.model_dump(exclude_none=True))
        elif isinstance(result, dict):
            logger.warning("Falling back to JSONResponse for result type: dict")
            return JSONResponse(result)
        else:
            logger.error(f"Unexpected result type: {type(result)}")
            raise ValueError(f"Unexpected result type: {type(result)}")
