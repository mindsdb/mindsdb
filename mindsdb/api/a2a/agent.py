import json
from typing import Any, AsyncIterable, Dict, List
import requests
import httpx
from mindsdb.api.a2a.utils import to_serializable, convert_a2a_message_to_qa_format
from mindsdb.api.a2a.constants import DEFAULT_STREAM_TIMEOUT
from mindsdb.api.a2a.common.types import A2AClientError, A2AClientHTTPError
from mindsdb.utilities import log
from mindsdb.utilities.config import config

logger = log.getLogger(__name__)


class MindsDBAgent:
    """An agent that communicates with MindsDB over HTTP following the A2A protocol."""

    SUPPORTED_CONTENT_TYPES = ["text", "text/plain", "application/json"]

    def __init__(
        self,
        agent_name="my_agent",
        project_name="mindsdb",
        user_info: Dict[str, Any] = None,
    ):
        self.agent_name = agent_name
        self.project_name = project_name
        port = config.get("api", {}).get("http", {}).get("port", 47334)
        host = config.get("api", {}).get("http", {}).get("host", "127.0.0.1")

        # Use 127.0.0.1 instead of localhost for better compatibility
        if host in ("0.0.0.0", ""):
            url = f"http://127.0.0.1:{port}/"
        else:
            url = f"http://{host}:{port}/"

        self.base_url = url
        self.agent_url = f"{self.base_url}/api/projects/{project_name}/agents/{agent_name}"
        self.sql_url = f"{self.base_url}/api/sql/query"
        self.headers = {k: v for k, v in user_info.items() if v is not None} or {}
        logger.info(f"Initialized MindsDB agent connector to {self.base_url}")

    def invoke(self, query, session_id) -> Dict[str, Any]:
        """Send a query to the MindsDB agent using SQL API."""
        try:
            escaped_query = query.replace("'", "''")
            sql_query = f"SELECT * FROM {self.project_name}.{self.agent_name} WHERE question = '{escaped_query}'"
            logger.debug(f"Sending SQL query to MindsDB: {sql_query[:100]}...")
            response = requests.post(self.sql_url, json={"query": sql_query}, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            logger.debug(f"Received response from MindsDB: {json.dumps(data)[:200]}...")
            if "data" in data and len(data["data"]) > 0:
                result_row = data["data"][0]
                for column in ["response", "result", "answer", "completion", "output"]:
                    if column in result_row:
                        content = result_row[column]
                        logger.info(f"Found result in column '{column}': {content[:100]}...")
                        return {
                            "content": content,
                            "parts": [{"type": "text", "text": content}],
                        }
                logger.info("No specific result column found, returning full row")
                content = json.dumps(result_row, indent=2)
                parts = [{"type": "text", "text": content}]
                if isinstance(result_row, dict):
                    parts.append(
                        {
                            "type": "data",
                            "data": result_row,
                            "metadata": {"subtype": "json"},
                        }
                    )
                return {
                    "content": content,
                    "parts": parts,
                }
            else:
                error_msg = "Error: No data returned from MindsDB"
                logger.error(error_msg)
                return {
                    "content": error_msg,
                    "parts": [{"type": "text", "text": error_msg}],
                }
        except requests.exceptions.RequestException as e:
            logger.exception("Error connecting to MindsDB:")
            return {
                "content": f"Error connecting to MindsDB: {e}",
                "parts": [{"type": "text", "text": error_msg}],
            }
        except Exception as e:
            logger.exception("Error: ")
            return {
                "content": f"Error: {e}",
                "parts": [{"type": "text", "text": error_msg}],
            }

    async def streaming_invoke(self, messages, timeout=DEFAULT_STREAM_TIMEOUT):
        url = f"{self.base_url}/api/projects/{self.project_name}/agents/{self.agent_name}/completions/stream"
        logger.debug(f"Sending streaming request to MindsDB agent: {self.agent_name}")
        try:
            async with httpx.AsyncClient(timeout=timeout, headers=self.headers) as client:
                async with client.stream("POST", url, json={"messages": to_serializable(messages)}) as response:
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        if not line.strip():
                            continue
                        # Only process actual SSE data lines
                        if line.startswith("data:"):
                            payload = line[len("data:") :].strip()
                            try:
                                yield json.loads(payload)
                            except Exception as e:
                                logger.exception(f"Failed to parse SSE JSON payload: {e}; line: {payload}")
                        # Ignore comments or control lines
                    # Signal the end of the stream
                    yield {"is_task_complete": True}
        except httpx.ReadTimeout:
            error_msg = f"Request timed out after {timeout} seconds while streaming from agent '{self.agent_name}'"
            logger.error(error_msg)
            raise TimeoutError(error_msg)
        except httpx.ConnectTimeout:
            error_msg = f"Connection timeout while connecting to agent '{self.agent_name}' at {url}"
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        except httpx.ConnectError as e:
            error_msg = f"Failed to connect to agent '{self.agent_name}' at {url}: {str(e)}"
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP error {e.response.status_code} from agent '{self.agent_name}': {str(e)}"
            logger.error(error_msg)
            raise A2AClientHTTPError(status_code=e.response.status_code, message=error_msg)
        except httpx.RequestError as e:
            error_msg = f"Request error while streaming from agent '{self.agent_name}': {str(e)}"
            logger.error(error_msg)
            raise A2AClientError(error_msg)

    async def stream(
        self,
        query: str,
        session_id: str,
        history: List[dict] | None = None,
        timeout: int = DEFAULT_STREAM_TIMEOUT,
    ) -> AsyncIterable[Dict[str, Any]]:
        """Stream responses from the MindsDB agent (uses streaming API endpoint)."""
        try:
            # Create A2A message structure with history and current query
            a2a_message = {"role": "user", "parts": [{"text": query}]}
            if history:
                a2a_message["history"] = history
            # Convert to Q&A format using centralized utility
            formatted_messages = convert_a2a_message_to_qa_format(a2a_message)
            logger.debug(f"Formatted messages for agent: {formatted_messages}")
            streaming_response = self.streaming_invoke(formatted_messages, timeout=timeout)
            async for chunk in streaming_response:
                content_value = chunk.get("text") or chunk.get("output") or json.dumps(chunk)
                wrapped_chunk = {"is_task_complete": False, "content": content_value, "metadata": {}}
                yield wrapped_chunk
        except Exception as e:
            logger.exception(f"Error in streaming: {e}")
            yield {
                "is_task_complete": True,
                "parts": [
                    {
                        "type": "text",
                        "text": f"Error: {e}",
                    }
                ],
                "metadata": {
                    "type": "reasoning",
                    "subtype": "error",
                },
            }
