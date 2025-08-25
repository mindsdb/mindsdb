import json
from typing import Any, AsyncIterable, Dict, List
import requests
import logging
import httpx
from mindsdb.api.a2a.utils import to_serializable, convert_a2a_message_to_qa_format
from mindsdb.api.a2a.constants import DEFAULT_STREAM_TIMEOUT

logger = logging.getLogger(__name__)


class MindsDBAgent:
    """An agent that communicates with MindsDB over HTTP following the A2A protocol."""

    SUPPORTED_CONTENT_TYPES = ["text", "text/plain", "application/json"]

    def __init__(
        self,
        agent_name="my_agent",
        project_name="mindsdb",
        host="localhost",
        port=47334,
    ):
        self.agent_name = agent_name
        self.project_name = project_name
        self.host = host
        self.port = port
        self.base_url = f"http://{host}:{port}"
        self.agent_url = f"{self.base_url}/api/projects/{project_name}/agents/{agent_name}"
        self.sql_url = f"{self.base_url}/api/sql/query"
        logger.info(f"Initialized MindsDB agent connector to {self.base_url}")

    def invoke(self, query, session_id) -> Dict[str, Any]:
        """Send a query to the MindsDB agent using SQL API."""
        try:
            escaped_query = query.replace("'", "''")
            sql_query = f"SELECT * FROM {self.project_name}.{self.agent_name} WHERE question = '{escaped_query}'"
            logger.info(f"Sending SQL query to MindsDB: {sql_query[:100]}...")
            response = requests.post(self.sql_url, json={"query": sql_query})
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
            error_msg = f"Error connecting to MindsDB: {str(e)}"
            logger.error(error_msg)
            return {
                "content": error_msg,
                "parts": [{"type": "text", "text": error_msg}],
            }
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            logger.error(error_msg)
            return {
                "content": error_msg,
                "parts": [{"type": "text", "text": error_msg}],
            }

    async def streaming_invoke(self, messages, timeout=DEFAULT_STREAM_TIMEOUT):
        url = f"{self.base_url}/api/projects/{self.project_name}/agents/{self.agent_name}/completions/stream"
        logger.info(f"Sending streaming request to MindsDB agent: {self.agent_name}")
        async with httpx.AsyncClient(timeout=timeout) as client:
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
                            logger.error(f"Failed to parse SSE JSON payload: {e}; line: {payload}")
                    # Ignore comments or control lines
                # Signal the end of the stream
                yield {"is_task_complete": True}

    async def stream(
        self,
        query: str,
        session_id: str,
        history: List[dict] | None = None,
        timeout: int = DEFAULT_STREAM_TIMEOUT,
    ) -> AsyncIterable[Dict[str, Any]]:
        """Stream responses from the MindsDB agent (uses streaming API endpoint)."""
        try:
            logger.info(f"Using streaming API for query: {query[:100]}...")
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
            logger.error(f"Error in streaming: {str(e)}")
            yield {
                "is_task_complete": True,
                "parts": [
                    {
                        "type": "text",
                        "text": f"Error: {str(e)}",
                    }
                ],
                "metadata": {
                    "type": "reasoning",
                    "subtype": "error",
                },
            }
