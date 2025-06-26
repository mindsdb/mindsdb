import json
from typing import Any, AsyncIterable, Dict, List, Iterator
import requests
import logging

from mindsdb.api.a2a.constants import DEFAULT_STREAM_TIMEOUT

logger = logging.getLogger(__name__)


class MindsDBAgent:
    """An agent that communicates with MindsDB over HTTP following the A2A protocol."""

    # Supported content-types according to A2A spec. We include both the
    # mime-type form and the simple "text" token so that clients using either
    # convention succeed.
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
            # Escape single quotes in the query for SQL
            escaped_query = query.replace("'", "''")

            # Build the SQL query to the agent
            sql_query = f"SELECT * FROM {self.project_name}.{self.agent_name} WHERE question = '{escaped_query}'"

            # Log request for debugging
            logger.info(f"Sending SQL query to MindsDB: {sql_query[:100]}...")

            # Send the request to MindsDB SQL API
            response = requests.post(self.sql_url, json={"query": sql_query})
            response.raise_for_status()

            # Process the response
            data = response.json()

            # Log the response for debugging
            logger.debug(f"Received response from MindsDB: {json.dumps(data)[:200]}...")

            if "data" in data and len(data["data"]) > 0:
                # The result should be in the first row
                result_row = data["data"][0]

                # Find the response column (might be 'response', 'answer', 'result', etc.)
                # Try common column names or just return all content
                for column in ["response", "result", "answer", "completion", "output"]:
                    if column in result_row:
                        content = result_row[column]
                        logger.info(f"Found result in column '{column}': {content[:100]}...")
                        return {
                            "content": content,
                            "parts": [{"type": "text", "text": content}],
                        }

                # If no specific column found, return the whole row as JSON
                logger.info("No specific result column found, returning full row")
                content = json.dumps(result_row, indent=2)

                # Return structured data only if it is a dictionary (A2A `data` part
                # must itself be a JSON object).  In some cases MindsDB may return a
                # list (for instance a list of rows or records).  If that happens we
                # downgrade it to plain-text to avoid schema-validation errors on the
                # A2A side.

                parts: List[dict] = [{"type": "text", "text": content}]

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

    def streaming_invoke(self, messages: List[dict], timeout: int = DEFAULT_STREAM_TIMEOUT) -> Iterator[Dict[str, Any]]:
        """Stream responses from the MindsDB agent using the direct API endpoint.

        Args:
            messages: List of message dictionaries, each containing 'question' and optionally 'answer'.
                Example: [{'question': 'what is the average rental price for a three bedroom?', 'answer': None}]
            timeout: Request timeout in seconds (default: 300)

        Returns:
            Iterator yielding chunks of the streaming response.
        """
        try:
            # Construct the URL for the streaming completions endpoint
            url = f"{self.base_url}/api/projects/{self.project_name}/agents/{self.agent_name}/completions/stream"

            # Log request for debugging
            logger.info(f"Sending streaming request to MindsDB agent: {self.agent_name}")
            logger.debug(f"Request messages: {json.dumps(messages)[:200]}...")

            # Send the request to MindsDB streaming API with timeout
            stream = requests.post(url, json={"messages": messages}, stream=True, timeout=timeout)
            stream.raise_for_status()

            # Process the streaming response directly
            for line in stream.iter_lines():
                if line:
                    # Parse each non-empty line
                    try:
                        line = line.decode("utf-8")
                        if line.startswith("data: "):
                            # Extract the JSON data from the line that starts with 'data: '
                            data = line[6:]  # Remove 'data: ' prefix
                            try:
                                chunk = json.loads(data)
                                # Pass through the chunk with minimal modifications
                                yield chunk
                            except json.JSONDecodeError as e:
                                logger.warning(f"Failed to parse JSON from line: {data}. Error: {str(e)}")
                                # Yield error information but continue processing
                                yield {
                                    "error": f"JSON parse error: {str(e)}",
                                    "data": data,
                                    "is_task_complete": False,
                                    "parts": [
                                        {
                                            "type": "text",
                                            "text": f"Error parsing response: {str(e)}",
                                        }
                                    ],
                                    "metadata": {},
                                }
                        else:
                            # Log other lines for debugging
                            logger.debug(f"Received non-data line: {line}")

                            # If it looks like a raw text response (not SSE format), wrap it
                            if not line.startswith("event:") and not line.startswith(":"):
                                yield {"content": line, "is_task_complete": False}
                    except UnicodeDecodeError as e:
                        logger.warning(f"Failed to decode line: {str(e)}")
                        # Continue processing despite decode errors

        except requests.exceptions.Timeout as e:
            error_msg = f"Request timed out after {timeout} seconds: {str(e)}"
            logger.error(error_msg)
            yield {
                "content": error_msg,
                "parts": [{"type": "text", "text": error_msg}],
                "is_task_complete": True,
                "error": "timeout",
                "metadata": {"error": True},
            }

        except requests.exceptions.ChunkedEncodingError as e:
            error_msg = f"Stream was interrupted: {str(e)}"
            logger.error(error_msg)
            yield {
                "content": error_msg,
                "parts": [{"type": "text", "text": error_msg}],
                "is_task_complete": True,
                "error": "stream_interrupted",
                "metadata": {"error": True},
            }

        except requests.exceptions.ConnectionError as e:
            error_msg = f"Connection error: {str(e)}"
            logger.error(error_msg)
            yield {
                "content": error_msg,
                "parts": [{"type": "text", "text": error_msg}],
                "is_task_complete": True,
                "error": "connection_error",
                "metadata": {"error": True},
            }

        except requests.exceptions.RequestException as e:
            error_msg = f"Error connecting to MindsDB streaming API: {str(e)}"
            logger.error(error_msg)
            yield {
                "content": error_msg,
                "parts": [{"type": "text", "text": error_msg}],
                "is_task_complete": True,
                "error": "request_error",
                "metadata": {"error": True},
            }

        except Exception as e:
            error_msg = f"Error in streaming: {str(e)}"
            logger.error(error_msg)
            yield {
                "content": error_msg,
                "parts": [{"type": "text", "text": error_msg}],
                "is_task_complete": True,
                "error": "unknown_error",
                "metadata": {"error": True},
            }

        # Send a final completion message
        yield {"is_task_complete": True, "metadata": {"complete": True}}

    async def stream(
        self,
        query: str,
        session_id: str,
        history: List[dict] | None = None,
    ) -> AsyncIterable[Dict[str, Any]]:
        """Stream responses from the MindsDB agent (uses streaming API endpoint).

        Args:
            query: The current query to send to the agent.
            session_id: Unique identifier for the conversation session.
            history: Optional list of previous messages in the conversation.

        Returns:
            AsyncIterable yielding chunks of the streaming response.
        """
        try:
            logger.info(f"Using streaming API for query: {query[:100]}...")

            # Format history into the expected format
            formatted_messages = []
            if history:
                for msg in history:
                    # Convert Message object to dict if needed
                    msg_dict = msg.dict() if hasattr(msg, "dict") else msg
                    role = msg_dict.get("role", "user")

                    # Extract text from parts
                    text = ""
                    for part in msg_dict.get("parts", []):
                        if part.get("type") == "text":
                            text = part.get("text", "")
                            break

                    if text:
                        if role == "user":
                            formatted_messages.append({"question": text, "answer": None})
                        elif role == "assistant" and formatted_messages:
                            # Add the answer to the last question
                            formatted_messages[-1]["answer"] = text

            # Add the current query to the messages
            formatted_messages.append({"question": query, "answer": None})

            logger.debug(f"Formatted messages for agent: {formatted_messages}")

            # Use the streaming_invoke method to get real streaming responses
            streaming_response = self.streaming_invoke(formatted_messages)

            # Yield all chunks directly from the streaming response
            for chunk in streaming_response:
                # Only add required fields if they don't exist
                # This preserves the original structure as much as possible
                if "is_task_complete" not in chunk:
                    chunk["is_task_complete"] = False

                if "metadata" not in chunk:
                    chunk["metadata"] = {}

                # Ensure parts exist, but try to preserve original content
                if "parts" not in chunk:
                    # If content exists, create a part from it
                    if "content" in chunk:
                        chunk["parts"] = [{"type": "text", "text": chunk["content"]}]
                    # If output exists, create a part from it
                    elif "output" in chunk:
                        chunk["parts"] = [{"type": "text", "text": chunk["output"]}]
                    # If actions exist, create empty parts
                    elif "actions" in chunk or "steps" in chunk or "messages" in chunk:
                        # These chunks have their own format, just add empty parts
                        chunk["parts"] = []
                    else:
                        # Skip chunks with no content
                        continue

                yield chunk

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
