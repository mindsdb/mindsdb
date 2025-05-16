import json
import asyncio
from typing import Any, AsyncIterable, Dict, List
import requests
import logging

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
        self.agent_url = (
            f"{self.base_url}/api/projects/{project_name}/agents/{agent_name}"
        )
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
                        logger.info(
                            f"Found result in column '{column}': {content[:100]}..."
                        )
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

    async def stream(self, query, session_id) -> AsyncIterable[Dict[str, Any]]:
        """Stream responses from the MindsDB agent (uses non-streaming as fallback)."""
        try:
            logger.info(
                "Note: MindsDB SQL API doesn't support streaming. Using regular query..."
            )

            # Use the non-streaming method and simulate streaming response
            result = self.invoke(query, session_id)

            # First yield a "thinking" response
            yield {
                "is_task_complete": False,
                "parts": [
                    {
                        "type": "text",
                        "text": "Thinking...",
                    }
                ],
                "metadata": {
                    "type": "reasoning",
                    "subtype": "plan",
                },
            }

            # Wait a bit to simulate processing
            await asyncio.sleep(1)

            # Then yield the actual response
            content = result.get("content", "")
            # Split content into chunks for more realistic streaming
            chunk_size = 150
            chunks = [
                content[i:i + chunk_size] for i in range(0, len(content), chunk_size)
            ]

            for i, chunk in enumerate(chunks):
                is_last = i == len(chunks) - 1
                yield {
                    "is_task_complete": is_last,
                    "parts": [
                        {
                            "type": "text",
                            "text": chunk,
                        }
                    ],
                    "metadata": {
                        "type": "reasoning",
                        "subtype": "respond",
                    },
                }
                # Small delay between chunks
                if not is_last:
                    await asyncio.sleep(0.3)

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
