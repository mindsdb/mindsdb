## MindsDB Agent-to-Agent (A2A) API

The A2A API enables MindsDB agents to communicate with external systems and other agents using a standardized protocol. It allows for both synchronous and streaming responses, making it suitable for a wide range of applications including chatbots, data analysis, and automated workflows.

## Overview

The A2A API runs as an optional subprocess of MindsDB, allowing you to:

- Query MindsDB agents using natural language
- Stream responses in real-time for interactive applications
- Connect MindsDB agents to external systems and other agents
- Process complex queries across multiple data sources

## Prerequisites

- MindsDB running
- Python 3.10 or higher

## Running A2A API

The A2A API can be enabled when starting MindsDB by including it in the API list:

```bash
python -m mindsdb --api=mysql,mcp,http,a2a
```

## Configuration

You can configure the A2A API using a config.json file. If not provided, default values will be used:

```json
{
  "a2a": {
    "host": "0.0.0.0",
    "port": 47338,
    "mindsdb_host": "localhost",
    "mindsdb_port": 47334,
    "project_name": "mindsdb",
    "log_level": "info"
  }
}
```

## Example Request

Here's an example of how to make a streaming request to the A2A API:

```bash
curl -X POST \
  "http://localhost:10002/a2a" \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -H "Cache-Control: no-cache" \
  -H "Connection: keep-alive" \
  -d '{
    "jsonrpc": "2.0",
    "id": "your-request-id",
    "method": "tasks/sendSubscribe",
    "params": {
      "id": "your-task-id",
      "sessionId": "your-session-id",
      "message": {
        "role": "user",
        "parts": [
          {"type": "text", "text": "What is the average rental price for a three bedroom?"}
        ],
        "metadata": {
          "agentName": "my_agent_123"
        }
      },
      "acceptedOutputModes": ["text/plain"]
    }
  }' \
  --no-buffer
```

**Note:** You must pass the agent name in metadata using either `agentName` or `agent_name` parameter.

## Example Queries

You can ask questions like:

- "Show me sales data from our CRM and combine it with customer feedback from our support tickets"
- "What are the top performing products across all our e-commerce platforms?"
- "Compare customer engagement metrics between our web analytics and email marketing platforms"

The agent will handle the complexity of joining and analyzing data across different sources and stream the responses back to you in real-time.
