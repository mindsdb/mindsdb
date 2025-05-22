## MindsDB Agent

This sample uses the Agent Development Kit (ADK) to create a MindsDB agent that can query and analyze data across hundreds of federated data sources including databases, data lakes, and SaaS applications.

The agent takes natural language queries from users and translates them into appropriate SQL queries for MindsDB, handling data federation across multiple sources. It can:

- Query data from various sources including databases, data lakes, and SaaS applications
- Perform analytics across federated data sources
- Handle natural language questions about your data
- Return structured results from multiple data sources

## Prerequisites

- Python 3.10 or higher
- [UV](https://docs.astral.sh/uv/)
- Access to an LLM and API Key

## Optional Dependencies

The A2A API requires several optional dependencies that are not included in the main MindsDB installation. To install these dependencies, run:

```bash
pip install httpx>=0.27.0 httpx-sse>=0.4.0 starlette>=0.36.3 sse-starlette>=2.0.0 PyJWT>=2.8.0 jwcrypto>=1.5.4 python-dotenv>=1.0.1
```

These dependencies are optional since the A2A functionality is not mandatory for using MindsDB.

## Running the Sample

1. Ensure you are in the a2a directory:
    ```bash
    cd mindsdb/api/a2a
    ```

2. Run the agent:
    ```bash
    uv run .
    ```

## Example Queries

You can ask questions like:

- "Show me sales data from our CRM and combine it with customer feedback from our support tickets"
- "What are the top performing products across all our e-commerce platforms?"
- "Compare customer engagement metrics between our web analytics and email marketing platforms"

The agent will handle the complexity of joining and analyzing data across different sources.
