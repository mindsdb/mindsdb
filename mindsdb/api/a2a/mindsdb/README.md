## MindsDB Agent

This sample uses the Agent Development Kit (ADK) to create a MindsDB agent that can query and analyze data across hundreds of federated data sources including databases, data lakes, and SaaS applications.

The agent takes natural language queries from users and translates them into appropriate SQL queries for MindsDB, handling data federation across multiple sources. It can:

- Query data from various sources including databases, data lakes, and SaaS applications
- Perform analytics across federated data sources
- Handle natural language questions about your data
- Return structured results from multiple data sources

## Prerequisites

- Python 3.9 or higher
- [UV](https://docs.astral.sh/uv/)
- MindsDB account and API credentials
- Access to an LLM and API Key

## Running the Sample

1. Navigate to the samples directory:
    ```bash
    cd samples/python/agents/mindsdb
    ```

2. Create an environment file with your API keys:
   ```bash
   echo "GOOGLE_API_KEY=your_api_key_here" > .env
   ```

3. Run the agent:
    ```bash
    uv run .
    ```

4. In a separate terminal, run the A2A client:
    ```bash
    # Connect to the agent (specify the agent URL with correct port)
    cd samples/python/hosts/cli
    uv run . --agent http://localhost:10002

    # If you changed the port when starting the agent, use that port instead
    # uv run . --agent http://localhost:YOUR_PORT
    ```

## Example Queries

You can ask questions like:

- "Show me sales data from our CRM and combine it with customer feedback from our support tickets"
- "What are the top performing products across all our e-commerce platforms?"
- "Compare customer engagement metrics between our web analytics and email marketing platforms"

The agent will handle the complexity of joining and analyzing data across different sources.
