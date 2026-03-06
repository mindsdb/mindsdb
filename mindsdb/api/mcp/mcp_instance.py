from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    name="MindsDB",
    instructions=(
        "MindsDB is a data platform that connects to external databases and data sources.\n"
        "Use the available resources to discover connected databases and their schema,\n"
        "then use the `query` tool to retrieve or manipulate data with SQL.\n"
        "\n"
        "Workflow:\n"
        "1. Read `schema://databases` to list available data sources.\n"
        "2. Read `schema://databases/{name}/tables` to explore tables in a source.\n"
        "3. Read `schema://databases/{name}/tables/{table}/columns` to inspect columns.\n"
        "4. Use the `query` tool to run SQL queries against the data."
    ),
    dependencies=["mindsdb"],
    streamable_http_path="/streamable",
)
