from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    name="MindsDB",
    dependencies=["mindsdb"],
    streamable_http_path="/streamable",
)
