from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from mindsdb.api.mcp.oauth import build_oauth_components
from mindsdb.utilities.config import config


def _create_mcp() -> FastMCP:
    token_verifier, auth_settings = build_oauth_components()

    dns_rebinding_protection = config["api"]["mcp"]["dns_rebinding_protection"]
    transport_security = TransportSecuritySettings(enable_dns_rebinding_protection=dns_rebinding_protection)

    return FastMCP(
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
        debug=False,
        token_verifier=token_verifier,
        auth=auth_settings,
        transport_security=transport_security,
    )


mcp = _create_mcp()
