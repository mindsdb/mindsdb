from mcp.types import TextContent

from mindsdb.api.mcp.mcp_instance import mcp


@mcp.prompt(name="sample_table", description="Fetch 5 sample rows from a table and describe its structure.")
def sample_table(database_name: str, table_name: str) -> list[TextContent]:
    return [
        TextContent(
            type="text",
            text=(
                f"Use the `query` tool to fetch 5 sample rows from the table `{table_name}` "
                f"in database `{database_name}`:\n\n"
                f"```sql\n"
                f"SELECT * FROM `{database_name}`.`{table_name}` LIMIT 5;\n"
                f"```\n\n"
                f"After getting the results, briefly describe the table structure "
                f"and what kind of data it contains."
            ),
        )
    ]
