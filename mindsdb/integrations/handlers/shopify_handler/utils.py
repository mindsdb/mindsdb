import json
import inspect
from enum import Enum
from dataclasses import dataclass

import shopify

from mindsdb.utilities import log

from .models.utils import Nodes, Extract
from .models.common import AliasesEnum

logger = log.getLogger(__name__)

MAX_PAGE_LIMIT = 250
PAGE_INFO = "pageInfo { hasNextPage endCursor }"


def _format_error(errors_list: list[dict]) -> str:
    """Format shopify's GraphQL error list into a single string.

    Args:
        errors_list: The list of errors.

    Returns:
        str: The formatted error string.
    """
    errors_text = [record.get("message", "undescribed") for record in errors_list]
    if len(errors_list) == 0:
        errors_text = errors_text[0]
        return f"An error occurred when executing the query: {errors_text}"
    errors_text = "\n".join(errors_text)
    return f"Error occurred when executing the query:\n{errors_text}"


def get_graphql_columns(root: AliasesEnum, targets: list[str] | None = None) -> str:
    """Get the GraphQL columns for a given object.

    Args:
        root: The object to get the GraphQL columns for.
        targets: The list of columns to include in the query.

    Returns:
        str: The GraphQL columns string.
    """
    acc = []
    if targets:
        targets = [name.lower() for name in targets]
    for name, value in root.aliases():
        if targets and name.lower() not in targets:
            continue
        if isinstance(value, Nodes):
            sub_fields = get_graphql_columns(value.enum)
            acc.append(f"{name}(first: {MAX_PAGE_LIMIT}) {{ nodes {{{sub_fields}}} {PAGE_INFO} }}")
        elif isinstance(value, Extract):
            acc.append(f"{name}:{value.obj} {{ {value.key} }}")
        elif inspect.isclass(value) and issubclass(value, Enum):
            sub_fields = get_graphql_columns(value)
            acc.append(f"{name} {{{sub_fields}}}")
        else:
            acc.append(value)
    return " ".join(acc)


@dataclass(slots=True, kw_only=True)
class ShopifyQuery:
    """A class to represent a Shopify GraphQL query.

    Args:
        operation_name: The name of the operation to execute.
        columns: The columns to include in the query.
        limit: The limit of the query.
        cursor: The cursor to use for pagination.
        sort_key: The key to use for sorting.
        reverse: Whether to reverse the sort.
        query: The query to execute.
    """

    operation_name: str
    columns: str
    limit: int | None = None
    cursor: str | None = None
    sort_key: str | None = None
    reverse: bool = False
    query: str | None = None

    def to_string(self) -> str:
        """Convert the query to a string.

        Returns:
            str: The string representation of the query.
        """
        items = [f"first: {self.limit or MAX_PAGE_LIMIT}"]
        if self.cursor:
            items.append(f'after: "{self.cursor}"')
        if self.sort_key:
            items.append(f"sortKey: {self.sort_key}, reverse: {'true' if self.reverse else 'false'}")
        if self.query:
            items.append(f'query: "{self.query}"')
        return f"{{ {self.operation_name} ({', '.join(items)}) {{ nodes {{ {self.columns} }} {PAGE_INFO} }} }}"

    def execute(self) -> list[dict]:
        """Execute the query.

        Returns:
            list[dict]: The result of the query.
        """
        result = shopify.GraphQL().execute(self.to_string())
        return json.loads(result)


def query_graphql_nodes(
    root_name: str,
    root_class: type,
    columns: str,
    cursor: str | None = None,
    limit: int | None = None,
    sort_key: str | None = None,
    sort_reverse: bool = False,
    query: str | None = None,
    depth: int = 1,
):
    """Query the GraphQL API for nodes.

    Args:
        root_name: The name of the root object.
        root_class: The root object.
        columns: The columns to include in the query.
        cursor: The cursor to use for pagination.
        limit: The limit of the query.
        sort_key: The key to use for sorting.
        sort_reverse: Whether to reverse the sort.
        query: The query to execute.
        depth: The depth of the nodes to fetch. Default is 1: fetch the first level of nested nodes.

    Returns:
        list[dict]: The list of nodes.
    """
    result_data = []
    hasNextPage = True
    while hasNextPage:
        result = ShopifyQuery(
            operation_name=root_name,
            columns=columns,
            limit=max(MAX_PAGE_LIMIT if limit is None else limit - len(result_data), 0),
            cursor=cursor,
            sort_key=sort_key,
            reverse=sort_reverse,
            query=query,
        ).execute()
        if "errors" in result:
            raise Exception(_format_error(result["errors"]))
        hasNextPage = result["data"][root_name]["pageInfo"]["hasNextPage"]
        cursor = result["data"][root_name]["pageInfo"]["endCursor"]
        result_data += result["data"][root_name]["nodes"]

    fetched_fields = []
    if len(result_data) > 0:
        fetched_fields = [name.lower() for name in result_data[0].keys()]

    nodes_name = [
        name for name, value in root_class.aliases() if isinstance(value, Nodes) if name.lower() in fetched_fields
    ]
    extracts_names = [
        name for name, value in root_class.aliases() if isinstance(value, Extract) if name.lower() in fetched_fields
    ]

    for row in result_data:
        for name in nodes_name:
            value = root_class[name].value
            node_data = row[name]["nodes"]
            hasNextPage = row[name]["pageInfo"]["hasNextPage"]
            if depth > 0 and hasNextPage:
                cursor = row[name]["pageInfo"]["endCursor"]
                result = query_graphql_nodes(
                    root_name=name,
                    cursor=cursor,
                    root_class=value.enum,
                    columns=get_graphql_columns(value.enum),
                    depth=depth - 1,
                )
                node_data += result
            row[name] = node_data
        for name in extracts_names:
            value = root_class[name].value
            row[name] = (row[name] or {}).get(value.key)

    if limit:
        result_data = result_data[:limit]

    return result_data
