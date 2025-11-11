import json
import inspect
from enum import Enum

import shopify

from mindsdb.utilities import log

from .models.utils import Nodes, Extract

logger = log.getLogger(__name__)

MAX_PAGE_LIMIT = 250
PAGE_INFO = "pageInfo { hasNextPage endCursor }"


def _format_error(errors_list: list[dict]) -> str:
    errors_text = [record.get('message', 'undescribed') for record in errors_list]
    if len(errors_list) == 0:
        errors_text = errors_text[0]
        return f"An error occurred when executing the query: {errors_text}"
    errors_text = '\n'.join(errors_text)
    return f"Error occurred when executing the query:\n{errors_text}"


def get_graphql_columns(root: Enum) -> str:
    acc = []
    for field in root:
        if isinstance(field.value, Nodes):
            sub_fields = get_graphql_columns(field.value.enum)
            acc.append(f'{field.name}(first: {MAX_PAGE_LIMIT}) {{ nodes {{{sub_fields}}} {PAGE_INFO} }}')
        elif isinstance(field.value, Extract):
            acc.append(f'{field.value.obj} {{ {field.value.key} }}')
        elif inspect.isclass(field.value) and issubclass(field.value, Enum):
            sub_fields = get_graphql_columns(field.value)
            acc.append(f'{field.name} {{{sub_fields}}}')
        else:
            acc.append(field.value)
    return ' '.join(acc)


def query_graphql(query: str) -> dict:
    result = shopify.GraphQL().execute(query)
    result = json.loads(result)
    if 'errors' in result:
        raise Exception(_format_error(result['errors']))
    return result


def query_graphql_nodes(
        root_name: str, root_class: type, columns: str, cursor: str | None = None,
        limit: int | None = None, sort: dict | None = None, query: str | None = None,
        depth: int = 1
    ):
    result_data = []
    hasNextPage = True
    cursor = f', after: "{cursor}"' if cursor else ""
    sort = f', sortKey: {sort["field"]}, reverse: {"true" if sort["reverse"] else "false"}' if sort else ""
    query = f', query: "{query}"' if query else ""
    while hasNextPage:
        result = shopify.GraphQL().execute(
            f'{{ {root_name}(first: {MAX_PAGE_LIMIT} {cursor} {sort} {query}) {{ nodes {{ {columns} }} {PAGE_INFO} }} }}'
        )
        result = json.loads(result)
        if 'errors' in result:
            raise Exception(_format_error(result['errors']))
        hasNextPage = result['data'][root_name]['pageInfo']['hasNextPage']
        cursor = result['data'][root_name]['pageInfo']['endCursor']
        cursor = f', after: "{cursor}"'
        result_data += result['data'][root_name]['nodes']

    nodes = [field for field in root_class if isinstance(field.value, Nodes)]
    extracts = [field for field in root_class if isinstance(field.value, Extract)]
    for row in result_data:
        for node in nodes:
            node_data = row[node.name]['nodes']
            hasNextPage = row[node.name]['pageInfo']['hasNextPage']
            if depth > 0 and hasNextPage:
                cursor = row[node.name]['pageInfo']['endCursor']
                result = query_graphql_nodes(root_name=node.name, cursor=cursor, root_class=node.value.enum, columns=get_graphql_columns(node.value.enum), depth=depth - 1)
                node_data += result
            row[node.name] = node_data
        for extract in extracts:
            row[extract.name] = row[extract.value.obj][extract.value.key]
    return result_data
