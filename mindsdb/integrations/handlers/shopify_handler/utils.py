import json
import inspect
from enum import Enum
from dataclasses import dataclass

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


def get_graphql_columns(root: Enum, targets: list[str] | None = None) -> str:
    acc = []
    if targets:
        targets = [name.lower() for name in targets]
    for field in root:
        if targets and field.name.lower() not in targets:
            continue
        if isinstance(field.value, Nodes):
            sub_fields = get_graphql_columns(field.value.enum)
            acc.append(f'{field.name}(first: {MAX_PAGE_LIMIT}) {{ nodes {{{sub_fields}}} {PAGE_INFO} }}')
        elif isinstance(field.value, Extract):
            acc.append(f'{field.name}:{field.value.obj} {{ {field.value.key} }}')
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


@dataclass(slots=True, kw_only=True)
class ShopifyQuery:
    operation_name: str
    columns: str
    limit: int | None = None
    cursor: str | None = None
    sort_key: str | None = None
    reverse: bool = False
    query: str | None = None

    def to_string(self) -> str:
        items = [f"first: {self.limit or MAX_PAGE_LIMIT}"]
        if self.cursor:
            items.append(f'after: "{self.cursor}"')
        if self.sort_key:
            items.append(f'sortKey: {self.sort_key}, reverse: {"true" if self.reverse else "false"}')
        if self.query:
            items.append(f'query: "{self.query}"')
        return f"{{ {self.operation_name} ({','.join(items)}) {{ nodes {{ {self.columns} }} {PAGE_INFO} }} }}"

    def execute(self) -> list[dict]:
        result = shopify.GraphQL().execute(self.to_string())
        return json.loads(result)



def query_graphql_nodes(
        root_name: str, root_class: type, columns: str, cursor: str | None = None,
        limit: int | None = None, sort_key: str | None = None, sort_reverse: bool = False, query: str | None = None,
        depth: int = 1
    ):
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
        if 'errors' in result:
            raise Exception(_format_error(result['errors']))
        hasNextPage = result['data'][root_name]['pageInfo']['hasNextPage']
        cursor = result['data'][root_name]['pageInfo']['endCursor']
        cursor = f', after: "{cursor}"'
        result_data += result['data'][root_name]['nodes']

    fetched_fields = []
    if len(result_data) > 0:
        fetched_fields = [name.lower() for name in result_data[0].keys()]

    nodes = [field for field in root_class if isinstance(field.value, Nodes) if field.name.lower() in fetched_fields]
    extracts = [field for field in root_class if isinstance(field.value, Extract) if field.name.lower() in fetched_fields]

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
            row[extract.name] = (row[extract.name] or {}).get(extract.value.key)

    if limit:
        result_data = result_data[:limit]

    return result_data
