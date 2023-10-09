import re
import pandas as pd

from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities.log import get_log
from mindsdb.integrations.libs.response import HandlerResponse as Response


logger = get_log("integrations.notion_handler")


class NotionDatabaseTable(APITable):

    def select(self, query: ast.Select) -> Response:
        conditions = extract_comparison_conditions(query.where)

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError(f'OR is not supported')

            if arg1 == 'database_id':
                if op == '=':
                    params[arg1] = arg2
                else:
                    NotImplementedError(f'Unknown op: {op}')

            else:
                filters.append([op, arg1, arg2])

        result = self.handler.call_notion_api(
            method_name='databases.query',
            params=params,
            filters=filters
        )

        # filter targets
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = []
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(columns) == 0:
            columns = self.get_columns()

        # columns to lower case
        columns = [name.lower() for name in columns]

        if len(result) == 0:
            result = pd.DataFrame([], columns=columns)
        else:
            # add absent columns
            for col in set(columns) & set(result.columns) ^ set(columns):
                result[col] = None

            # filter by columns
            result = result[columns]
        return result

    def get_columns(self):
        return [
            'id',
            'created_time',
            'last_edited_time',
            'created_by',
            'last_edited_by',
            'cover',
            'icon',
            'parent',
            'archived',
            'properties',
            'url',
            'public_url',
        ]

    def insert(self, query: ast.Insert):
        pass


class NotionPagesTable(APITable):

    def select(self, query: ast.Select) -> Response:
        conditions = extract_comparison_conditions(query.where)

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError(f'OR is not supported')

            if arg1 == 'page_id':
                if op == '=':
                    params[arg1] = arg2
                else:
                    raise NotImplementedError

            else:
                filters.append([op, arg1, arg2])

        if 'query' not in params:
            # search not works without query, use 'mindsdb'
            params['query'] = 'mindsdb'

        result = self.handler.call_notion_api(
            method_name='pages.retrieve',
            params=params,
            filters=filters
        )

        # filter targets
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = []
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(columns) == 0:
            columns = self.get_columns()

        # columns to lower case
        columns = [name.lower() for name in columns]

        if len(result) == 0:
            result = pd.DataFrame([], columns=columns)
        else:
            # add absent columns
            for col in set(columns) & set(result.columns) ^ set(columns):
                result[col] = None

            # filter by columns
            result = result[columns]
        return result

    def get_columns(self):
        return [
            'id',
            'object',
            'created_time',
            'last_edited_time',
            'created_by',
            'last_edited_by',
            'cover',
            'icon',
            'parent',
            'archived',
            'properties',
            'url',
            'public_url',
        ]

    def insert(self, query: ast.Insert):
        columns = [col.name for col in query.columns]

        insert_params = ('notion_api_token',)
        for p in insert_params:
            if p not in self.handler.connection_args:
                raise Exception(f'To insert data into Notion, you need to provide the following parameters when connecting it to MindsDB: {insert_params}')  # noqa

        for row in query.values:
            params = dict(zip(columns, row))

            words = re.split('( )', text)

            messages = []

            text2 = ''
            for word in words:
                if len(text2) + len(word) > max_text_len - 3 - 7:
                    messages.append(text2.strip())

                    text2 = ''
                text2 += word

            # the last message
            if text2.strip() != '':
                messages.append(text2.strip())

            len_messages = len(messages)
            for i, text in enumerate(messages):
                if i < len_messages - 1:
                    text += '...'
                else:
                    text += ' '

                text += f'({i + 1}/{len_messages})'

                params['text'] = text
                ret = self.handler.call_notion_api('pages.create', params)
                inserted_id = ret.id[0]


class NotionBlocksTable(APITable):

    def select(self, query: ast.Select) -> Response:
        conditions = extract_comparison_conditions(query.where)

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError(f'OR is not supported')

            if arg1 == 'block_id':
                if op == '=':
                    params[arg1] = arg2
                else:
                    NotImplementedError(f'Unknown op: {op}')

            else:
                filters.append([op, arg1, arg2])

        result = self.handler.call_notion_api(
            method_name='blocks.retrieve',
            params=params,
            filters=filters
        )

        # filter targets
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = []
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(columns) == 0:
            columns = self.get_columns()

        # columns to lower case
        columns = [name.lower() for name in columns]

        if len(result) == 0:
            result = pd.DataFrame([], columns=columns)
        else:
            # add absent columns
            for col in set(columns) & set(result.columns) ^ set(columns):
                result[col] = None

            # filter by columns
            result = result[columns]
        return result

    def get_columns(self):
        return [
            'object',
            'id',
            'parent',
            'has_children',
            'created_time',
            'last_edited_time',
            'created_by',
            'last_edited_by',
            'archived',
            'type',
            'bookmark',
            'breadcrumb',
            'bulleted_list_item',
            'callout',
            'child_database',
            'child_page',
            'column',
            'column_list',
            'divider',
            'embed',
            'equation',
            'file',
            'heading_1',
            'heading_2',
            'heading_3',
            'image',
            'link_preview',
            'link_to_page',
            'numbered_list_item',
            'paragraph',
            'pdf',
            'quote',
            'synced_block',
            'table',
            'table_of_contents',
            'table_row',
            'template',
            'to_do',
            'toggle',
            'unsupported',
            'video',
        ]

    def insert(self, query: ast.Insert):
        pass


class NotionCommentsTable(APITable):

    def select(self, query: ast.Select) -> Response:
        conditions = extract_comparison_conditions(query.where)

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError(f'OR is not supported')

            if arg1 == 'block_id':
                if op == '=':
                    params[arg1] = arg2
                else:
                    NotImplementedError(f'Unknown op: {op}')

            else:
                filters.append([op, arg1, arg2])

        result = self.handler.call_notion_api(
            method_name='comments.list',
            params=params,
            filters=filters
        )

        # filter targets
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = []
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(columns) == 0:
            columns = self.get_columns()

        # columns to lower case
        columns = [name.lower() for name in columns]

        if len(result) == 0:
            result = pd.DataFrame([], columns=columns)
        else:
            # add absent columns
            for col in set(columns) & set(result.columns) ^ set(columns):
                result[col] = None

            # filter by columns
            result = result[columns]
        return result

    def get_columns(self):
        return [
            'id',
            'object',
            'parent',
            'discussion_id',
            'created_time',
            'last_edited_time',
            'created_by',
            'rich_text',
        ]

    def insert(self, query: ast.Insert):
        pass
