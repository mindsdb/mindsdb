import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions



class StoriesTable(APITable):

    def __init__(self, handler):
        super().__init__(handler)

        self.name = 'stories'
        self.primary_key = 'id'

    def get(self, select=None, where=None, group_by=None, having=None, order_by=None, limit=None):

        if 'id' not in select:
            select.append('id')

        if not where:
            where = []

        if group_by or having or order_by:
            raise NotImplementedError('This method does not support group_by, having, or order_by arguments')

        query_string = f'get_top_stories({where})'
        response = self.handler.native_query(query_string)

        data_frame = response.data_frame

        data_frame = data_frame[select]

        if limit:
            data_frame = data_frame.head(limit)

        return data_frame

class CommentsTable(APITable):

    def __init__(self, handler):
        super().__init__(handler)

        self.name = 'comments'
        self.primary_key = 'id'

    def get(self, select=None, where=None, group_by=None, having=None, order_by=None, limit=None):

        if 'id' not in select:
            select.append('id')

        item_id = None
        for condition in where:
            if condition[0] == 'item_id':
                item_id = condition[2]
                break

        if item_id is None:
            raise ValueError("An 'item_id' must be provided in the 'where' condition")

        if group_by or having or order_by:
            raise NotImplementedError('This method does not support group_by, having, or order_by arguments')

        query_string = f'get_comments(item_id={item_id})'
        response = self.handler.native_query(query_string)

        data_frame = response.data_frame

        data_frame = data_frame[select]

        if limit:
            data_frame = data_frame.head(limit)

        return data_frame
