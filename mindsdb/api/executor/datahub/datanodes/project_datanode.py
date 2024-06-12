from copy import deepcopy

import pandas as pd
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import (
    BinaryOperation,
    Identifier,
    Constant,
    Update,
    Select,
    Delete,
)

from mindsdb.utilities.exception import EntityNotExistsError
from mindsdb.api.executor.datahub.datanodes.datanode import DataNode
from mindsdb.api.executor.datahub.classes.tables_row import TablesRow
from mindsdb.api.executor import SQLQuery
from mindsdb.api.executor.utilities.sql import query_df
from mindsdb.interfaces.query_context.context_controller import query_context_controller


class ProjectDataNode(DataNode):
    type = 'project'

    def __init__(self, project, integration_controller, information_schema):
        self.project = project
        self.integration_controller = integration_controller
        self.information_schema = information_schema

    def get_type(self):
        return self.type

    def get_tables(self):
        tables = self.project.get_tables()
        table_types = {
            'table': 'BASE TABLE',
            'model': 'MODEL',
            'view': 'VIEW'
        }
        tables = [
            {
                'TABLE_NAME': key,
                'TABLE_TYPE': table_types.get(val['type'])
            }
            for key, val in tables.items()
        ]
        result = [TablesRow.from_dict(row) for row in tables]
        return result

    def has_table(self, table_name):
        tables = self.project.get_tables()
        return table_name in tables

    def get_table_columns(self, table_name):
        return self.project.get_columns(table_name)

    def predict(self, model_name: str, df, version=None, params=None):
        model_metadata = self.project.get_model(model_name)
        if model_metadata is None:
            raise Exception(f"Can't find model '{model_name}'")
        model_metadata = model_metadata['metadata']
        if model_metadata['update_status'] == 'available':
            raise Exception(f"model '{model_name}' is obsolete and needs to be updated. Run 'RETRAIN {model_name};'")
        ml_handler = self.integration_controller.get_ml_handler(model_metadata['engine_name'])
        return ml_handler.predict(model_name, df, project_name=self.project.name, version=version, params=params)

    def query(self, query=None, native_query=None, session=None):
        if query is None and native_query is not None:
            query = parse_sql(native_query, dialect='mindsdb')

        if isinstance(query, Update):
            query_table = query.table.parts[0].lower()
            kb_table = session.kb_controller.get_table(query_table, self.project.id)
            if kb_table:
                # this is the knowledge db
                kb_table.update_query(query)
                return pd.DataFrame(), []

            raise NotImplementedError(f"Can't update object: {query_table}")

        elif isinstance(query, Delete):
            query_table = query.table.parts[0].lower()
            kb_table = session.kb_controller.get_table(query_table, self.project.id)
            if kb_table:
                # this is the knowledge db
                kb_table.delete_query(query)
                return pd.DataFrame(), []

            raise NotImplementedError(f"Can't delete object: {query_table}")

        elif isinstance(query, Select):
            # region is it query to 'models'?
            query_table = query.from_table.parts[0].lower()
            if query_table in ('models', 'jobs', 'mdb_triggers', 'chatbots', 'skills', 'agents'):
                new_query = deepcopy(query)
                project_filter = BinaryOperation('=', args=[
                    Identifier('project'),
                    Constant(self.project.name)
                ])
                if new_query.where is None:
                    new_query.where = project_filter
                else:
                    new_query.where = BinaryOperation('and', args=[
                        new_query.where,
                        project_filter
                    ])
                df, columns_info = self.information_schema.query(new_query)
                return df, columns_info
            # endregion

            # other table from project

            if self.project.get_view(query_table):
                # this is the view

                view_meta = self.project.query_view(query)

                query_context_controller.set_context('view', view_meta['id'])

                try:
                    sqlquery = SQLQuery(
                        view_meta['query_ast'],
                        session=session
                    )
                    result = sqlquery.fetch(view='dataframe')

                finally:
                    query_context_controller.release_context('view', view_meta['id'])

                if result['success'] is False:
                    raise Exception(f"Cant execute view query: {view_meta['query_ast']}")
                df = result['result']

                df = query_df(df, query, session=session)

                columns_info = [
                    {
                        'name': k,
                        'type': v
                    }
                    for k, v in df.dtypes.items()
                ]

                return df, columns_info

            kb_table = session.kb_controller.get_table(query_table, self.project.id)
            if kb_table:
                # this is the knowledge db
                df = kb_table.select_query(query)
                columns_info = [
                    {
                        'name': k,
                        'type': v
                    }
                    for k, v in df.dtypes.items()
                ]

                return df, columns_info

            raise EntityNotExistsError(f"Can't select from {query_table} in project")
        else:
            raise NotImplementedError(f"Query not supported {query}")

    def create_table(self, table_name: Identifier, result_set=None, is_replace=False, **kwargs):
        # is_create - create table
        # is_replace - drop table if exists
        # is_create==False and is_replace==False: just insert

        from mindsdb.api.executor.controllers.session_controller import SessionController
        session = SessionController()

        table_name = table_name.parts[-1]
        kb_table = session.kb_controller.get_table(table_name, self.project.id)
        if kb_table:
            # this is the knowledge db
            if is_replace:
                kb_table.clear()

            df = result_set.to_df()
            return kb_table.insert(df)
        raise NotImplementedError(f"Cant create table {table_name}")
