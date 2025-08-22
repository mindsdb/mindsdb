from copy import deepcopy
from dataclasses import astuple

import pandas as pd
from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import (
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
from mindsdb.api.executor.datahub.classes.response import DataHubResponse
from mindsdb.utilities.partitioning import process_dataframe_in_partitions
from mindsdb.integrations.libs.response import INF_SCHEMA_COLUMNS_NAMES


class ProjectDataNode(DataNode):
    type = "project"

    def __init__(self, project, integration_controller, information_schema):
        self.project = project
        self.integration_controller = integration_controller
        self.information_schema = information_schema

    def get_type(self):
        return self.type

    def get_tables(self):
        tables = self.project.get_tables()
        table_types = {
            "table": "BASE TABLE",
            "model": "MODEL",
            "view": "VIEW",
            "agent": "AGENT",
            "knowledge_base": "KNOWLEDGE BASE",
        }
        tables = [{"TABLE_NAME": key, "TABLE_TYPE": table_types.get(val["type"])} for key, val in tables.items()]
        result = [TablesRow.from_dict(row) for row in tables]
        return result

    def get_table_columns_df(self, table_name: str, schema_name: str | None = None) -> pd.DataFrame:
        """Get a DataFrame containing representation of information_schema.columns for the specified table.

        Args:
            table_name (str): The name of the table to get columns from.
            schema_name (str | None): Not in use. The name of the schema to get columns from.

        Returns:
            pd.DataFrame: A DataFrame containing representation of information_schema.columns for the specified table.
                          The DataFrame has list of columns as in the integrations.libs.response.INF_SCHEMA_COLUMNS_NAMES
                          but only 'COLUMN_NAME' column is filled with the actual column names.
                          Other columns are filled with None.
        """
        columns = self.project.get_columns(table_name)

        data = []
        row = {name: None for name in astuple(INF_SCHEMA_COLUMNS_NAMES)}
        for column_name in columns:
            r = row.copy()
            r[INF_SCHEMA_COLUMNS_NAMES.COLUMN_NAME] = column_name
            data.append(r)

        return pd.DataFrame(data, columns=astuple(INF_SCHEMA_COLUMNS_NAMES))

    def get_table_columns_names(self, table_name: str, schema_name: str | None = None) -> list[str]:
        """Get a list of column names for the specified table.

        Args:
            table_name (str): The name of the table to get columns from.
            schema_name (str | None): Not in use. The name of the schema to get columns from.

        Returns:
            list[str]: A list of column names for the specified table.
        """
        return self.project.get_columns(table_name)

    def predict(self, model_name: str, df, version=None, params=None):
        model_metadata = self.project.get_model(model_name)
        if model_metadata is None:
            raise Exception(f"Can't find model '{model_name}'")
        model_metadata = model_metadata["metadata"]
        if model_metadata["update_status"] == "available":
            raise Exception(f"model '{model_name}' is obsolete and needs to be updated. Run 'RETRAIN {model_name};'")
        ml_handler = self.integration_controller.get_ml_handler(model_metadata["engine_name"])
        if params is not None and "partition_size" in params:

            def callback(chunk):
                return ml_handler.predict(
                    model_name, chunk, project_name=self.project.name, version=version, params=params
                )

            return pd.concat(process_dataframe_in_partitions(df, callback, params["partition_size"]))

        return ml_handler.predict(model_name, df, project_name=self.project.name, version=version, params=params)

    def query(self, query=None, native_query=None, session=None) -> DataHubResponse:
        if query is None and native_query is not None:
            query = parse_sql(native_query)

        if isinstance(query, Update):
            query_table = query.table.parts[0].lower()
            kb_table = session.kb_controller.get_table(query_table, self.project.id)
            if kb_table:
                # this is the knowledge db
                kb_table.update_query(query)
                return DataHubResponse()

            raise NotImplementedError(f"Can't update object: {query_table}")

        elif isinstance(query, Delete):
            query_table = query.table.parts[0].lower()
            kb_table = session.kb_controller.get_table(query_table, self.project.id)
            if kb_table:
                # this is the knowledge db
                kb_table.delete_query(query)
                return DataHubResponse()

            raise NotImplementedError(f"Can't delete object: {query_table}")

        elif isinstance(query, Select):
            match query.from_table.parts, query.from_table.is_quoted:
                case [query_table], [is_quoted]:
                    ...
                case [query_table, int(_)], [is_quoted, _]:
                    ...
                case [query_table, str(version)], [is_quoted, _] if version.isdigit():
                    ...
                case _:
                    raise ValueError("Table name should contain only one part")

            if not is_quoted:
                query_table = query_table.lower()

            # region is it query to 'models'?
            if query_table in ("models", "jobs", "mdb_triggers", "chatbots", "skills", "agents"):
                new_query = deepcopy(query)
                project_filter = BinaryOperation("=", args=[Identifier("project"), Constant(self.project.name)])
                if new_query.where is None:
                    new_query.where = project_filter
                else:
                    new_query.where = BinaryOperation("and", args=[new_query.where, project_filter])
                return self.information_schema.query(new_query)
            # endregion

            # other table from project
            if self.project.get_view(query_table, strict_case=is_quoted):
                # this is the view
                df = self.project.query_view(query, session)

                columns_info = [{"name": k, "type": v} for k, v in df.dtypes.items()]

                return DataHubResponse(data_frame=df, columns=columns_info)

            kb_table = session.kb_controller.get_table(query_table, self.project.id)
            if kb_table:
                # this is the knowledge db
                df = kb_table.select_query(query)
                columns_info = [{"name": k, "type": v} for k, v in df.dtypes.items()]

                return DataHubResponse(data_frame=df, columns=columns_info)

            raise EntityNotExistsError(f"Table '{query_table}' not found in database", self.project.name)
        else:
            raise NotImplementedError(f"Query not supported {query}")

    def create_table(
        self, table_name: Identifier, result_set=None, is_replace=False, params=None, **kwargs
    ) -> DataHubResponse:
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
            kb_table.insert(df, params=params)
            return DataHubResponse()
        raise NotImplementedError(f"Can't create table {table_name}")
