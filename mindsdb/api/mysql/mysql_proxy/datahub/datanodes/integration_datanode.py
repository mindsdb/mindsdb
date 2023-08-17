import numpy as np
from numpy import dtype as np_dtype
import pandas as pd
from pandas.api import types as pd_types
from sqlalchemy.types import (
    Integer, Float, Text
)

from mindsdb_sql.parser.ast import Insert, Identifier, CreateTable, TableColumn, DropTables

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.datahub.classes.tables_row import TablesRow
import mindsdb.utilities.profiler as profiler
from mindsdb.utilities import log

logger = log.getLogger(__name__)

class DBHandlerException(Exception):
    pass


class IntegrationDataNode(DataNode):
    type = 'integration'

    def __init__(self, integration_name, ds_type, integration_controller):
        self.integration_name = integration_name
        self.ds_type = ds_type
        self.integration_controller = integration_controller
        self.integration_handler = self.integration_controller.get_handler(self.integration_name)

    def get_type(self):
        return self.type

    def get_tables(self):
        response = self.integration_handler.get_tables()
        if response.type == RESPONSE_TYPE.TABLE:
            result_dict = response.data_frame.to_dict(orient='records')
            result = []
            for row in result_dict:
                result.append(TablesRow.from_dict(row))
            return result
        else:
            raise Exception(f"Can't get tables: {response.error_message}")

    def has_table(self, tableName):
        return True

    def get_table_columns(self, tableName):
        return []

    def create_table(self, table_name: Identifier, result_set, is_replace=False, is_create=False):
        # is_create - create table
        # is_replace - drop table if exists
        # is_create==False and is_replace==False: just insert

        table_columns_meta = {}
        table_columns = []
        for col in result_set.columns:
            # assume this is pandas type
            column_type = Text
            if isinstance(col.type, np_dtype):
                if pd_types.is_integer_dtype(col.type):
                    column_type = Integer
                elif pd_types.is_numeric_dtype(col.type):
                    column_type = Float

            table_columns.append(
                TableColumn(
                    name=col.alias,
                    type=column_type
                )
            )
            table_columns_meta[col.alias] = column_type

        if is_replace:
            # drop
            drop_ast = DropTables(
                tables=[table_name],
                if_exists=True
            )
            result = self.integration_handler.query(drop_ast)
            if result.type == RESPONSE_TYPE.ERROR:
                raise Exception(result.error_message)
            is_create = True

        if is_create:
            create_table_ast = CreateTable(
                name=table_name,
                columns=table_columns,
                is_replace=True
            )

            result = self.integration_handler.query(create_table_ast)
            if result.type == RESPONSE_TYPE.ERROR:
                raise Exception(result.error_message)

        insert_columns = [Identifier(parts=[x.alias]) for x in result_set.columns]
        formatted_data = []

        for rec in result_set.get_records():
            new_row = []
            for col in result_set.columns:
                value = rec[col.alias]
                column_type = table_columns_meta[col.alias]

                python_type = str
                if column_type == Integer:
                    python_type = int
                elif column_type == Float:
                    python_type = float

                try:
                    value = python_type(value) if value is not None else value
                except Exception:
                    pass
                new_row.append(value)
            formatted_data.append(new_row)

        if len(formatted_data) == 0:
            # not need to insert
            return

        insert_ast = Insert(
            table=table_name,
            columns=insert_columns,
            values=formatted_data
        )

        try:
            result = self.integration_handler.query(insert_ast)
        except Exception as e:
            msg = f'[{self.ds_type}/{self.integration_name}]: {str(e)}'
            raise DBHandlerException(msg) from e

        if result.type == RESPONSE_TYPE.ERROR:
            raise Exception(result.error_message)

    @profiler.profile()
    def query(self, query=None, native_query=None, session=None):
        try:
            if query is not None:
                result = self.integration_handler.query(query)
            else:
                # try to fetch native query
                result = self.integration_handler.native_query(native_query)
        except Exception as e:
            msg = str(e).strip()
            if msg == '':
                msg = e.__class__.__name__
            msg = f'[{self.ds_type}/{self.integration_name}]: {msg}'
            raise DBHandlerException(msg) from e

        if result.type == RESPONSE_TYPE.ERROR:
            raise Exception(f'Error in {self.integration_name}: {result.error_message}')
        if result.type == RESPONSE_TYPE.OK:
            return [], []

        df = result.data_frame
        # region clearing df from NaN values
        # recursion error appears in pandas 1.5.3 https://github.com/pandas-dev/pandas/pull/45749
        if isinstance(df, pd.Series):
            df = df.to_frame()

        try:
            df = df.replace(np.NaN, pd.NA)
        except Exception as e:
            logger.error(f'Issue with clearing DF from NaN values: {e}')

        try:
            df = df.where(pd.notnull(df), None)
        except Exception as e:
            logger.error(f'Issue with clearing DF from NaN values: {e}')
        # endregion

        columns_info = [
            {
                'name': k,
                'type': v
            }
            for k, v in df.dtypes.items()
        ]
        data = df.to_dict(orient='records')
        return data, columns_info
