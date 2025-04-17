import time
import inspect
from dataclasses import astuple

import numpy as np
from numpy import dtype as np_dtype
import pandas as pd
from pandas.api import types as pd_types
from sqlalchemy.types import (
    Integer, Float, Text
)

from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser.ast import Insert, Identifier, CreateTable, TableColumn, DropTables

from mindsdb.api.executor.datahub.classes.response import DataHubResponse
from mindsdb.api.executor.datahub.datanodes.datanode import DataNode
from mindsdb.api.executor.datahub.classes.tables_row import TablesRow
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.integrations.libs.response import HandlerResponse, INF_SCHEMA_COLUMNS_NAMES
from mindsdb.integrations.utilities.utils import get_class_name
from mindsdb.metrics import metrics
from mindsdb.utilities import log
from mindsdb.utilities.profiler import profiler
from mindsdb.api.executor.datahub.datanodes.system_tables import infer_mysql_type

logger = log.getLogger(__name__)


class DBHandlerException(Exception):
    pass


class IntegrationDataNode(DataNode):
    type = 'integration'

    def __init__(self, integration_name, ds_type, integration_controller):
        self.integration_name = integration_name
        self.ds_type = ds_type
        self.integration_controller = integration_controller
        self.integration_handler = self.integration_controller.get_data_handler(self.integration_name)

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

    def get_table_columns_df(self, table_name: str, schema_name: str | None = None) -> pd.DataFrame:
        """Get a DataFrame containing representation of information_schema.columns for the specified table.

        Args:
            table_name (str): The name of the table to get columns from.
            schema_name (str | None): The name of the schema to get columns from.

        Returns:
            pd.DataFrame: A DataFrame containing representation of information_schema.columns for the specified table.
                          The DataFrame has list of columns as in the integrations.libs.response.INF_SCHEMA_COLUMNS_NAMES.
        """
        if 'schema_name' in inspect.signature(self.integration_handler.get_columns).parameters:
            response = self.integration_handler.get_columns(table_name, schema_name)
        else:
            response = self.integration_handler.get_columns(table_name)

        if response.type == RESPONSE_TYPE.COLUMNS_TABLE:
            return response.data_frame

        if response.type != RESPONSE_TYPE.TABLE:
            logger.warning(f"Wrong response type for handler's `get_columns` call: {response.type}")
            return pd.DataFrame([], columns=astuple(INF_SCHEMA_COLUMNS_NAMES))

        # region fallback for old handlers
        df = response.data_frame
        df.columns = [name.upper() for name in df.columns]
        if 'FIELD' not in df.columns or 'TYPE' not in df.columns:
            logger.warning(
                f"Response from the handler's `get_columns` call does not contain required columns: f{df.columns}"
            )
            return pd.DataFrame([], columns=astuple(INF_SCHEMA_COLUMNS_NAMES))

        new_df = df[['FIELD', 'TYPE']]
        new_df.columns = ['COLUMN_NAME', 'DATA_TYPE']

        new_df[INF_SCHEMA_COLUMNS_NAMES.MYSQL_DATA_TYPE] = new_df[
            INF_SCHEMA_COLUMNS_NAMES.DATA_TYPE
        ].apply(lambda x: infer_mysql_type(x).value)

        for column_name in astuple(INF_SCHEMA_COLUMNS_NAMES):
            if column_name in new_df.columns:
                continue
            new_df[column_name] = None
        # endregion

        return new_df

    def get_table_columns_names(self, table_name: str, schema_name: str | None = None) -> list[str]:
        """Get a list of column names for the specified table.

        Args:
            table_name (str): The name of the table to get columns from.
            schema_name (str | None): The name of the schema to get columns from.

        Returns:
            list[str]: A list of column names for the specified table.
        """
        df = self.get_table_columns_df(table_name, schema_name)
        return df[INF_SCHEMA_COLUMNS_NAMES.COLUMN_NAME].to_list()

    def drop_table(self, name: Identifier, if_exists=False):
        drop_ast = DropTables(
            tables=[name],
            if_exists=if_exists
        )
        result = self._query(drop_ast)
        if result.type == RESPONSE_TYPE.ERROR:
            raise Exception(result.error_message)

    def create_table(self, table_name: Identifier, result_set: ResultSet = None, columns=None,
                     is_replace=False, is_create=False) -> DataHubResponse:
        # is_create - create table
        # is_replace - drop table if exists
        # is_create==False and is_replace==False: just insert

        table_columns_meta = {}

        if columns is None:
            columns = []

            df = result_set.get_raw_df()

            for idx, col in enumerate(result_set.columns):
                dtype = col.type
                # assume this is pandas type
                column_type = Text
                if isinstance(dtype, np_dtype):
                    if pd_types.is_object_dtype(dtype):
                        # try to infer
                        dtype = df[idx].infer_objects().dtype

                    if pd_types.is_integer_dtype(dtype):
                        column_type = Integer
                    elif pd_types.is_numeric_dtype(dtype):
                        column_type = Float

                columns.append(
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
            result = self._query(drop_ast)
            if result.type == RESPONSE_TYPE.ERROR:
                raise Exception(result.error_message)
            is_create = True

        if is_create:
            create_table_ast = CreateTable(
                name=table_name,
                columns=columns,
                is_replace=is_replace
            )
            result = self._query(create_table_ast)
            if result.type == RESPONSE_TYPE.ERROR:
                raise Exception(result.error_message)

        if result_set is None:
            # it is just a 'create table'
            return DataHubResponse()

        # native insert
        if hasattr(self.integration_handler, 'insert'):
            df = result_set.to_df()

            result: HandlerResponse = self.integration_handler.insert(table_name.parts[-1], df)
            return DataHubResponse(affected_rows=result.affected_rows)

        insert_columns = [Identifier(parts=[x.alias]) for x in result_set.columns]

        # adapt table types
        for col_idx, col in enumerate(result_set.columns):
            column_type = table_columns_meta[col.alias]

            if column_type == Integer:
                type_name = 'int'
            elif column_type == Float:
                type_name = 'float'
            else:
                continue

            try:
                result_set.set_col_type(col_idx, type_name)
            except Exception:
                pass

        values = result_set.to_lists()

        if len(values) == 0:
            # not need to insert
            return DataHubResponse()

        insert_ast = Insert(
            table=table_name,
            columns=insert_columns,
            values=values,
            is_plain=True
        )

        try:
            result = self._query(insert_ast)
        except Exception as e:
            msg = f'[{self.ds_type}/{self.integration_name}]: {str(e)}'
            raise DBHandlerException(msg) from e

        if result.type == RESPONSE_TYPE.ERROR:
            raise Exception(result.error_message)

        return DataHubResponse(affected_rows=result.affected_rows)

    def _query(self, query) -> HandlerResponse:
        time_before_query = time.perf_counter()
        result = self.integration_handler.query(query)
        elapsed_seconds = time.perf_counter() - time_before_query
        query_time_with_labels = metrics.INTEGRATION_HANDLER_QUERY_TIME.labels(
            get_class_name(self.integration_handler), result.type)
        query_time_with_labels.observe(elapsed_seconds)

        num_rows = 0
        if result.data_frame is not None:
            num_rows = len(result.data_frame.index)
        response_size_with_labels = metrics.INTEGRATION_HANDLER_RESPONSE_SIZE.labels(
            get_class_name(self.integration_handler), result.type)
        response_size_with_labels.observe(num_rows)
        return result

    def _native_query(self, native_query) -> HandlerResponse:
        time_before_query = time.perf_counter()
        result = self.integration_handler.native_query(native_query)
        elapsed_seconds = time.perf_counter() - time_before_query
        query_time_with_labels = metrics.INTEGRATION_HANDLER_QUERY_TIME.labels(
            get_class_name(self.integration_handler), result.type)
        query_time_with_labels.observe(elapsed_seconds)

        num_rows = 0
        if result.data_frame is not None:
            num_rows = len(result.data_frame.index)
        response_size_with_labels = metrics.INTEGRATION_HANDLER_RESPONSE_SIZE.labels(
            get_class_name(self.integration_handler), result.type)
        response_size_with_labels.observe(num_rows)
        return result

    @profiler.profile()
    def query(self, query: ASTNode | None = None, native_query: str | None = None, session=None) -> DataHubResponse:
        try:
            if query is not None:
                result: HandlerResponse = self._query(query)
            else:
                # try to fetch native query
                result: HandlerResponse = self._native_query(native_query)
        except Exception as e:
            msg = str(e).strip()
            if msg == '':
                msg = e.__class__.__name__
            msg = f'[{self.ds_type}/{self.integration_name}]: {msg}'
            raise DBHandlerException(msg) from e

        if result.type == RESPONSE_TYPE.ERROR:
            raise Exception(f'Error in {self.integration_name}: {result.error_message}')
        if result.type == RESPONSE_TYPE.OK:
            return DataHubResponse(affected_rows=result.affected_rows)

        df = result.data_frame
        # region clearing df from NaN values
        # recursion error appears in pandas 1.5.3 https://github.com/pandas-dev/pandas/pull/45749
        if isinstance(df, pd.Series):
            df = df.to_frame()

        try:
            # replace python's Nan, np.NaN, np.nan and pd.NA to None
            df.replace([np.NaN, pd.NA], None, inplace=True)
        except Exception as e:
            logger.error(f"Issue with clearing DF from NaN values: {e}")
        # endregion

        columns_info = [
            {
                'name': k,
                'type': v
            }
            for k, v in df.dtypes.items()
        ]

        return DataHubResponse(
            data_frame=df,
            columns=columns_info,
            affected_rows=result.affected_rows
        )
