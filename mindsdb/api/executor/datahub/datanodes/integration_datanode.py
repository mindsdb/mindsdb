import time
import inspect
from typing import Optional

import numpy as np
from numpy import dtype as np_dtype
import pandas as pd
from pandas.api import types as pd_types
from sqlalchemy.types import (
    Integer, Float, Text
)

from mindsdb_sql_parser.ast import Insert, Identifier, CreateTable, TableColumn, DropTables

from mindsdb.api.executor.datahub.datanodes.datanode import DataNode
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.api.executor.datahub.classes.tables_row import TablesRow
from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.integrations.utilities.utils import get_class_name
from mindsdb.metrics import metrics
from mindsdb.utilities import log
from mindsdb.utilities.profiler import profiler

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

    def has_table(self, tableName):
        return True

    def get_table_columns(self, table_name: str, schema_name: Optional[str] = None):
        if 'schema_name' in inspect.signature(self.integration_handler.get_columns).parameters:
            response = self.integration_handler.get_columns(table_name, schema_name)
        else:
            response = self.integration_handler.get_columns(table_name)
        if response.type == RESPONSE_TYPE.TABLE:
            df = response.data_frame
            # case independent
            columns = [str(c).lower() for c in df.columns]

            col_name = None
            # looking for specific column names
            for col in ('field', 'column_name', 'column', 'name'):
                if col in columns:
                    col_name = columns.index(col)
                    break
            # if not found - pick first one
            if col_name is None:
                col_name = 0

            names = df[df.columns[col_name]]

            # type
            if 'type' in columns:
                types = df[df.columns[columns.index('type')]]
            else:
                types = [None] * len(columns)

            ret = []
            for i, name in enumerate(names):
                ret.append({
                    'name': name,
                    'type': types[i]
                })

            return ret

        return []

    def drop_table(self, name: Identifier, if_exists=False):
        drop_ast = DropTables(
            tables=[name],
            if_exists=if_exists
        )
        result = self._query(drop_ast)
        if result.type == RESPONSE_TYPE.ERROR:
            raise Exception(result.error_message)

    def create_table(self, table_name: Identifier, result_set: ResultSet = None, columns=None,
                     is_replace=False, is_create=False):
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
            return

        # native insert
        if hasattr(self.integration_handler, 'insert'):
            df = result_set.to_df()

            self.integration_handler.insert(table_name.parts[-1], df)
            return

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
            return

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

    def _query(self, query):
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

    def _native_query(self, native_query):
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
    def query(self, query=None, native_query=None, session=None):
        try:
            if query is not None:
                result = self._query(query)
            else:
                # try to fetch native query
                result = self._native_query(native_query)
        except Exception as e:
            msg = str(e).strip()
            if msg == '':
                msg = e.__class__.__name__
            msg = f'[{self.ds_type}/{self.integration_name}]: {msg}'
            raise DBHandlerException(msg) from e

        if result.type == RESPONSE_TYPE.ERROR:
            raise Exception(f'Error in {self.integration_name}: {result.error_message}')
        if result.type == RESPONSE_TYPE.OK:
            return pd.DataFrame(), []

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

        return df, columns_info
