from typing import Optional

import pandas as pd
from pandas.api import types as pd_types
import duckdb

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.utilities import log
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


logger = log.getLogger(__name__)


def _pandas_dtype_to_sql_type(dtype) -> str:
    """Convert pandas dtype to SQL data type string.
    
    Args:
        dtype: pandas dtype object
        
    Returns:
        str: SQL data type string (VARCHAR, INTEGER, DECIMAL, etc.)
    """
    # Handle string dtypes
    if pd_types.is_string_dtype(dtype):
        return "VARCHAR"
    
    # Handle integer dtypes
    if pd_types.is_integer_dtype(dtype):
        return "INTEGER"
    
    # Handle float/numeric dtypes
    if pd_types.is_float_dtype(dtype) or pd_types.is_numeric_dtype(dtype):
        return "DECIMAL"
    
    # Handle boolean dtypes
    if pd_types.is_bool_dtype(dtype):
        return "BOOLEAN"
    
    # Handle datetime dtypes
    if pd_types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    
    # Handle date dtypes
    if pd_types.is_date_dtype(dtype):
        return "DATE"
    
    # Default to VARCHAR for object and unknown types
    return "VARCHAR"


def _infer_data_type(value) -> str:
    """Infer SQL data type from Python value.
    
    Args:
        value: Python value to infer type from
        
    Returns:
        str: SQL data type string (VARCHAR, INTEGER, DECIMAL, etc.)
    """
    if value is None:
        return "VARCHAR"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "DECIMAL"
    elif isinstance(value, str):
        # Check if it looks like a timestamp
        if "T" in value and ("Z" in value or "+" in value or "-" in value[-6:]):
            return "TIMESTAMP"
        # Check if it looks like a date
        try:
            pd.to_datetime(value)
            if len(value) == 10:  # Just date, no time
                return "DATE"
            return "DATETIME"
        except (ValueError, TypeError):
            pass
        return "VARCHAR"
    elif pd_types.is_datetime64_any_dtype(type(value)) or isinstance(value, pd.Timestamp):
        return "DATETIME"
    else:
        return "VARCHAR"


def _infer_data_type_from_samples(values) -> str:
    """Infer data type from multiple sample values for better accuracy.
    
    Args:
        values: List of sample values from a column
        
    Returns:
        str: SQL data type string
    """
    non_null_values = [v for v in values if v is not None and pd.notna(v)]
    
    if not non_null_values:
        return "VARCHAR"
    
    # Analyze types across all samples
    type_counts = {}
    for value in non_null_values:
        inferred_type = _infer_data_type(value)
        type_counts[inferred_type] = type_counts.get(inferred_type, 0) + 1
    
    # Return the most common type
    if type_counts:
        return max(type_counts.items(), key=lambda x: x[1])[0]
    
    return "VARCHAR"


def _map_type(data_type: str) -> MYSQL_DATA_TYPE:
    """Map SQL data types to MySQL types.
    
    Args:
        data_type (str): The SQL data type name
        
    Returns:
        MYSQL_DATA_TYPE: The corresponding MySQL data type
    """
    if data_type is None:
        return MYSQL_DATA_TYPE.VARCHAR
    
    data_type_upper = data_type.upper()
    
    type_map = {
        "VARCHAR": MYSQL_DATA_TYPE.VARCHAR,
        "TEXT": MYSQL_DATA_TYPE.TEXT,
        "INTEGER": MYSQL_DATA_TYPE.INT,
        "INT": MYSQL_DATA_TYPE.INT,
        "BIGINT": MYSQL_DATA_TYPE.BIGINT,
        "DECIMAL": MYSQL_DATA_TYPE.DECIMAL,
        "FLOAT": MYSQL_DATA_TYPE.FLOAT,
        "DOUBLE": MYSQL_DATA_TYPE.DOUBLE,
        "BOOLEAN": MYSQL_DATA_TYPE.BOOL,
        "BOOL": MYSQL_DATA_TYPE.BOOL,
        "DATE": MYSQL_DATA_TYPE.DATE,
        "DATETIME": MYSQL_DATA_TYPE.DATETIME,
        "TIMESTAMP": MYSQL_DATA_TYPE.DATETIME,
        "TIME": MYSQL_DATA_TYPE.TIME,
    }
    
    return type_map.get(data_type_upper, MYSQL_DATA_TYPE.VARCHAR)


def _cast_column_to_type(series: pd.Series, sql_type: str) -> pd.Series:
    """Cast a pandas Series to the appropriate type based on SQL data type.
    
    Args:
        series: pandas Series to cast
        sql_type: SQL data type string (VARCHAR, INTEGER, etc.)
        
    Returns:
        pandas Series with appropriate dtype
    """
    sql_type_upper = sql_type.upper() if sql_type else "VARCHAR"
    
    try:
        if sql_type_upper in ("INTEGER", "INT", "BIGINT"):
            # Try to convert to integer, handling NaN values
            return pd.to_numeric(series, errors='coerce').astype('Int64')
        elif sql_type_upper in ("DECIMAL", "FLOAT", "DOUBLE"):
            # Convert to float
            return pd.to_numeric(series, errors='coerce').astype('float64')
        elif sql_type_upper in ("BOOLEAN", "BOOL"):
            # Convert to boolean
            return series.astype('boolean')
        elif sql_type_upper in ("DATE", "DATETIME", "TIMESTAMP"):
            # Convert to datetime
            return pd.to_datetime(series, errors='coerce')
        else:
            # VARCHAR, TEXT, or unknown - keep as string
            return series.astype('string')
    except Exception as e:
        logger.warning(f"Error casting column to {sql_type}: {e}, keeping original type")
        return series


class SheetsHandler(DatabaseHandler):
    """
    This handler handles connection and execution of queries against the Excel Sheet.
    TODO: add authentication for private sheets
    """

    name = "sheets"

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.renderer = SqlalchemyRender("postgresql")
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self._column_types_cache = None  # Cache for column types

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """
        url = f"https://docs.google.com/spreadsheets/d/{self.connection_data['spreadsheet_id']}/gviz/tq?tqx=out:csv&sheet={self.connection_data['sheet_name']}"
        try:
            self.sheet = pd.read_csv(url, on_bad_lines='skip')
        except pd.errors.EmptyDataError:
            error_msg = (
                f"Google Sheet '{self.connection_data['sheet_name']}' "
                f"(ID: {self.connection_data['spreadsheet_id']}) is empty or has no columns. "
                f"Please ensure the sheet contains data."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = (
                f"Error reading Google Sheet '{self.connection_data['sheet_name']}' "
                f"(ID: {self.connection_data['spreadsheet_id']}): {str(e)}"
            )
            logger.error(error_msg)
            raise
        
        self.connection = duckdb.connect()
        self.connection.register(self.connection_data["sheet_name"], self.sheet)
        self.is_connected = True
        # Clear column types cache when reconnecting
        self._column_types_cache = None

        return self.connection

    def disconnect(self):
        """
        Close any existing connections.
        """
        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to the Google Sheet with ID {self.connection_data['spreadsheet_id']}, {e}!")
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> StatusResponse:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """

        need_to_close = self.is_connected is False
        connection = self.connect()
        try:
            result_df = connection.execute(query).fetchdf()
            if not result_df.empty:
                # Get column types and cast result columns accordingly
                column_types = self._get_column_types()
                
                # Cast each column to its inferred type
                for column_name in result_df.columns:
                    if column_name in column_types:
                        sql_type = column_types[column_name]
                        logger.debug(f"Casting column '{column_name}' to type '{sql_type}'")
                        result_df[column_name] = _cast_column_to_type(result_df[column_name], sql_type)
                
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    result_df
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
                connection.commit()
        except Exception as e:
            logger.error(
                f"Error running query: {query} on the Google Sheet with ID {self.connection_data['spreadsheet_id']}!"
            )
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> StatusResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
        query_str = self.renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """
        response = Response(
            RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame([self.connection_data["sheet_name"]], columns=["table_name"])
        )

        return response

    def _get_column_types(self) -> dict:
        """Get or infer column types for the sheet.
        
        Returns:
            dict: Mapping of column names to SQL data types
        """
        # Return cached types if available
        if self._column_types_cache is not None:
            return self._column_types_cache
        
        # Ensure we're connected and have the sheet data
        if not hasattr(self, 'sheet') or self.sheet is None:
            self.connect()
        
        # Sample 3 rows to infer data types from actual values
        sample_size = min(3, len(self.sheet))
        sample_data = self.sheet.head(sample_size) if not self.sheet.empty else pd.DataFrame()
        
        logger.debug(f"Sampling {sample_size} rows for column type inference")
        logger.debug(f"Sampled data:\n{sample_data.to_string()}")
        logger.debug(f"Column names: {list(self.sheet.columns)}")
        
        # Infer data types from sample values for each column
        column_types = {}
        for column_name in self.sheet.columns:
            if not sample_data.empty:
                column_values = sample_data[column_name].tolist()
                logger.debug(f"Column '{column_name}' sample values: {column_values}")
                inferred_type = _infer_data_type_from_samples(column_values)
                logger.debug(f"Column '{column_name}' inferred type: {inferred_type}")
                column_types[column_name] = inferred_type
            else:
                # If no data, fall back to pandas dtype
                dtype = self.sheet[column_name].dtype
                inferred_type = _pandas_dtype_to_sql_type(dtype) if dtype is not None else "VARCHAR"
                logger.debug(f"Column '{column_name}' has no sample data, using pandas dtype '{dtype}' -> '{inferred_type}'")
                column_types[column_name] = inferred_type
        
        # Cache the types
        self._column_types_cache = column_types
        return column_types

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns in standard information_schema.columns format.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """
        # Ensure we're connected and have the sheet data
        if not hasattr(self, 'sheet') or self.sheet is None:
            self.connect()
        
        # Validate that table_name matches the configured sheet_name
        if table_name != self.connection_data.get('sheet_name'):
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Table '{table_name}' not found. Available table: {self.connection_data.get('sheet_name')}"
            )
        
        # Get column types
        column_types = self._get_column_types()
        sql_types = [column_types.get(col, "VARCHAR") for col in self.sheet.columns]
        
        # Transform to information_schema.columns format (all required fields)
        columns_data = []
        for ordinal_position, (column_name, sql_type) in enumerate(zip(self.sheet.columns, sql_types), start=1):
            columns_data.append(
                {
                    "COLUMN_NAME": column_name,
                    "DATA_TYPE": sql_type,
                    "ORDINAL_POSITION": ordinal_position,
                    "COLUMN_DEFAULT": None,
                    "IS_NULLABLE": "YES",  # Assume nullable since we can't determine from CSV
                    "CHARACTER_MAXIMUM_LENGTH": None,
                    "CHARACTER_OCTET_LENGTH": None,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                    "DATETIME_PRECISION": None,
                    "CHARACTER_SET_NAME": None,
                    "COLLATION_NAME": None,
                }
            )
        
        df = pd.DataFrame(columns_data)
        result = Response(RESPONSE_TYPE.TABLE, data_frame=df)
        result.to_columns_table_response(map_type_fn=_map_type)
        
        return result
