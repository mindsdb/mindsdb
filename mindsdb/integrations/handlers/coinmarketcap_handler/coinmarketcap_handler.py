import requests
import pandas as pd
from typing import Dict, List
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse,
    RESPONSE_TYPE
)
from mindsdb.utilities import log

from .coinmarketcap_tables import (
    ListingTable,
    QuotesTable,
    InfoTable,
    GlobalMetricsTable
)

logger = log.getLogger(__name__)

class CoinMarketCapHandler(APIHandler):
    """ 
    The CoinMarketCap Handler implementation
    """

    def __init__(self, name: str, **kwargs):
        """ 
        Initialize the CoinMarketCap Handler
        """
        super().__init__(name)

        # Get connection data (API key, etc.)
        connection_data = kwargs.get('connection_data', {})
        self.connection_data = connection_data

        # API configuration
        self.api_key = connection_data.get('api_key')
        self.base_url = connection_data.get('base_url', 'https://pro-api.coinmarketcap.com')

        # Initialize tables - THIS IS CRUCIAL FOR get_tables() TO WORK
        self._register_table('listings', ListingTable(self))
        self._register_table('quotes', QuotesTable(self))
        self._register_table('info', InfoTable(self))
        self._register_table('global_metrics', GlobalMetricsTable(self))

        self.is_connected = False

    def connect(self) -> StatusResponse:
        """Set up the connection to CoinMarketCap API"""
        if self.is_connected:
            return StatusResponse(True)
            
        # Test the connection
        try:
            # Make a simple test request
            headers = {
                'X-CMC_PRO_API_KEY': self.api_key,
                'Accept': 'application/json'
            }
            
            response = requests.get(
                f'{self.base_url}/v1/key/info',  # API key info endpoint
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                self.is_connected = True
                logger.info("Successfully connected to CoinMarketCap API")
                return StatusResponse(True)
            else:
                error_msg = f"API returned status {response.status_code}"
                logger.error(f"Connection failed: {error_msg}")
                return StatusResponse(False, error_message=error_msg)
                
        except Exception as e:
            error_msg = f"Connection error: {str(e)}"
            logger.error(error_msg)
            return StatusResponse(False, error_message=error_msg)

    def check_connection(self) -> StatusResponse:  # Fixed return type annotation
        """Check connection to the handler
        
        Returns:
            StatusResponse
        """
        return self.connect()
    
    def query(self, query) -> HandlerResponse:
        """
        Execute a SQL query against the CoinMarketCap API
        
        Args:
            query: SQL query object (parsed by MindsDB)
            
        Returns:
            HandlerResponse: Query result
        """
        try:
            # Ensure we're connected
            if not self.is_connected:
                connection_result = self.connect()
                if not connection_result.success:
                    return HandlerResponse(
                        RESPONSE_TYPE.ERROR,
                        error_message=f"Failed to connect: {connection_result.error_message}"
                    )
            
            # Get the table name from the query
            table_name = self._normalize_table_name(query.from_table)
            
            # Check if table exists
            if not self._table_exists(table_name):
                return HandlerResponse(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Table '{table_name}' not found. Available tables: {', '.join(self._get_table_names())}"
                )
            
            # Get the table instance
            table = self._get_table(table_name)
            if table is None:
                return HandlerResponse(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Failed to get table instance for '{table_name}'"
                )
            
            # Execute the query on the table
            return table.select(query)
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            return HandlerResponse(
                RESPONSE_TYPE.ERROR,
                error_message=f"Query execution failed: {str(e)}"
            )

    def native_query(self, query: str) -> HandlerResponse:
        """Execute a native query (not implemented for API handlers usually)"""
        raise NotImplementedError("Native queries not supported for CoinMarketCap API")

    def _normalize_table_name(self, table_name) -> str:
        """Convert table name (string or Identifier) to string"""
        if hasattr(table_name, 'parts'):
            return table_name.parts[-1]
        return str(table_name)

    def get_tables(self) -> HandlerResponse:
        """Return list of entities that can be used as tables
        
        Returns:
            Response: Should have same columns as information_schema.tables
                     Column 'TABLE_NAME' is mandatory, other is optional.
        """
        tables = []
        
        # Get all registered table names
        for table_name in self._get_table_names():
            tables.append({
                'TABLE_NAME': table_name,
                'TABLE_SCHEMA': None,
                'TABLE_TYPE': 'BASE TABLE',
                'TABLE_COMMENT': self._get_table_description(table_name)
            })
        
        # Convert to DataFrame for proper response format
        result_df = pd.DataFrame(tables)
        
        return HandlerResponse(
            RESPONSE_TYPE.TABLE,
            result_df
        )

    def get_columns(self, table_name: str) -> HandlerResponse:
        """Returns a list of entity columns"""
        try:
            # Normalize the table name in case it's an Identifier
            table_name_str = self._normalize_table_name(table_name)
            logger.info(f"Getting columns for table: '{table_name_str}'")
            
            # Check if table exists
            if not self._table_exists(table_name_str):
                return HandlerResponse(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Table '{table_name_str}' not found"
                )
            
            # Get table instance and its columns
            table = self._get_table(table_name_str)
            if table is None:
                return HandlerResponse(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Failed to get table instance for '{table_name_str}'"
                )
            
            # Get column definitions from the table
            logger.info(f"Calling get_columns on table: {type(table).__name__}")
            columns_response = table.get_columns()
            
            if columns_response.type == RESPONSE_TYPE.ERROR:
                return columns_response
                
            columns_df = columns_response.data_frame
            logger.info(f"Table returned DataFrame with shape: {columns_df.shape}")
            logger.info(f"DataFrame columns: {list(columns_df.columns)}")
            logger.info(f"DataFrame head:\n{columns_df.head()}")
            
            # Check if the DataFrame has the expected columns
            expected_cols = ['name', 'type', 'description']
            missing_cols = [col for col in expected_cols if col not in columns_df.columns]
            if missing_cols:
                logger.error(f"Missing columns in table response: {missing_cols}")
                return HandlerResponse(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Table response missing columns: {missing_cols}"
                )
            
            # Transform to information_schema.columns format
            result_columns = []
            for idx, row in columns_df.iterrows():
                result_columns.append({
                    'COLUMN_NAME': str(row['name']),
                    'DATA_TYPE': str(row['type']), 
                    'IS_NULLABLE': 'YES',
                    'COLUMN_DEFAULT': None,
                    'COLUMN_COMMENT': str(row.get('description', '')),
                    'ORDINAL_POSITION': idx + 1
                })
            
            logger.info(f"Created {len(result_columns)} result columns")
            
            # Create DataFrame with explicit column order and ensure proper structure
            result_df = pd.DataFrame(result_columns)
            
            # Ensure we have all required columns in the right order
            required_columns = ['COLUMN_NAME', 'DATA_TYPE', 'IS_NULLABLE', 'COLUMN_DEFAULT', 'COLUMN_COMMENT', 'ORDINAL_POSITION']
            
            # Reorder columns to match expected format
            result_df = result_df[required_columns]
            
            logger.info(f"Final DataFrame shape: {result_df.shape}")
            logger.info(f"Final DataFrame columns: {list(result_df.columns)}")
            logger.info(f"Sample data:\n{result_df.head()}")
            
            return HandlerResponse(
                RESPONSE_TYPE.TABLE,
                result_df
            )
            
        except Exception as e:
            error_msg = f"Error getting columns for table '{table_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            return HandlerResponse(
                RESPONSE_TYPE.ERROR,
                error_message=error_msg
            )

    def _get_table_names(self) -> List[str]:
        """Get list of registered table names"""
        return list(self._tables.keys())

    def _table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        table_name_str = self._normalize_table_name(table_name)
        return table_name_str in self._tables

    def _get_table(self, table_name):
        """Get table instance by name"""
        table_name_str = self._normalize_table_name(table_name)
        return self._tables.get(table_name_str)

    def _get_table_description(self, table_name: str) -> str:
        """Get description for a table"""
        table_name_str = self._normalize_table_name(table_name)  # Add this line for consistency
        descriptions = {
            'listings': 'Latest cryptocurrency listings with market data',
            'quotes': 'Latest price quotes for specific cryptocurrencies',
            'info': 'Metadata information about cryptocurrencies',
            'global_metrics': 'Global cryptocurrency market metrics'
        }
        return descriptions.get(table_name_str, f'CoinMarketCap {table_name_str} data')

    def call_coinmarketcap_api(self, endpoint: str, params: dict = None) -> dict:
        """
        Utility method to make API calls to CoinMarketCap
        
        Args:
            endpoint (str): API endpoint path (e.g., '/v1/cryptocurrency/listings/latest')
            params (dict): Query parameters for the API call
            
        Returns:
            dict: JSON response from the API
            
        Raises:
            Exception: If API call fails
        """
        if not self.is_connected:
            raise Exception("Not connected to CoinMarketCap API")

        headers = {
            'X-CMC_PRO_API_KEY': self.api_key,
            'Accept': 'application/json'
        }
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params or {},
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                raise Exception("Invalid API key or unauthorized access")
            elif response.status_code == 403:
                raise Exception("API quota exceeded or forbidden access")
            elif response.status_code == 429:
                raise Exception("Rate limit exceeded. Please try again later")
            else:
                raise Exception(f"API request failed with status {response.status_code}: {response.text}")
                
        except requests.exceptions.Timeout:
            raise Exception("API request timed out")
        except requests.exceptions.ConnectionError:
            raise Exception("Failed to connect to CoinMarketCap API")
        except requests.exceptions.RequestException as e:
            raise Exception(f"API request failed: {str(e)}")

    def get_api_key_info(self) -> dict:
        """
        Get information about the API key usage and limits
        
        Returns:
            dict: API key information
        """
        try:
            return self.call_coinmarketcap_api('/v1/key/info')
        except Exception as e:
            logger.error(f"Failed to get API key info: {str(e)}")
            raise