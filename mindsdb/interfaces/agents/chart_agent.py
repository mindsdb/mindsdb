"""Chart generation agent using Pydantic AI"""

from typing import Optional, Dict, Any
import copy
import pandas as pd

from pydantic_ai import Agent

from mindsdb.utilities import log
from mindsdb.interfaces.agents.utils.chart_toolkit import ChartConfig
from mindsdb.interfaces.agents.utils.pydantic_ai_model_factory import (
    get_model_instance_from_kwargs
)
from mindsdb.interfaces.agents.prompts import agent_prompts
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import SQLAnswer
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE as SQL_RESPONSE_TYPE
from mindsdb.api.executor.exceptions import ExecutorException, UnknownError
from mindsdb.utilities.exception import QueryError

logger = log.getLogger(__name__)


class ChartAgent:
    """Lightweight agent for generating Chart.js configurations"""
    
    def __init__(self, executor=None):
        """
        Initialize Chart Agent using system default LLM configuration.
        
        Args:
            executor: FakeMysqlProxy instance for executing queries. If None, will create one when needed.
        """
        self.args = self._initialize_args()
        
        # Provider model instance
        self.model_instance = get_model_instance_from_kwargs(self.args)
        
        # System prompt for chart generation
        self.system_prompt = self.args.get(
            "prompt_template",
            "You are an expert at generating Chart.js configurations from SQL queries"
        )
        
        # Executor for query execution
        self.executor = executor
    
    def _initialize_args(self) -> dict:
        """
        Initialize the arguments for agent execution using system default LLM config.
        
        Returns:
            dict: Final parameters for agent execution
        """
        from mindsdb.utilities.config import config
        
        # Get default LLM config from system config
        args = copy.deepcopy(config.get("default_llm", {}))
        
        # Remove use_default_llm flag if present
        args.pop("use_default_llm", None)
        
        if "model_name" not in args:
            raise ValueError(
                "No model name provided for chart agent. Please configure default_llm in system settings."
            )
        
        return args
    
    def _generate_data_catalog(
        self, 
        query: str, 
        context: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate a data catalog by executing the query with LIMIT 5 and analyzing the results.
        
        Args:
            query: SQL query string
            context: Optional context dict for query execution
            params: Optional params dict for query execution
            
        Returns:
            str: Data catalog string with sample data and column analysis
        """
        if context is None:
            context = {}
        if params is None:
            params = {}
        
        # Wrap query with LIMIT 5
        # Check if query already has LIMIT
        query_upper = query.upper().strip()
        import re
        # Remove final semicolon from query, if any, using regex
        query = re.sub(r';\s*$', '', query.strip())
        query_upper = query.upper().strip()
        
        # Query already has LIMIT, wrap it in a subquery
        sample_query = f"SELECT * FROM ({query}) LIMIT 5"
    
        # Use provided executor or create a new one
        if self.executor is None:
            from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
            executor = FakeMysqlProxy()
        else:
            executor = self.executor
        
        executor.set_context(context)
        
        try:
            result: SQLAnswer = executor.process_query(sample_query, params=params)
            
            if result.type == SQL_RESPONSE_TYPE.ERROR:
                # If query fails, return a simple error message in catalog
                return f"Data Catalog:\nNote: Could not sample data from query. Error: {result.error_message or 'Unknown error'}"
            
            if result.type != SQL_RESPONSE_TYPE.TABLE or result.result_set is None:
                return "Data Catalog:\nNote: Query did not return tabular data."
            
            # Convert result to DataFrame
            df = result.result_set.to_df()
            
            if df.empty:
                return "Data Catalog:\nNote: Query returned no rows."
            
            # Build data catalog string
            catalog_parts = ["=== DATA CATALOG ==="]
            catalog_parts.append(f"\nSample Query:\n{sample_query}")
            
            # Column analysis using column metadata from result_set
            catalog_parts.append("\nColumn Analysis:")
            columns = result.result_set.columns
            
            for i, column in enumerate(columns):
                col_name = column.alias or column.name
                
                # Use column type from result_set if available, otherwise infer from DataFrame
                if column.type is not None:
                    # MYSQL_DATA_TYPE enum value
                    type_desc = column.type.value if hasattr(column.type, 'value') else str(column.type)
                else:
                    # Fallback to DataFrame dtype if column type not available
                    col_type = str(df.iloc[:, i].dtype)
                    # Map pandas dtypes to more readable types
                    if 'int' in col_type:
                        type_desc = "integer"
                    elif 'float' in col_type:
                        type_desc = "float"
                    elif 'bool' in col_type:
                        type_desc = "boolean"
                    elif 'datetime' in col_type or 'timestamp' in col_type:
                        type_desc = "datetime/timestamp"
                    elif 'object' in col_type or 'string' in col_type:
                        type_desc = "string/text"
                    else:
                        type_desc = col_type
                
                # Get sample values (non-null)
                sample_values = df.iloc[:, i].dropna().head(3).tolist()
                sample_str = ", ".join(str(v) for v in sample_values[:3])
                
                catalog_parts.append(f"  - {col_name}: {type_desc}")
                if sample_str:
                    catalog_parts.append(f"    Sample values: {sample_str}")
            
            # Sample data as CSV-like format
            catalog_parts.append("\nSample Data (first 5 rows):")
            # Convert DataFrame to CSV string
            catalog_parts.append(df.to_csv(index=False, lineterminator='\n'))
            
            return "\n".join(catalog_parts)
            
        except Exception as e:
            logger.warning(f"ChartAgent._generate_data_catalog: Error generating catalog: {e}")
            return f"Data Catalog:\nNote: Could not generate data catalog. Error: {str(e)}"
    
    def generate_chart_config(
        self, 
        query: str, 
        prompt: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> ChartConfig:
        """
        Generate Chart.js configuration and data query from SQL query.
        
        Args:
            query: SQL query string
            prompt: Optional prompt describing chart intent
            context: Optional context dict for query execution
            params: Optional params dict for query execution
            
        Returns:
            ChartConfig: Pydantic model with chartjs_config and data_query_string
        """
        # Generate data catalog
        data_catalog = self._generate_data_catalog(query, context, params)
        
        # Create agent with ChartConfig as output type
        agent = Agent(
            self.model_instance,
            system_prompt=self.system_prompt,
            output_type=ChartConfig
        )
        
        # Build prompt for chart generation
        chart_prompt = f"""Given the following SQL query and optional intent, generate a Chart.js configuration and data transformation query.

SQL Query:
{query}
"""
        
        if prompt:
            chart_prompt += f"\nChart Intent: {prompt}\n"
        
        chart_prompt += f"\nSample Data Catalog:\n{data_catalog}\nInstructions:\n{agent_prompts.sql_description}\n\n{agent_prompts.chart_generation_prompt}"
        
        logger.debug(f"ChartAgent.generate_chart_config: Sending prompt to LLM")
        
        try:
            # Generate chart config
            result = agent.run_sync(chart_prompt)
            chart_config = result.output
            
            logger.debug(f"ChartAgent.generate_chart_config: Received chart config with type: {chart_config.chartjs_config.get('type', 'unknown')}")
            
            return chart_config
        except Exception as e:
            logger.error(f"ChartAgent.generate_chart_config: Error generating chart config: {e}")
            # Log the raw result if available for debugging
            if hasattr(e, 'result') and hasattr(e.result, 'output'):
                logger.debug(f"ChartAgent.generate_chart_config: Raw output: {e.result.output}")
            raise
    
    def generate_chart_with_data(
        self, 
        query: str, 
        prompt: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate Chart.js configuration, execute data query, and populate datasets.
        
        Args:
            query: SQL query string
            prompt: Optional prompt describing chart intent
            context: Optional context dict for query execution
            params: Optional params dict for query execution
            
        Returns:
            dict: Dictionary with 'data_query_string' and populated 'chartjs_config'
            
        Raises:
            QueryError: If query execution fails
            ValueError: If data structure is invalid
        """
        if context is None:
            context = {}
        if params is None:
            params = {}
        
        # Generate chart configuration (with data catalog)
        chart_config = self.generate_chart_config(query, prompt, context, params)
        
        # Execute the data query
        logger.debug(f"ChartAgent.generate_chart_with_data: Executing data query: {chart_config.data_query_string[:100]}...")
        
        # Use provided executor or create a new one
        if self.executor is None:
            from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
            executor = FakeMysqlProxy()
        else:
            executor = self.executor
        
        executor.set_context(context)
        
        try:
            result: SQLAnswer = executor.process_query(chart_config.data_query_string, params=params)
            
            if result.type == SQL_RESPONSE_TYPE.ERROR:
                error_message = result.error_message or "Unknown error executing data query"
                raise QueryError(
                    db_error_msg=error_message,
                    failed_query=chart_config.data_query_string,
                    is_external=False,
                    is_expected=True
                )
            
            if result.type != SQL_RESPONSE_TYPE.TABLE or result.result_set is None:
                raise ValueError(
                    "Data query did not return tabular data. The query may have executed successfully but returned no data rows."
                )
            
            # Convert result to DataFrame
            df = result.result_set.to_df()
            
            if df.empty:
                raise ValueError(
                    "Data query returned no rows. Please check your query filters or data availability."
                )
            
            # Validate DataFrame structure
            if len(df.columns) < 2:
                raise ValueError(
                    f"Data query must return at least 2 columns (labels and at least one dataset). Got {len(df.columns)} column(s)."
                )
            
            # Populate Chart.js config with data
            chartjs_config = chart_config.chartjs_config.copy()
            
            # First column is labels
            labels = df.iloc[:, 0].tolist()
            chartjs_config["labels"] = labels
            
            # Remaining columns are datasets
            existing_datasets = chartjs_config.get("datasets", [])
            num_data_columns = len(df.columns) - 1  # Excluding labels column
            
            # If datasets is empty or doesn't match column count, create datasets from columns
            if not existing_datasets or len(existing_datasets) != num_data_columns:
                datasets = []
                for col_idx in range(1, len(df.columns)):
                    col_name = df.columns[col_idx]
                    dataset = {
                        "label": str(col_name),
                        "data": []
                    }
                    # Try to preserve properties from existing dataset if available
                    dataset_idx = col_idx - 1
                    if dataset_idx < len(existing_datasets):
                        existing_dataset = existing_datasets[dataset_idx]
                        # Copy properties like backgroundColor, borderColor, etc.
                        for key in ["backgroundColor", "borderColor", "borderWidth", "fill"]:
                            if key in existing_dataset:
                                dataset[key] = existing_dataset[key]
                    datasets.append(dataset)
            else:
                # Use existing datasets structure, just populate data
                datasets = existing_datasets
            
            # Populate data arrays
            for dataset_idx, dataset in enumerate(datasets):
                col_idx = dataset_idx + 1
                if col_idx < len(df.columns):
                    dataset["data"] = df.iloc[:, col_idx].tolist()
            
            chartjs_config["datasets"] = datasets
            
            # Return response
            return {
                "data_query_string": chart_config.data_query_string,
                "chartjs_config": chartjs_config
            }
            
        except ExecutorException as e:
            error_text = str(e)
            raise QueryError(
                db_error_msg=error_text,
                failed_query=chart_config.data_query_string,
                is_external=False,
                is_expected=True
            )
        except QueryError:
            raise
        except UnknownError as e:
            error_text = str(e)
            raise QueryError(
                db_error_msg=error_text,
                failed_query=chart_config.data_query_string,
                is_external=False,
                is_expected=False
            )
        except Exception as e:
            # Re-raise ValueError and QueryError as-is, wrap others
            if isinstance(e, (ValueError, QueryError)):
                raise
            error_text = str(e)
            raise QueryError(
                db_error_msg=error_text,
                failed_query=chart_config.data_query_string if hasattr(chart_config, 'data_query_string') else query,
                is_external=False,
                is_expected=False
            )

