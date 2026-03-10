"""Chart generation agent using Pydantic AI"""

from typing import Optional, Dict, Any
import copy
import pandas as pd

from pydantic_ai import Agent
from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import Identifier, Constant, OrderBy

from mindsdb.utilities import log
from mindsdb.interfaces.agents.utils.chart_toolkit import ChartConfig
from mindsdb.interfaces.agents.utils.pydantic_ai_model_factory import get_model_instance_from_kwargs
from mindsdb.interfaces.agents.modes import prompts
from mindsdb.utilities.exception import QueryError
from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.api.executor.utilities.sql import query_dfs


logger = log.getLogger(__name__)


def _replace_table_refs_with_df(query_ast):
    """Replace all table references in the query AST with the identifier 'df' (in-place).
    Chart-generated queries may use subqueries and reference the original table; we run
    against a single dataframe, so all table refs must point to 'df'.
    """

    def replace_table(node, is_table, **kwargs):
        if is_table and isinstance(node, Identifier):
            node.parts = ["df"]

    query_traversal(query_ast, replace_table)


def _fix_order_by_string_literals(query_ast):
    """Replace ORDER BY string literals (Constant) with column identifiers (Identifier) in-place.
    DuckDB rejects ORDER BY non-integer literals; the LLM often generates ORDER BY 'column alias'.
    Converting to Identifier ensures the rendered SQL uses ORDER BY "column alias" (valid).
    """

    def fix_order_by(node, **kwargs):
        if isinstance(node, OrderBy) and node.field is not None and isinstance(node.field, Constant):
            node.field = Identifier(parts=[str(node.field.value)])

    query_traversal(query_ast, fix_order_by)


def _prepare_chart_data_query(data_query_string: str, df: pd.DataFrame):
    """Parse chart-generated SQL, fix common LLM mistakes (table refs, ORDER BY literals), run on df."""
    query_ast = parse_sql(data_query_string)
    _replace_table_refs_with_df(query_ast)
    _fix_order_by_string_literals(query_ast)
    return query_dfs({"df": df}, query_ast, session=None)


class ChartAgent:
    """Lightweight agent for generating Chart.js configurations"""

    MAX_RETRIES = 3

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
            "prompt_template", "You are an expert at generating Chart.js configurations from SQL queries"
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
            raise ValueError("No model name provided for chart agent. Please configure default_llm in system settings.")

        return args

    def _generate_data_catalog(
        self,
        query: str,
        df: pd.DataFrame,
    ) -> str:
        """
        Generate a data catalog by executing the query with LIMIT 5 and analyzing the results.

        Args:
            query: SQL query string
            df: dataframe with all data that got by executing query

        Returns:
            str: Data catalog string with sample data and column analysis
        """
        sample_df = df.head(5)
        if sample_df.empty:
            return "Data Catalog:\nNote: Query returned no rows."

        # Build data catalog string
        catalog_parts = ["=== DATA CATALOG ==="]
        catalog_parts.append(f"\nSample Query:\n{query}")

        # Column analysis using column metadata from result_set
        catalog_parts.append("\nColumn Analysis:")

        for col_name in sample_df.columns:
            col_type = str(sample_df[col_name].dtype)
            # Map pandas dtypes to more readable types
            if "int" in col_type:
                type_desc = "integer"
            elif "float" in col_type:
                type_desc = "float"
            elif "bool" in col_type:
                type_desc = "boolean"
            elif "datetime" in col_type or "timestamp" in col_type:
                type_desc = "datetime/timestamp"
            elif "object" in col_type or "string" in col_type:
                type_desc = "string/text"
            else:
                type_desc = col_type

            # Get sample values (non-null)
            sample_values = sample_df[col_name].dropna().head(3).tolist()
            sample_str = ", ".join(str(v) for v in sample_values[:3])

            catalog_parts.append(f"  - {col_name}: {type_desc}")
            if sample_str:
                catalog_parts.append(f"    Sample values: {sample_str}")

        # Sample data as CSV-like format
        catalog_parts.append("\nSample Data (first 5 rows):")
        # Convert DataFrame to CSV string
        catalog_parts.append(sample_df.to_csv(index=False, lineterminator="\n"))

        return "\n".join(catalog_parts)

    def generate_chart_config(
        self,
        query: str,
        df: pd.DataFrame,
        prompt: Optional[str] = None,
        error_context: Optional[str] = None,
        retry_count: Optional[int] = None,
    ) -> ChartConfig:
        """
        Generate Chart.js configuration and data query from SQL query.

        Args:
            query: SQL query string
            df: dataframe with all data that got by executing query
            prompt: Optional prompt describing chart intent
            error_context: Optional error context from previous failed attempts
            retry_count: Optional retry attempt number for error messages

        Returns:
            ChartConfig: Pydantic model with chartjs_config and data_query_string
        """
        # Generate data catalog
        data_catalog = self._generate_data_catalog(query, df)

        # Create agent with ChartConfig as output type
        agent = Agent(self.model_instance, system_prompt=self.system_prompt, output_type=ChartConfig)

        # Build prompt for chart generation
        chart_prompt = f"""Given the following SQL query and optional intent, generate a Chart.js configuration and data transformation query.

SQL Query:
{query}
"""

        if prompt:
            chart_prompt += f"\nChart Intent: {prompt}\n"

        chart_prompt += f"\nSample Data Catalog:\n{data_catalog}\nInstructions:\n{prompts.sql_description}\n\n{prompts.chart_generation_prompt}"

        # Add error context if provided (for retry attempts)
        if error_context:
            chart_prompt += f"\n\nPrevious query errors:\n{error_context}"
            if retry_count is not None:
                chart_prompt += f"\n\nPlease fix the query and try again. This is retry attempt {retry_count} of {self.MAX_RETRIES}."

        logger.debug("ChartAgent.generate_chart_config: Sending prompt to LLM")

        try:
            # Generate chart config
            result = agent.run_sync(chart_prompt)
            chart_config = result.output

            logger.debug(
                f"ChartAgent.generate_chart_config: Received chart config with type: {chart_config.chartjs_config.get('type', 'unknown')}"
            )

            return chart_config
        except Exception as e:
            logger.error(f"ChartAgent.generate_chart_config: Error generating chart config: {e}")
            # Log the raw result if available for debugging
            if hasattr(e, "result") and hasattr(e.result, "output"):
                logger.debug(f"ChartAgent.generate_chart_config: Raw output: {e.result.output}")
            raise

    def generate_chart_with_data(
        self,
        query: str,
        df: pd.DataFrame,
        prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate Chart.js configuration, execute data query, and populate datasets.
        Args:
            query: SQL query string
            df: dataframe with all data that got by executing query
            prompt: Optional prompt describing chart intent
            context: Optional context dict for query execution
            params: Optional params dict for query execution
        Returns:
            dict: Dictionary with 'data_query_string' and populated 'chartjs_config'
        Raises:
            QueryError: If query execution fails
            ValueError: If data structure is invalid
        """

        # Initialize retry tracking
        retry_count = 0
        accumulated_errors = []
        # Use provided executor or create a new one

        # Retry loop for query execution
        while retry_count <= self.MAX_RETRIES:
            # Generate chart configuration (with data catalog and error context if retrying)
            error_context = None
            if accumulated_errors:
                # Format error context from accumulated errors (keep last 3)
                error_context = "\n---\n".join(accumulated_errors[-3:])
                logger.debug(
                    f"ChartAgent.generate_chart_with_data: Regenerating chart config with error context (attempt {retry_count}/{self.MAX_RETRIES})"
                )

            chart_config = self.generate_chart_config(
                query, df, prompt, error_context=error_context, retry_count=retry_count if retry_count > 0 else None
            )
            try:
                logger.debug(
                    f"ChartAgent.generate_chart_with_data: Executing transformed query on provided dataframe: {chart_config.data_query_string[:100]}..."
                )
                data_df = _prepare_chart_data_query(chart_config.data_query_string, df)

                if data_df.empty:
                    raise ValueError(
                        "Data query returned no rows. Please check your query filters or data availability."
                    )
                # Validate DataFrame structure
                if len(data_df.columns) < 2:
                    raise ValueError(
                        f"Data query must return at least 2 columns (labels and at least one dataset). Got {len(data_df.columns)} column(s)."
                    )
                # Populate Chart.js config with data
                chartjs_config = chart_config.chartjs_config.copy()
                # First column is labels
                labels = data_df.iloc[:, 0].tolist()
                chartjs_config["labels"] = labels
                # Remaining columns are datasets
                existing_datasets = chartjs_config.get("datasets", [])
                num_data_columns = len(data_df.columns) - 1  # Excluding labels column
                # If datasets is empty or doesn't match column count, create datasets from columns
                if not existing_datasets or len(existing_datasets) != num_data_columns:
                    datasets = []
                    for col_idx in range(1, len(data_df.columns)):
                        col_name = data_df.columns[col_idx]
                        dataset = {"label": str(col_name), "data": []}
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
                    if col_idx < len(data_df.columns):
                        dataset["data"] = data_df.iloc[:, col_idx].tolist()
                chartjs_config["datasets"] = datasets
                # Return response
                return {"data_query_string": chart_config.data_query_string, "chartjs_config": chartjs_config}
            except (QueryError, ValueError) as e:
                # Extract error message
                if isinstance(e, QueryError):
                    error_message = e.db_error_msg or str(e)
                    failed_query = (
                        e.failed_query
                        if hasattr(e, "failed_query")
                        else (chart_config.data_query_string if "chart_config" in locals() else query)
                    )
                else:
                    error_message = str(e)
                    failed_query = chart_config.data_query_string if "chart_config" in locals() else query
                # Accumulate error for context
                accumulated_errors.append(f"Query: {failed_query}\nError: {error_message}")
                # Check if we should retry
                if retry_count < self.MAX_RETRIES:
                    logger.warning(
                        f"ChartAgent.generate_chart_with_data: Query execution failed (retry {retry_count + 1}/{self.MAX_RETRIES}): {error_message}"
                    )
                    retry_count += 1
                    # Continue loop to retry with new chart config
                    continue
                else:
                    # All retries exhausted, raise the error
                    logger.error(
                        f"ChartAgent.generate_chart_with_data: Query execution failed after {self.MAX_RETRIES} retries. Last error: {error_message}"
                    )
                    raise e
