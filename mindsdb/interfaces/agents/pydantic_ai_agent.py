"""Pydantic AI Agent wrapper to replace LangchainAgent"""

import json
import warnings
from typing import Dict, List, Optional, Any, Iterable
import pandas as pd

from pydantic_ai import Agent
from pydantic_ai.messages import ModelRequest, ModelResponse, ModelMessage, TextPart

from mindsdb.utilities import log
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.agents.utils.constants import (
    USER_COLUMN,
    ASSISTANT_COLUMN,
    CONTEXT_COLUMN,
    TRACE_ID_COLUMN,
)

from mindsdb.interfaces.agents.utils.sql_toolkit import MindsDBQuery, SQLQuery, QueryType, Plan
from mindsdb.interfaces.agents.utils.pydantic_ai_model_factory import (
    get_model_instance_from_kwargs
)
from mindsdb.interfaces.agents.utils.data_catalog_builder import DataCatalogBuilder, dataframe_to_markdown

from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.langfuse import LangfuseClientWrapper
from mindsdb.interfaces.agents.prompts import agent_prompts
logger = log.getLogger(__name__)
DEBUG_LOGGER = logger.info



# Suppress asyncio warnings about unretrieved task exceptions from httpx cleanup
# This is a known issue where httpx.AsyncClient tries to close connections after the event loop is closed
warnings.filterwarnings("ignore", message=".*Task exception was never retrieved.*", category=RuntimeWarning)

class PydanticAIAgent:
    """Pydantic AI-based agent to replace LangchainAgent"""
    
    def __init__(self, agent: db.Agents, model: dict = None, llm_params: dict = None):
        """
        Initialize Pydantic AI agent.
        
        Args:
            agent: Agent database record
            model: Model information (optional)
            llm_params: LLM parameters (optional)
        """
        self.agent = agent
        self.model = model
        
        self.run_completion_span: Optional[object] = None
        self.llm: Optional[object] = None
        self.embedding_model: Optional[object] = None
        
        self.log_callback_handler: Optional[object] = None
        self.langfuse_callback_handler: Optional[object] = None
        self.mdb_langfuse_callback_handler: Optional[object] = None
        
        self.langfuse_client_wrapper = LangfuseClientWrapper()
        self.args = self._initialize_args(llm_params)
        
        # Provider model instance
        self.model_instance = get_model_instance_from_kwargs(self.args)
        
        # Command executor for queries
        self.executor = MindsDBQuery()
        
        self.system_prompt = self.args.get("prompt_template", "You are an expert MindsDB SQL data analyst")
        
        # Track current query state
        self._current_prompt: Optional[str] = None
        self._current_sql_query: Optional[str] = None
        self._current_query_result: Optional[pd.DataFrame] = None
        
        # Track SQL query context (when called from SQL query)
        self._sql_context: Optional[Dict] = None
    
    
    
    def _initialize_args(self, llm_params: dict = None) -> dict:
        """
        Initialize the arguments for agent execution.
        Uses the same pattern as knowledge bases: get default config and merge with user params.
        
        Args:
            llm_params: Parameters for agent execution (already merged with defaults from agents_controller)
            
        Returns:
            dict: Final parameters for agent execution
        """
        from mindsdb.utilities.config import config
        import copy
        
        # Get default LLM config from system config (same pattern as knowledge bases)
        default_llm_config = copy.deepcopy(config.get("default_llm", {}))
        
        # Start with agent params
        args = self.agent.params.copy() if self.agent.params else {}
        
        # Get model params from agent params (same structure as knowledge bases)
        if "model" in args:
            model_params = args.get("model", {})
        else:
            # If no "model" key, use params directly (backward compatibility)
            model_params = args
        
        # Merge default config with model params (same as knowledge bases get_model_params)
        if model_params:
            if not isinstance(model_params, dict):
                raise ValueError("Model parameters must be passed as a JSON object")
            
            # If provider mismatches - don't use default values
            if "provider" in model_params and model_params["provider"] != default_llm_config.get("provider"):
                combined_model_params = model_params.copy()
            else:
                combined_model_params = copy.deepcopy(default_llm_config)
                combined_model_params.update(model_params)
        else:
            # No model params provided - use defaults from config
            combined_model_params = default_llm_config
        
        # Remove use_default_llm flag if present
        combined_model_params.pop("use_default_llm", None)
        
        # Update args with combined model params
        args.update(combined_model_params)
        
        # Apply llm_params if provided (from agents_controller.get_agent_llm_params)
        if llm_params:
            args.update(llm_params)
        
        # Set model name and provider if given in create agent (these take precedence)
        if self.agent.model_name is not None:
            args["model_name"] = self.agent.model_name
        if self.agent.provider is not None:
            args["provider"] = self.agent.provider
        
        
        
        # Handle MindsDB provider
        if self.agent.provider == "mindsdb":
            args["model_name"] = self.agent.model_name
            prompt_template = self.model.get("problem_definition", {}).get("using", {}).get("prompt_template")
            if prompt_template is not None:
                args["prompt_template"] = prompt_template
        
        
        
        if "model_name" not in args:
            raise ValueError(
                "No model name provided for agent. Provide it in the model parameter or in the default model setup."
            )
        
        return args
    
    
    def _convert_messages_to_history(self, df: pd.DataFrame) -> List[ModelMessage]:
        """
        Convert DataFrame messages to Pydantic AI message history format.
        
        Args:
            df: DataFrame with user/assistant columns or role/content columns
            
        Returns:
            List of Pydantic AI Message objects
        """
        messages = []
        
        # Check if DataFrame has 'role' and 'content' columns (API format)
        if "role" in df.columns and "content" in df.columns:
            for _, row in df.iterrows():
                role = row.get("role")
                content = row.get("content", "")
                if pd.notna(role) and pd.notna(content):
                    if role == "user":
                        messages.append(ModelRequest.user_text_prompt(str(content)))
                    elif role == "assistant":
                        messages.append(ModelResponse(parts=[TextPart(content=str(content))]))
        else:
            # Legacy format with question/answer columns
            user_column = self.args.get("user_column", USER_COLUMN)
            assistant_column = self.args.get("assistant_column", ASSISTANT_COLUMN)
            
            for _, row in df.iterrows():
                user_msg = row.get(user_column)
                assistant_msg = row.get(assistant_column)
                
                if pd.notna(user_msg) and str(user_msg).strip():
                    messages.append(ModelRequest.user_text_prompt(str(user_msg)))
                
                if pd.notna(assistant_msg) and str(assistant_msg).strip():
                    messages.append(ModelResponse(parts=[TextPart(content=str(assistant_msg))]))
        
        return messages
    
    def _extract_current_prompt_and_history(self, messages: Any, args: Dict) -> tuple[str, List[ModelMessage]]:
        """
        Extract current prompt and message history from messages in various formats.
        
        Args:
            messages: Can be:
                - List of dicts with 'role' and 'content' (API format)
                - List of dicts with 'question' and 'answer' (Q&A format from A2A)
                - DataFrame with 'role'/'content' columns (API format)
                - DataFrame with 'question'/'answer' columns (legacy format)
            args: Arguments dict
            
        Returns:
            Tuple of (current_prompt: str, message_history: List[ModelMessage])
        """
        # Handle list of dicts with 'role' and 'content' (API format)
        if isinstance(messages, list) and len(messages) > 0:
            if isinstance(messages[0], dict) and "role" in messages[0]:
                # Convert to Pydantic AI Message objects
                pydantic_messages = []
                for msg in messages:
                    if msg.get("role") == "user":
                        pydantic_messages.append(ModelRequest.user_text_prompt(msg.get("content", "")))
                    elif msg.get("role") == "assistant":
                        pydantic_messages.append(ModelResponse(parts=[TextPart(content=msg.get("content", ""))]))
                
                # Get current prompt (last user message)
                current_prompt = ""
                for msg in reversed(messages):
                    if msg.get("role") == "user":
                        current_prompt = msg.get("content", "")
                        break
                
                # Get message history (all except last message)
                message_history = pydantic_messages[:-1] if len(pydantic_messages) > 1 else []
                return current_prompt, message_history
            
            # Handle Q&A format (from A2A conversion): list of dicts with 'question' and 'answer' keys
            elif isinstance(messages[0], dict) and "question" in messages[0]:
                # Convert Q&A format to role/content format for processing
                role_content_messages = []
                for qa_msg in messages:
                    question = qa_msg.get("question", "")
                    answer = qa_msg.get("answer", "")
                    
                    # Add user message (question)
                    if question:
                        role_content_messages.append({"role": "user", "content": str(question)})
                    
                    # Add assistant message (answer) if present
                    if answer:
                        role_content_messages.append({"role": "assistant", "content": str(answer)})
                
                # Now process as role/content format
                if len(role_content_messages) > 0:
                    pydantic_messages = []
                    for msg in role_content_messages:
                        if msg.get("role") == "user":
                            pydantic_messages.append(ModelRequest.user_text_prompt(msg.get("content", "")))
                        elif msg.get("role") == "assistant":
                            pydantic_messages.append(ModelResponse(parts=[TextPart(content=msg.get("content", ""))]))
                    
                    # Get current prompt (last user message)
                    current_prompt = ""
                    for msg in reversed(role_content_messages):
                        if msg.get("role") == "user":
                            current_prompt = msg.get("content", "")
                            break
                    
                    # Get message history (all except last message)
                    message_history = pydantic_messages[:-1] if len(pydantic_messages) > 1 else []
                    return current_prompt, message_history
        
        # Handle DataFrame format
        df = messages if isinstance(messages, pd.DataFrame) else pd.DataFrame(messages)
        df = df.reset_index(drop=True)
        
        # Check if DataFrame has 'role' and 'content' columns (API format)
        if "role" in df.columns and "content" in df.columns:
            # Convert to Pydantic AI Message objects
            pydantic_messages = []
            for _, row in df.iterrows():
                role = row.get("role")
                content = row.get("content", "")
                if pd.notna(role) and pd.notna(content):
                    if role == "user":
                        pydantic_messages.append(ModelRequest.user_text_prompt(str(content)))
                    elif role == "assistant":
                        pydantic_messages.append(ModelResponse(parts=[TextPart(content=str(content))]))
            
            # Get current prompt (last user message)
            current_prompt = ""
            for _, row in reversed(df.iterrows()):
                if row.get("role") == "user":
                    current_prompt = str(row.get("content", ""))
                    break
            
            # Get message history (all except last message)
            message_history = pydantic_messages[:-1] if len(pydantic_messages) > 1 else []
            return current_prompt, message_history
        
        # Legacy DataFrame format with question/answer columns
        user_column = args.get("user_column", USER_COLUMN)
        current_prompt = ""
        if len(df) > 0 and user_column in df.columns:
            user_messages = df[user_column].dropna()
            if len(user_messages) > 0:
                current_prompt = str(user_messages.iloc[-1])
        
        # Convert history (all except last)
        history_df = df[:-1] if len(df) > 1 else pd.DataFrame()
        message_history = self._convert_messages_to_history(history_df)
        return current_prompt, message_history
    
    def get_metadata(self) -> Dict:
        """Get metadata for observability"""
        return {
            
            "model_name": self.args["model_name"],
            "user_id": ctx.user_id,
            "session_id": ctx.session_id,
            "company_id": ctx.company_id,
            "user_class": ctx.user_class,
            "email_confirmed": ctx.email_confirmed,
        }
    
    def get_tags(self) -> List:
        """Get tags for observability"""
        return ['AGENT', 'PYDANTIC_AI']
    
    
    def get_select_targets_from_sql(self) -> Optional[List[str]]:
        """
        Get the SELECT targets from the original SQL query if available.
        Extracts only the column names, ignoring aliases (e.g., "col1 as alias" -> "col1").
        
        Returns:
            List of SELECT target column names if available, None otherwise
        """
        if self._sql_context and 'select_targets' in self._sql_context:
            select_targets = self._sql_context['select_targets']
            
            # Handle string format: split by commas
            if isinstance(select_targets, str):
                parts = select_targets.split(',')
            elif isinstance(select_targets, list):
                parts = select_targets
            else:
                return None
            
            # Extract only the first word from each part (before "as" or additional text)
            cleaned_columns = []
            for part in parts:
                if isinstance(part, str):
                    # Strip whitespace
                    part = part.strip()
                    # Split by "as" (case-insensitive) and take the first part
                    if ' as ' in part.lower():
                        part = part.split(' as ', 1)[0]
                    elif ' AS ' in part:
                        part = part.split(' AS ', 1)[0]
                    # Take only the first word (in case of other patterns)
                    first_word = part.split()[0] if part.split() else part
                    cleaned_columns.append(first_word.strip())
                else:
                    # If not a string, convert to string and take first word
                    part_str = str(part).strip()
                    first_word = part_str.split()[0] if part_str.split() else part_str
                    cleaned_columns.append(first_word.strip())
            if cleaned_columns == ['*']:
                return ['question', 'answer']
            return cleaned_columns if cleaned_columns else None
        return None
    
    def get_completion(self, messages, stream: bool = False, params: dict | None = None):
        """
        Get completion from agent.
        
        Args:
            messages: List of message dictionaries or DataFrame
            stream: Whether to stream the response
            params: Additional parameters
            
        Returns:
            DataFrame with assistant response
        """
        # Extract SQL context from params if present
        if params and '_sql_context' in params:
            self._sql_context = params.pop('_sql_context')
        else:
            self._sql_context = None
        
        if stream:
            return self._get_completion_stream(messages)
        else:
            for message in self._get_completion_stream(messages):
                if message.get("type") == "end":
                    break
                elif message.get("type") == "error":
                    error_message = f"Agent failed with model error: {message.get('content')}"
                    raise RuntimeError(error_message)
                last_message = message
            
                # if last_message.get("type") == "sql":
                #     sql_query = last_message.get("content")
                    
                if last_message.get("type") == "data":
                    data = last_message.get("content")
                    

            else:
                error_message = f"Agent failed with model error: {last_message.get('content')}"
                return self._create_error_response(error_message, return_context=self.args.get("return_context", True))
            
    
            # Extract the current prompt and message history
            # current_prompt, message_history = self._extract_current_prompt_and_history(messages, self.args)
            # table_markdown = dataframe_to_markdown(data)
            
            # Validate select targets if specified
            select_targets = self.get_select_targets_from_sql()
            if select_targets is not None:
                # Skip validation if '*' is in the targets
                if '*' not in select_targets:
                    # Ensure all expected columns are present
                    if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                        # Create DataFrame with one row of nulls for all expected columns
                        data = pd.DataFrame({col: [None] for col in select_targets})
                    else:
                        # Ensure all expected columns exist, add missing ones with null values
                        for col in select_targets:
                            if col not in data.columns:
                                data[col] = None
                        # Reorder columns to match select_targets order
                        data = data[select_targets]
            elif data is None or (isinstance(data, pd.DataFrame) and data.empty):
                # No select_targets specified, but data is empty - create a simple row
                if isinstance(data, pd.DataFrame) and len(data.columns) > 0:
                    # Use existing columns from empty DataFrame
                    data = pd.DataFrame({col: [None] for col in data.columns})
                else:
                    # Fallback: create a simple DataFrame with a single null row
                    data = pd.DataFrame([None], columns=["result"])
            
            return data        
           
        
        
    def _create_error_response(self, error_message: str, return_context: bool = True) -> pd.DataFrame:
        """Create error response DataFrame"""
        response_data = {
            ASSISTANT_COLUMN: [error_message],
            TRACE_ID_COLUMN: [self.langfuse_client_wrapper.get_trace_id()],
        }
        if return_context:
            response_data[CONTEXT_COLUMN] = [json.dumps([])]
        return pd.DataFrame(response_data)
    
    def _create_cannot_solve_response(self, select_targets_str: Optional[str] = None) -> pd.DataFrame:
        """
        Create a DataFrame indicating the problem cannot be solved.
        
        Args:
            select_targets_str: Comma-separated string of expected column names, or None
            
        Returns:
            DataFrame with select_targets columns (if provided) plus 'thoughts' column
        """
        thoughts_message = "cannot solve this problem with the data we have, provide more context"
        
        if select_targets_str is None:
            # No specific columns expected - return simple DataFrame with just thoughts
            return pd.DataFrame({"thoughts": [thoughts_message]})
        
        # Parse select_targets_str to get column names
        column_names = [col.strip() for col in select_targets_str.split(",")]
        
        # Create DataFrame with expected columns (all None) plus thoughts column
        response_data = {col: [None] for col in column_names}
        response_data["thoughts"] = [thoughts_message]
        
        return pd.DataFrame(response_data)
    
    def _get_completion_stream(self, messages: List[dict]) -> Iterable[Dict]:
        """
        Get completion as a stream of chunks.
        
        Args:
            messages: List of message dictionaries or DataFrame
            
        Returns:
            Iterator of chunk dictionaries
        """
        
        # Set up trace
        metadata = self.get_metadata()
        tags = self.get_tags()
        
        self.langfuse_client_wrapper.setup_trace(
            name="api-completion",
            input=messages,
            tags=tags,
            metadata=metadata,
            user_id=ctx.user_id,
            session_id=ctx.session_id,
        )
        
        
        self.run_completion_span = self.langfuse_client_wrapper.start_span(
            name="run-completion", 
            input=messages
        )
        
        DEBUG_LOGGER(f"PydanticAIAgent._get_completion_stream: Messages: {messages}")
        
        # Extract current prompt and message history from messages
        # This handles multiple formats: list of dicts, DataFrame with role/content, or legacy DataFrame
        current_prompt, message_history = self._extract_current_prompt_and_history(messages, self.args)
        DEBUG_LOGGER(f"PydanticAIAgent._get_completion_stream: Extracted prompt and {len(message_history)} history messages")
        
        # Create agent
        agent = Agent(
            self.model_instance,
            system_prompt=self.system_prompt,
            output_type=SQLQuery
        )
        
        DEBUG_LOGGER(f"PydanticAIAgent._get_completion_stream: SQL context: {self._sql_context}")
        
        yield self._add_chunk_metadata({"type": "status", "content": "Generating Data Catalog..."})
        tables_list = self.agent.params.get("data", {}).get("tables", [])
        knowledge_bases_list = self.agent.params.get("data", {}).get("knowledge_bases", [])
        sql_instructions = ''
        if knowledge_bases_list:
            sql_instructions = f"{agent_prompts.sql_description}\n\n{agent_prompts.sql_with_kb_description}"
        else:
            sql_instructions = agent_prompts.sql_description

        data_catalog = DataCatalogBuilder().build_data_catalog(tables=tables_list, knowledge_bases=knowledge_bases_list)
        
        # Initialize counters and accumulators
        exploratory_query_count = 0
        MAX_EXPLORATORY_QUERIES = 20
        MAX_RETRIES = 3
        accumulated_errors = []
        exploratory_query_results = []
        
        # Planning step: Create a plan before generating queries
        yield self._add_chunk_metadata({"type": "status", "content": "Creating execution plan..."})
        
        # Create planning agent
        planning_agent = Agent(
            self.model_instance,
            system_prompt=self.system_prompt,
            output_type=Plan
        )
        
        # Build planning prompt
        planning_prompt_text = f"""Take into account the following Data Catalog:\n{data_catalog}\n\n{agent_prompts.planning_prompt}\n\nQuestion to answer: {current_prompt}"""
        DEBUG_LOGGER(f"PydanticAIAgent._get_completion_stream: Planning prompt text: {planning_prompt_text}")
        # Get select targets for planning context
        select_targets = self.get_select_targets_from_sql()
        select_targets_str = None
        if select_targets is not None:
            if isinstance(select_targets, (list, tuple)):
                select_targets_str = ", ".join(str(t) for t in select_targets)
            else:
                select_targets_str = str(select_targets)
            planning_prompt_text += f"\n\nFor the final query, the user expects to have a table such that this query is valid: SELECT {select_targets_str} FROM (<generated query>); when creating your plan, make sure to account for these expected columns."
        
        # Generate plan
        plan_result = planning_agent.run_sync(planning_prompt_text)
        plan = plan_result.output
        # Validate plan steps don't exceed MAX_EXPLORATORY_QUERIES
        if plan.estimated_steps > MAX_EXPLORATORY_QUERIES:
            logger.warning(f"Plan estimated {plan.estimated_steps} steps, but maximum is {MAX_EXPLORATORY_QUERIES}. Adjusting plan.")
            plan.plan += f"\n\nNote: The plan has been adjusted to ensure it does not exceed {MAX_EXPLORATORY_QUERIES} steps."
        
        DEBUG_LOGGER(f"Generated plan with {plan.estimated_steps} estimated steps: {plan.plan}")
        
        # Yield the plan as a status message
        yield self._add_chunk_metadata({"type": "status", "content": f"Proposed Execution Plan:\n{plan.plan}\n\nEstimated steps: {plan.estimated_steps}\n\n"})
        
        # Build base prompt with plan included
        base_prompt = f"\n\nTake into account the following Data Catalog:\n{data_catalog}\nMindsDB SQL instructions:\n{sql_instructions}\n\nProposedExecution Plan:\n{plan.plan}\n\nEstimated steps: {plan.estimated_steps} (maximum allowed: {MAX_EXPLORATORY_QUERIES})\n\nPlease follow this plan and write Mindsdb SQL queries to answer the question:\n{current_prompt}"
        
        if select_targets_str is not None:
            base_prompt += f"\n\nFor the final query the user expects to have a table such that this query is valid:SELECT {select_targets_str} FROM (<generated query>); when generating the SQL query make sure to include those columns, do not fix grammar on columns. Keep them as the user wants them"
        
        current_prompt = base_prompt

        DEBUG_LOGGER(f"PydanticAIAgent._get_completion_stream: Sending LLM request with Current prompt: {current_prompt}")
        DEBUG_LOGGER(f"PydanticAIAgent._get_completion_stream: Message history: {message_history}")

        try:
            while True:
                # Check if we've reached max exploratory queries
                if exploratory_query_count >= MAX_EXPLORATORY_QUERIES:
                    # Add warning that next query must be final
                    current_prompt += f"\n\nIMPORTANT: You have reached the maximum number of exploratory queries ({MAX_EXPLORATORY_QUERIES}). The next query you generate MUST be a final_query (not exploratory_query). If you cannot solve the problem with the data available, respond with a final_query that returns a table with the expected columns plus a 'thoughts' column containing: 'cannot solve this problem with the data we have, provide more context'."

                yield self._add_chunk_metadata({"type": "status", "content": "Generating SQL query..."})
                result = agent.run_sync(
                    current_prompt,
                    message_history=message_history if message_history else None,
                )
            
                # Extract output
                output = result.output 
                yield self._add_chunk_metadata({"type": "sql", "content": output.sql_query})

                DEBUG_LOGGER(f"PydanticAIAgent._get_completion_stream: Received LLM response: {output.sql_query}, query_type: {output.query_type}")

                # Initialize retry counter for this query
                retry_count = 0
                query_succeeded = False
                query_data = None
                query_error = None

                # Retry loop for this query (up to MAX_RETRIES)
                while retry_count <= MAX_RETRIES:
                    try:
                        yield self._add_chunk_metadata({"type": "status", "content": "Executing SQL query..."})

                        query_data = self.executor.execute(output.sql_query)
                        DEBUG_LOGGER("PydanticAIAgent._get_completion_stream: Executed SQL query successfully")
                        query_succeeded = True
                        break  # Query succeeded, exit retry loop

                    except Exception as e:
                        # Unexpected error - only log essential error information
                        query_error = f"Error executing SQL query: {str(e)}"
                        logger.error(f"Unexpected error executing SQL query (retry {retry_count}/{MAX_RETRIES}): Query: {output.sql_query[:100]}... Error: {str(e)}")
                        
                        if retry_count < MAX_RETRIES:
                            accumulated_errors.append(f"Query: {output.sql_query}\nError: {query_error}")
                            retry_count += 1
                            
                            error_context = "\n\nPrevious query errors:\n" + "\n---\n".join(accumulated_errors[-3:])
                            current_prompt = base_prompt
                            if exploratory_query_results:
                                current_prompt += "\n\nPrevious exploratory query results:\n" + "\n---\n".join(exploratory_query_results)
                            current_prompt += error_context
                            current_prompt += f"\n\nPlease fix the query and try again. This is retry attempt {retry_count} of {MAX_RETRIES}."
                            
                            if exploratory_query_count >= MAX_EXPLORATORY_QUERIES:
                                current_prompt += f"\n\nIMPORTANT: You have reached the maximum number of exploratory queries ({MAX_EXPLORATORY_QUERIES}). The next query you generate MUST be a final_query."
                            
                            yield self._add_chunk_metadata({"type": "status", "content": f"Retrying query (attempt {retry_count}/{MAX_RETRIES})..."})
                            result = agent.run_sync(
                                current_prompt,
                                message_history=message_history if message_history else None,
                            )
                            output = result.output
                            yield self._add_chunk_metadata({"type": "sql", "content": output.sql_query})
                            DEBUG_LOGGER(f"PydanticAIAgent._get_completion_stream: Retry {retry_count} - Received LLM response: {output.sql_query}")
                        else:
                            break

                # Handle query result
                if not query_succeeded:
                    # Query failed after all retries
                    error_message = query_error or "Query failed after maximum retries"
                    logger.error(f"Query failed after {MAX_RETRIES} retries. Query: {output.sql_query[:100]}... Error: {error_message}")
                    
                    # If we've exhausted retries and reached max exploratory queries, return "cannot solve" response
                    if exploratory_query_count >= MAX_EXPLORATORY_QUERIES:
                        # Create "cannot solve" response DataFrame
                        cannot_solve_df = self._create_cannot_solve_response(select_targets_str)
                        yield self._add_chunk_metadata({
                            "type": "data",
                            "content": cannot_solve_df
                        })
                        yield self._add_chunk_metadata({"type": "end"})
                        self.langfuse_client_wrapper.end_span_stream(span=self.run_completion_span)
                        return
                    
                    # Otherwise, yield error and continue (might generate new query)
                    yield self._add_chunk_metadata({
                        "type": "error",
                        "content": error_message,
                    })
                    # Add error to accumulated errors for next iteration
                    accumulated_errors.append(f"Query: {output.sql_query}\nError: {error_message}")
                    # Update prompt with errors for next query
                    error_context = "\n\nPrevious query errors:\n" + "\n---\n".join(accumulated_errors[-3:])
                    current_prompt = base_prompt
                    if exploratory_query_results:
                        current_prompt += "\n\nPrevious exploratory query results:\n" + "\n---\n".join(exploratory_query_results)
                    current_prompt += error_context

                    continue  # Continue to next iteration to generate new query

                # Query succeeded
                if not query_succeeded or query_data is None:
                    # This shouldn't happen, but handle it gracefully
                    logger.error("Query marked as succeeded but query_data is None")
                    yield self._add_chunk_metadata({
                        "type": "error",
                        "content": "Query execution succeeded but no data was returned",
                    })
                    continue
                
                if output.query_type == QueryType.EXPLORATORY:
                    # Check if we've already exceeded max exploratory queries
                    if exploratory_query_count >= MAX_EXPLORATORY_QUERIES:
                        # We've already reached max, but agent generated another exploratory query
                        # Convert to "cannot solve" response
                        logger.warning(f"Agent generated exploratory query after reaching maximum ({MAX_EXPLORATORY_QUERIES}), returning cannot solve response")
                        cannot_solve_df = self._create_cannot_solve_response(select_targets_str)
                        yield self._add_chunk_metadata({
                            "type": "data",
                            "content": cannot_solve_df
                        })
                        yield self._add_chunk_metadata({"type": "end"})
                        self.langfuse_client_wrapper.end_span_stream(span=self.run_completion_span)
                        return
                    
                    # This is an exploratory query
                    exploratory_query_count += 1
                    DEBUG_LOGGER(f"Exploratory query {exploratory_query_count}/{MAX_EXPLORATORY_QUERIES} succeeded")
                    
                    # Format query result for prompt
                    query_result_str = f"Query: {output.sql_query}\nDescription: {output.short_description}\nResult:\n{dataframe_to_markdown(query_data)}"
                    exploratory_query_results.append(query_result_str)
                    
                    # Update prompt with exploratory results for next iteration
                    current_prompt = base_prompt
                    current_prompt += "\n\nPrevious exploratory query results:\n" + "\n---\n".join(exploratory_query_results[-5:])  # Show last 5 results
                    
                    if accumulated_errors:
                        error_context = "\n\nPrevious query errors (now resolved):\n" + "\n---\n".join(accumulated_errors[-3:])
                        current_prompt += error_context
                    
                    if exploratory_query_count >= MAX_EXPLORATORY_QUERIES:
                        current_prompt += f"\n\nIMPORTANT: You have reached the maximum number of exploratory queries ({MAX_EXPLORATORY_QUERIES}). The next query you generate MUST be a final_query (not exploratory_query). If you cannot solve the problem with the data available, respond with a final_query that returns a table with the expected columns plus a 'thoughts' column containing: 'cannot solve this problem with the data we have, provide more context'."
                    
                    # Reset retry counter for next query
                    retry_count = 0
                    accumulated_errors = []  # Clear errors after successful query
                    
                    # Continue loop to generate next query
                    continue
                    
                elif output.query_type == QueryType.FINAL:
                    # This is a final query - return the result
                    DEBUG_LOGGER("Final query succeeded, returning result")
                    yield self._add_chunk_metadata({
                        "type": "data",
                        "content": query_data
                    })
                    yield self._add_chunk_metadata({"type": "end"})
                    self.langfuse_client_wrapper.end_span_stream(span=self.run_completion_span)
                    return
                else:
                    # Unknown query type - treat as final
                    logger.warning(f"Unknown query type: {output.query_type}, treating as final")
                    yield self._add_chunk_metadata({
                        "type": "data",
                        "content": query_data
                    })
                    yield self._add_chunk_metadata({"type": "end"})
                    self.langfuse_client_wrapper.end_span_stream(span=self.run_completion_span)
                    return
       
        except Exception as e:
            # Suppress the "Event loop is closed" error from httpx cleanup
            # This is a known issue where async HTTP clients try to close after the event loop is closed
            error_msg = str(e)
            if "Event loop is closed" in error_msg:
                # This is a cleanup issue, not a critical error - log at debug level
                DEBUG_LOGGER(f"Async cleanup warning (non-critical): {error_msg}")
            else:
                logger.error(f"Agent streaming failed: {error_msg}")
                error_chunk = self._add_chunk_metadata({
                    "type": "error",
                    "content": f"Agent streaming failed: {error_msg}",
                })
                yield error_chunk
    
    def _add_chunk_metadata(self, chunk: Dict) -> Dict:
        """Add metadata to chunk"""
        chunk["trace_id"] = self.langfuse_client_wrapper.get_trace_id()
        return chunk

