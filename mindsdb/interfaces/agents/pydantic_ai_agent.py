"""Pydantic AI Agent wrapper to replace LangchainAgent"""

import json
import asyncio
from typing import Dict, List, Optional, Any, Iterable
import pandas as pd
import logging

from pydantic_ai import Agent, RunContext
from pydantic_ai.exceptions import UnexpectedModelBehavior, ModelRetry
from pydantic_ai.messages import ModelRequest, ModelResponse, ModelMessage, TextPart

from mindsdb.utilities import log
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.agents.constants import (
    USER_COLUMN,
    ASSISTANT_COLUMN,
    CONTEXT_COLUMN,
    TRACE_ID_COLUMN,
    DEFAULT_AGENT_TIMEOUT_SECONDS,
)

from mindsdb.interfaces.agents.utils.sql_toolkit import MindsDBQuery, SQLQuery
from mindsdb.interfaces.agents.utils.pydantic_ai_model_factory import (
    get_model_instance_from_kwargs
)

from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseController
from mindsdb.api.executor.command_executor import ExecuteCommands
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.langfuse import LangfuseClientWrapper
from mindsdb.interfaces.agents.prompts import agent_prompts
logger = log.getLogger(__name__)

from mindsdb.interfaces.agents.utils.data_catalog_builder import DataCatalogBuilder, dataframe_to_markdown

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
            
                if last_message.get("type") == "sql":
                    sql_query = last_message.get("content")
                    
                if last_message.get("type") == "data":
                    data = last_message.get("content")
                    

            else:
                error_message = f"Agent failed with model error: {last_message.get('content')}"
                return self._create_error_response(error_message, return_context=self.args.get("return_context", True))
            
    
            # Extract the current prompt and message history
            current_prompt, message_history = self._extract_current_prompt_and_history(messages, self.args)
            table_markdown = dataframe_to_markdown(data)
            
            # Validate select targets if specified
            select_targets = self.get_select_targets_from_sql()
            if select_targets is not None:
                # Skip validation if '*' is in the targets
                if '*' not in select_targets:
                    return data
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
        
        # Extract current prompt and message history from messages
        # This handles multiple formats: list of dicts, DataFrame with role/content, or legacy DataFrame
        current_prompt, message_history = self._extract_current_prompt_and_history(messages, self.args)
        logger.info(f"PydanticAIAgent._get_completion_stream: Extracted prompt and {len(message_history)} history messages")
        
        # Create agent
        agent = Agent(
            self.model_instance,
            system_prompt=self.system_prompt,
            output_type=SQLQuery
        )
        
        logger.info(f"PydanticAIAgent._get_completion_stream: SQL context: {self._sql_context}")
        
        yield self._add_chunk_metadata({"type": "status", "content": "Generating Data Catalog..."})
        tables_list = self.agent.params.get("data", {}).get("tables", [])
        knowledge_bases_list = self.agent.params.get("data", {}).get("knowledge_bases", [])
        data_catalog = DataCatalogBuilder().build_data_catalog(tables=tables_list, knowledge_bases=knowledge_bases_list)
        current_prompt = f"\n\nTake into account the following Data Catalog:\n{data_catalog}\nMindsDB SQL instructions:\n{agent_prompts.sql_description}\n\nPlease write a Mindsdb SQL query to answer the question:\n{current_prompt}"

        if self.get_select_targets_from_sql() is not None:
            select_targets = self.get_select_targets_from_sql()
            if isinstance(select_targets, (list, tuple)):
                select_targets_str = ", ".join(str(t) for t in select_targets)
            else:
                select_targets_str = str(select_targets)
            current_prompt += f"\n\nThe user expects to have a table such that this query is valid:SELECT {select_targets_str} FROM (<generated query>); when generating the SQL query make sure to include those columns"

        logger.info(f"PydanticAIAgent._get_completion_stream: Sending LLM request with Current prompt: {current_prompt}")

        try:
            yield self._add_chunk_metadata({"type": "status", "content": "Generating SQL query..."})
            result = agent.run_sync(
                current_prompt,
                message_history=message_history if message_history else None,
            )
                
            # Extract output
            output = result.output 
            yield self._add_chunk_metadata({"type": "sql", "content": output.sql_query})

            logger.info(f"PydanticAIAgent._get_completion_stream: Received LLM response: {output.sql_query}")

            try:

                yield self._add_chunk_metadata({"type": "status", "content": "Executing SQL query..."})

                data = self.executor.execute(output.sql_query)
                logger.info(f"PydanticAIAgent._get_completion_stream: Executed SQL query: {data}")

                yield self._add_chunk_metadata({
                    "type": "data",
                    "content": data
                })
            except Exception as e:
                logger.error(f"Error executing SQL query: {e}", exc_info=True)
                yield self._add_chunk_metadata({
                    "type": "error",
                    "content": f"Error executing SQL query: {str(e)}",
                })

            # Yield end chunk
            yield self._add_chunk_metadata({"type": "end"})
            
            self.langfuse_client_wrapper.end_span_stream(span=self.run_completion_span)

            
       
        except Exception as e:
            logger.error(f"Streaming error: {e}", exc_info=True)
            error_chunk = self._add_chunk_metadata({
                "type": "error",
                "content": f"Agent streaming failed: {str(e)}",
            })
            yield error_chunk
    
    def _add_chunk_metadata(self, chunk: Dict) -> Dict:
        """Add metadata to chunk"""
        chunk["trace_id"] = self.langfuse_client_wrapper.get_trace_id()
        return chunk

