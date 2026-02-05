"""Pydantic AI Agent wrapper to replace LangchainAgent"""

import json
import warnings
from typing import Dict, List, Optional, Any, Iterable
import pandas as pd


from mindsdb_sql_parser import parse_sql, ast

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

from mindsdb.interfaces.agents.utils.sql_toolkit import MindsDBQuery
from mindsdb.interfaces.agents.utils.pydantic_ai_model_factory import get_model_instance_from_kwargs
from mindsdb.interfaces.agents.utils.data_catalog_builder import DataCatalogBuilder, dataframe_to_markdown
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.langfuse import LangfuseClientWrapper
from mindsdb.interfaces.agents.modes import sql as sql_mode, text_sql as text_sql_mode
from mindsdb.interfaces.agents.modes.base import ResponseType, PlanResponse

logger = log.getLogger(__name__)
DEBUG_LOGGER = logger.info


# Suppress asyncio warnings about unretrieved task exceptions from httpx cleanup
# This is a known issue where httpx.AsyncClient tries to close connections after the event loop is closed
warnings.filterwarnings("ignore", message=".*Task exception was never retrieved.*", category=RuntimeWarning)


class PydanticAIAgent:
    """Pydantic AI-based agent to replace LangchainAgent"""

    def __init__(
        self,
        agent: db.Agents,
        llm_params: dict = None,
    ):
        """
        Initialize Pydantic AI agent.

        Args:
            agent: Agent database record
            args: Agent parameters (optional)
            llm_params: LLM parameters (optional)
        """
        self.agent = agent

        self.run_completion_span: Optional[object] = None
        self.llm: Optional[object] = None
        self.embedding_model: Optional[object] = None

        self.log_callback_handler: Optional[object] = None
        self.langfuse_callback_handler: Optional[object] = None
        self.mdb_langfuse_callback_handler: Optional[object] = None

        self.langfuse_client_wrapper = LangfuseClientWrapper()
        self.agent_mode = self.agent.params.get("mode", "text")

        self.llm_params = llm_params

        # Provider model instance
        self.model_instance = get_model_instance_from_kwargs(self.llm_params)

        # Command executor for queries
        tables_list = self.agent.params.get("data", {}).get("tables", [])
        knowledge_bases_list = self.agent.params.get("data", {}).get("knowledge_bases", [])
        self.sql_toolkit = MindsDBQuery(tables_list, knowledge_bases_list)

        self.system_prompt = self.agent.params.get("prompt_template", "You are an expert MindsDB SQL data analyst")

        # Track current query state
        self._current_prompt: Optional[str] = None
        self._current_sql_query: Optional[str] = None
        self._current_query_result: Optional[pd.DataFrame] = None

        self.select_targets = None

    def _convert_messages_to_history(self, df: pd.DataFrame, args: dict) -> List[ModelMessage]:
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
            user_column = args.get("user_column", USER_COLUMN)
            assistant_column = args.get("assistant_column", ASSISTANT_COLUMN)

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
        message_history = self._convert_messages_to_history(history_df, args)
        return current_prompt, message_history

    def get_metadata(self) -> Dict:
        """Get metadata for observability"""
        return {
            "model_name": self.llm_params["model_name"],
            "user_id": ctx.user_id,
            "session_id": ctx.session_id,
            "company_id": ctx.company_id,
            "user_class": ctx.user_class,
        }

    def get_tags(self) -> List:
        """Get tags for observability"""
        return ["AGENT", "PYDANTIC_AI"]

    def get_select_targets_from_sql(self, sql) -> Optional[List[str]]:
        """
        Get the SELECT targets from the original SQL query if available.
        Extracts only the column names, ignoring aliases (e.g., "col1 as alias" -> "col1").

        Returns:
            List of SELECT target column names if available, None otherwise
        """

        try:
            parsed = parse_sql(sql)
        except Exception:
            return

        if not isinstance(parsed, ast.Select):
            return

        targets = []
        for target in parsed.targets:
            if isinstance(target, ast.Identifier):
                targets.append(target.parts[-1])

            elif isinstance(target, ast.Star):
                return  # ['question', 'answer']

            elif isinstance(target, ast.Function):
                # For functions, get the function name and args
                func_str = target.op
                targets.append(func_str)
                if target.args:
                    for arg in target.args:
                        if isinstance(arg, ast.Identifier):
                            targets.append(arg.parts[-1])

        return targets

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
        if params and "original_query" in params:
            original_query = params.pop("original_query")

            self.select_targets = self.get_select_targets_from_sql(original_query)

        args = {}
        args.update(self.agent.params or {})
        args.update(params or {})

        data = None
        if stream:
            return self._get_completion_stream(messages, args)
        else:
            for message in self._get_completion_stream(messages, args):
                if message.get("type") == "end":
                    break
                elif message.get("type") == "error":
                    error_message = f"Agent failed with model error: {message.get('content')}"
                    raise RuntimeError(error_message)
                last_message = message

                # if last_message.get("type") == "sql":
                #     sql_query = last_message.get("content")

                if last_message.get("type") == "data":
                    if "text" in last_message:
                        data = pd.DataFrame([{"answer": last_message["text"]}])
                    else:
                        data = last_message.get("content")

            else:
                error_message = f"Agent failed with model error: {last_message.get('content')}"
                return self._create_error_response(error_message, return_context=params.get("return_context", True))

            # Validate select targets if specified

            if self.select_targets is not None:
                # Ensure all expected columns are present
                if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                    # Create DataFrame with one row of nulls for all expected columns
                    data = pd.DataFrame({col: [None] for col in self.select_targets})
                else:
                    # Ensure all expected columns exist, add missing ones with null values
                    cols_map = {c.lower(): c for c in data.columns}

                    for col in self.select_targets:
                        if col not in data.columns:
                            # try to find case independent
                            if col.lower() in cols_map:
                                data[col] = data[col] = data[cols_map[col.lower()]]
                            else:
                                data[col] = None
                    # Reorder columns to match select_targets order
                    data = data[self.select_targets]

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

    def _get_completion_stream(self, messages: List[dict], params) -> Iterable[Dict]:
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

        self.run_completion_span = self.langfuse_client_wrapper.start_span(name="run-completion", input=messages)

        DEBUG_LOGGER(f"PydanticAIAgent._get_completion_stream: Messages: {messages}")

        # Extract current prompt and message history from messages
        # This handles multiple formats: list of dicts, DataFrame with role/content, or legacy DataFrame
        current_prompt, message_history = self._extract_current_prompt_and_history(messages, params)
        DEBUG_LOGGER(
            f"PydanticAIAgent._get_completion_stream: Extracted prompt and {len(message_history)} history messages"
        )

        yield self._add_chunk_metadata({"type": "status", "content": "Generating Data Catalog..."})

        if self.agent_mode == "text":
            agent_prompts = text_sql_mode
            AgentResponse = text_sql_mode.AgentResponse
        else:
            agent_prompts = sql_mode
            AgentResponse = sql_mode.AgentResponse

        if self.sql_toolkit.knowledge_bases:
            sql_instructions = f"{agent_prompts.sql_description}\n\n{agent_prompts.sql_with_kb_description}"
        else:
            sql_instructions = agent_prompts.sql_description

        data_catalog = DataCatalogBuilder(sql_toolkit=self.sql_toolkit).build_data_catalog()

        # Initialize counters and accumulators
        exploratory_query_count = 0
        MAX_EXPLORATORY_QUERIES = 20
        MAX_RETRIES = 3
        accumulated_errors = []
        exploratory_query_results = []

        # Planning step: Create a plan before generating queries
        yield self._add_chunk_metadata({"type": "status", "content": "Creating execution plan..."})

        # Create planning agent
        planning_agent = Agent(self.model_instance, system_prompt=self.system_prompt, output_type=PlanResponse)

        # Build planning prompt
        planning_prompt_text = f"""Take into account the following Data Catalog:\n{data_catalog}\n\n{agent_prompts.planning_prompt}\n\nQuestion to answer: {current_prompt}"""
        DEBUG_LOGGER(f"PydanticAIAgent._get_completion_stream: Planning prompt text: {planning_prompt_text}")
        # Get select targets for planning context

        select_targets_str = None
        if self.select_targets is not None:
            select_targets_str = ", ".join(str(t) for t in self.select_targets)
            planning_prompt_text += f"\n\nFor the final query, the user expects to have a table such that this query is valid: SELECT {select_targets_str} FROM (<generated query>); when creating your plan, make sure to account for these expected columns."

        # Generate plan
        plan_result = planning_agent.run_sync(planning_prompt_text)
        plan = plan_result.output
        # Validate plan steps don't exceed MAX_EXPLORATORY_QUERIES
        if plan.estimated_steps > MAX_EXPLORATORY_QUERIES:
            logger.warning(
                f"Plan estimated {plan.estimated_steps} steps, but maximum is {MAX_EXPLORATORY_QUERIES}. Adjusting plan."
            )
            plan.plan += (
                f"\n\nNote: The plan has been adjusted to ensure it does not exceed {MAX_EXPLORATORY_QUERIES} steps."
            )

        DEBUG_LOGGER(f"Generated plan with {plan.estimated_steps} estimated steps: {plan.plan}")

        # Yield the plan as a status message
        yield self._add_chunk_metadata(
            {
                "type": "status",
                "content": f"Proposed Execution Plan:\n{plan.plan}\n\nEstimated steps: {plan.estimated_steps}\n\n",
            }
        )

        # Build base prompt with plan included
        base_prompt = f"\n\nTake into account the following Data Catalog:\n{data_catalog}\nMindsDB SQL instructions:\n{sql_instructions}\n\nProposedExecution Plan:\n{plan.plan}\n\nEstimated steps: {plan.estimated_steps} (maximum allowed: {MAX_EXPLORATORY_QUERIES})\n\nPlease follow this plan and write Mindsdb SQL queries to answer the question:\n{current_prompt}"

        if select_targets_str is not None:
            base_prompt += f"\n\nFor the final query the user expects to have a table such that this query is valid:SELECT {select_targets_str} FROM (<generated query>); when generating the SQL query make sure to include those columns, do not fix grammar on columns. Keep them as the user wants them"

        DEBUG_LOGGER(
            f"PydanticAIAgent._get_completion_stream: Sending LLM request with Current prompt: {current_prompt}"
        )
        DEBUG_LOGGER(f"PydanticAIAgent._get_completion_stream: Message history: {message_history}")

        # Create agent
        agent = Agent(self.model_instance, system_prompt=self.system_prompt, output_type=AgentResponse)

        error_context = None
        retry_count = 0

        try:
            while True:
                yield self._add_chunk_metadata({"type": "status", "content": "Generating agent response..."})

                current_prompt = base_prompt
                if exploratory_query_results:
                    current_prompt += "\n\nPrevious exploratory query results:\n" + "\n---\n".join(
                        exploratory_query_results
                    )

                if exploratory_query_count >= MAX_EXPLORATORY_QUERIES:
                    current_prompt += f"\n\nIMPORTANT: You have reached the maximum number of exploratory queries ({MAX_EXPLORATORY_QUERIES}). The next query you generate MUST be a final_query or final_text."

                if error_context:
                    current_prompt += error_context
                    current_prompt += (
                        f"\n\nPlease fix the query and try again. This is retry attempt {retry_count} of {MAX_RETRIES}."
                    )

                result = agent.run_sync(
                    current_prompt,
                    message_history=message_history if message_history else None,
                )

                # Extract output
                output = result.output

                # Yield description before SQL query
                if output.short_description:
                    yield self._add_chunk_metadata({"type": "context", "content": output.short_description})

                if output.type == ResponseType.FINAL_TEXT:
                    yield self._add_chunk_metadata({"type": "status", "content": "Returning text response"})

                    # return text to user and exit
                    yield self._add_chunk_metadata({"type": "data", "text": output.text})
                    yield self._add_chunk_metadata({"type": "end"})
                    return

                sql_query = output.sql_query
                DEBUG_LOGGER(
                    f"PydanticAIAgent._get_completion_stream: Received LLM response: sql: {sql_query}, query_type: {output.type}, description: {output.short_description}"
                )

                # Initialize retry counter for this query

                # Retry loop for this query (up to MAX_RETRIES)

                error_context = None

                try:
                    query_type = "final" if output.type == ResponseType.FINAL_QUERY else "exploratory"
                    yield self._add_chunk_metadata(
                        {"type": "status", "content": f"Executing {query_type} SQL query: {sql_query}"}
                    )
                    query_data = self.sql_toolkit.execute_sql(sql_query)

                except Exception as e:
                    # Extract error message - prefer db_error_msg for QueryError, otherwise use str(e)
                    query_error = str(e)

                    # Yield descriptive error message
                    error_message = f"Error executing SQL query: {query_error}"

                    yield self._add_chunk_metadata({"type": "status", "content": error_message})

                    accumulated_errors.append(f"Query: {sql_query}\nError: {query_error}")
                    retry_count += 1
                    if retry_count >= MAX_RETRIES:
                        error_context = "\n\nPrevious query errors:\n" + "\n---\n".join(accumulated_errors[-3:])
                        if output.type == ResponseType.FINAL_QUERY:
                            raise RuntimeError(f"Problem with final query: {query_error}")
                    else:
                        query_result_str = f"Query: {sql_query}\nError: {query_error}"
                        exploratory_query_results.append(query_result_str)

                    continue

                DEBUG_LOGGER("PydanticAIAgent._get_completion_stream: Executed SQL query successfully")
                retry_count = 0

                if output.type == ResponseType.FINAL_QUERY:
                    # return response to user
                    yield self._add_chunk_metadata({"type": "data", "content": query_data})
                    yield self._add_chunk_metadata({"type": "end"})
                    return

                # is exploratory
                exploratory_query_count += 1
                debug_message = f"Exploratory query {exploratory_query_count}/{MAX_EXPLORATORY_QUERIES} succeeded"
                DEBUG_LOGGER(debug_message)
                yield self._add_chunk_metadata({"type": "status", "content": debug_message})

                # Format query result for prompt
                markdown_table = dataframe_to_markdown(query_data)
                query_result_str = (
                    f"Query: {sql_query}\nDescription: {output.short_description}\nResult:\n{markdown_table}"
                )
                yield self._add_chunk_metadata({"type": "status", "content": f"Query result: {markdown_table}"})
                exploratory_query_results.append(query_result_str)

        except Exception as e:
            # Suppress the "Event loop is closed" error from httpx cleanup
            # This is a known issue where async HTTP clients try to close after the event loop is closed
            error_msg = str(e)
            if "Event loop is closed" in error_msg:
                # This is a cleanup issue, not a critical error - log at debug level
                DEBUG_LOGGER(f"Async cleanup warning (non-critical): {error_msg}")
            else:
                # Extract error message - prefer db_error_msg for QueryError, otherwise use str(e)
                from mindsdb.utilities.exception import QueryError

                if isinstance(e, QueryError):
                    error_content = e.db_error_msg or str(e)
                    descriptive_error = f"Database query error: {error_content}"
                    if e.failed_query:
                        descriptive_error += f"\n\nFailed query: {e.failed_query}"
                else:
                    error_content = error_msg
                    descriptive_error = f"Agent streaming failed: {error_content}"

                logger.error(f"Agent streaming failed: {error_content}")
                error_chunk = self._add_chunk_metadata(
                    {
                        "type": "error",
                        "content": descriptive_error,
                    }
                )
                yield error_chunk

    def _add_chunk_metadata(self, chunk: Dict) -> Dict:
        """Add metadata to chunk"""
        chunk["trace_id"] = self.langfuse_client_wrapper.get_trace_id()
        return chunk
