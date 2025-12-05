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
from mindsdb.interfaces.agents.pydantic_ai_model_factory import (
    create_pydantic_ai_model,
    get_pydantic_ai_model_kwargs,
)
from mindsdb.interfaces.agents.pydantic_ai_tools import build_tools_from_agent_config
from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseController
from mindsdb.api.executor.command_executor import ExecuteCommands
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.langfuse import LangfuseClientWrapper

logger = log.getLogger(__name__)


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
        
        # Provider for compatibility
        self.provider = self.args.get("provider", self._get_llm_provider(self.args))
        
        # Pydantic AI agent instance (created lazily)
        self._pydantic_agent: Optional[Agent] = None
        
        # Command executor for tools
        self._command_executor: Optional[Any] = None
        self._kb_controller: Optional[KnowledgeBaseController] = None
    
    def _get_llm_provider(self, args: Dict) -> str:
        """Get LLM provider from args"""
        from mindsdb.interfaces.agents.pydantic_ai_model_factory import get_llm_provider
        return get_llm_provider(args)
    
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
        
        args["embedding_model_provider"] = args.get(
            "embedding_model", 
            self._get_embedding_model_provider(args)
        )
        
        # Handle MindsDB provider
        if self.agent.provider == "mindsdb":
            args["model_name"] = self.agent.model_name
            prompt_template = self.model.get("problem_definition", {}).get("using", {}).get("prompt_template")
            if prompt_template is not None:
                args["prompt_template"] = prompt_template
        
        # Set default prompt template if not provided
        if args.get("prompt_template") is None:
            default_prompt = "you are an assistant, answer using the tables connected"
            args["prompt_template"] = default_prompt
            logger.info(f"Using default prompt template: {default_prompt}")
        
        if "model_name" not in args:
            raise ValueError(
                "No model name provided for agent. Provide it in the model parameter or in the default model setup."
            )
        
        return args
    
    def _get_embedding_model_provider(self, args: Dict) -> str:
        """Get embedding model provider from args"""
        from mindsdb.interfaces.agents.pydantic_ai_model_factory import get_embedding_model_provider
        return get_embedding_model_provider(args)
    
    def _get_command_executor(self):
        """Get or create command executor"""
        if self._command_executor is None:
            from mindsdb.api.executor.command_executor import ExecuteCommands
            from mindsdb.api.executor.controllers import SessionController
            session = SessionController()
            self._command_executor = ExecuteCommands(session)
        return self._command_executor
    
    def _get_kb_controller(self):
        """Get or create knowledge base controller"""
        if self._kb_controller is None:
            from mindsdb.api.executor.controllers import SessionController
            session = SessionController()
            self._kb_controller = KnowledgeBaseController(session)
        return self._kb_controller
    
    def _create_pydantic_agent(self) -> Agent:
        """Create and configure Pydantic AI agent"""
        if self._pydantic_agent is not None:
            return self._pydantic_agent
        
        # Get model string
        try:
            model_string = create_pydantic_ai_model(self.args)
        except Exception as e:
            logger.error(f"Error creating Pydantic AI model: {e}", exc_info=True)
            # Fallback: try to use OpenAI format
            model_string = f"openai:{self.args.get('model_name', 'gpt-4')}"
        
        # Handle MindsDB custom provider
        if model_string == "mindsdb:custom":
            # For MindsDB provider, we'll need a custom model wrapper
            # For now, raise an error - this needs custom implementation
            raise ValueError("MindsDB provider requires custom model wrapper - not yet implemented")
        
        # Get model kwargs (includes API keys from system config)
        model_kwargs = get_pydantic_ai_model_kwargs(self.args)
        
        # Pydantic AI reads API keys from environment variables
        # Set them temporarily if provided in model_kwargs
        import os
        env_vars_to_restore = {}
        if model_kwargs:
            provider = self.args.get("provider", "openai")
            
            # Set API key in environment if provided
            if "api_key" in model_kwargs:
                if provider == "openai" or provider == "vllm":
                    if "OPENAI_API_KEY" not in os.environ:
                        env_vars_to_restore["OPENAI_API_KEY"] = os.environ.get("OPENAI_API_KEY")
                        os.environ["OPENAI_API_KEY"] = model_kwargs["api_key"]
                elif provider == "anthropic":
                    if "ANTHROPIC_API_KEY" not in os.environ:
                        env_vars_to_restore["ANTHROPIC_API_KEY"] = os.environ.get("ANTHROPIC_API_KEY")
                        os.environ["ANTHROPIC_API_KEY"] = model_kwargs["api_key"]
                elif provider == "google":
                    if "GOOGLE_API_KEY" not in os.environ:
                        env_vars_to_restore["GOOGLE_API_KEY"] = os.environ.get("GOOGLE_API_KEY")
                        os.environ["GOOGLE_API_KEY"] = model_kwargs["api_key"]
            
            # Set base_url if provided (some providers support this via env vars)
            if "base_url" in model_kwargs:
                if provider == "openai" or provider == "vllm":
                    if "OPENAI_BASE_URL" not in os.environ:
                        env_vars_to_restore["OPENAI_BASE_URL"] = os.environ.get("OPENAI_BASE_URL")
                        os.environ["OPENAI_BASE_URL"] = model_kwargs["base_url"]
                elif provider == "anthropic":
                    if "ANTHROPIC_API_URL" not in os.environ:
                        env_vars_to_restore["ANTHROPIC_API_URL"] = os.environ.get("ANTHROPIC_API_URL")
                        os.environ["ANTHROPIC_API_URL"] = model_kwargs["base_url"]
        
        try:
            # Create agent with system prompt
            # Pydantic AI Agent doesn't accept model kwargs directly - it reads from environment
            # We've set the environment variables above, so they'll be picked up when the model is created
            system_prompt = self.args.get("prompt_template", "you are an assistant")
            
            # Create agent - Pydantic AI will read API keys from environment variables
            agent = Agent(
                model_string,
                system_prompt=system_prompt,
            )
        finally:
            # Restore original environment variables
            for key, value in env_vars_to_restore.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        
        # Build and register tools
        command_executor = self._get_command_executor()
        kb_controller = self._get_kb_controller()
        project_id = self.agent.project_id
        
        # Get LLM for tools (we'll create a simple wrapper if needed)
        # For now, tools will work without LLM in some cases
        llm = None  # Tools may not need LLM directly
        
        # Get embedding model if needed
        embedding_model = None
        if self.args.get("embedding_model_provider"):
            try:
                from mindsdb.interfaces.knowledge_base.embedding_model_utils import construct_embedding_model_from_args
                embedding_args = self.args.get("embedding_model", {})
                if embedding_args:
                    embedding_model = construct_embedding_model_from_args(embedding_args)
            except Exception as e:
                logger.warning(f"Could not create embedding model: {e}")
        
        # Build tools
        tools = build_tools_from_agent_config(
            agent_params=self.agent.params,
            command_executor=command_executor,
            llm=llm,
            embedding_model=embedding_model,
            kb_controller=kb_controller,
            project_id=project_id,
        )
        
        # Register tools with agent
        # Pydantic AI tools are typically registered via decorators, but we can use tool_plain
        # or register them by creating a new agent with tools
        # For now, we'll register them using the tool registration API
        for tool_func in tools:
            # Register tool using tool_plain for plain functions
            # This allows registering async functions without decorators
            try:
                agent.tool_plain(tool_func)
            except Exception as e:
                logger.warning(f"Could not register tool {tool_func.__name__}: {e}")
                # Try alternative registration method
                try:
                    agent.tool(tool_func)
                except Exception as e2:
                    logger.error(f"Failed to register tool {tool_func.__name__}: {e2}")
        
        self._pydantic_agent = agent
        return agent
    
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
            "provider": self.provider,
            "model_name": self.args["model_name"],
            "embedding_model_provider": self.args.get(
                "embedding_model_provider", 
                self._get_embedding_model_provider(self.args)
            ),
            "user_id": ctx.user_id,
            "session_id": ctx.session_id,
            "company_id": ctx.company_id,
            "user_class": ctx.user_class,
            "email_confirmed": ctx.email_confirmed,
        }
    
    def get_tags(self) -> List:
        """Get tags for observability"""
        return [self.provider]
    
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
        
        if stream:
            return self._get_completion_stream(messages)
        
        # Merge params
        args = {}
        args.update(self.args)
        args.update(params or {})
        
        # Extract current prompt and message history from messages
        # This handles multiple formats: list of dicts, DataFrame with role/content, or legacy DataFrame
        current_prompt, message_history = self._extract_current_prompt_and_history(messages, args)
        logger.info(f"PydanticAIAgent.get_completion: Extracted prompt and {len(message_history)} history messages")
        
        # Create agent
        agent = self._create_pydantic_agent()
        
        # Run agent
        try:
            result = agent.run_sync(
                current_prompt,
                message_history=message_history if message_history else None,
            )
            
            # Extract output
            output = result.output if hasattr(result, 'output') else str(result)
            
            # Create response DataFrame
            return_context = args.get("return_context", True)
            response_data = {
                ASSISTANT_COLUMN: [output],
                TRACE_ID_COLUMN: [self.langfuse_client_wrapper.get_trace_id()],
            }
            
            if return_context:
                # Extract context from result if available
                context = []
                if hasattr(result, 'data') and isinstance(result.data, dict):
                    context = result.data.get('context', [])
                response_data[CONTEXT_COLUMN] = [json.dumps(context)]
            
            response_df = pd.DataFrame(response_data)
            
            # End span
            self.langfuse_client_wrapper.end_span(
                span=self.run_completion_span, 
                output=response_df.to_dict('records')
            )
            
            return response_df
            
        except UnexpectedModelBehavior as e:
            logger.error(f"Model error: {e}", exc_info=True)
            error_message = f"Agent failed with model error: {str(e)}"
            return self._create_error_response(error_message, return_context=args.get("return_context", True))
        except Exception as e:
            logger.error(f"Agent error: {e}", exc_info=True)
            error_message = f"Agent failed with error: {str(e)}"
            return self._create_error_response(error_message, return_context=args.get("return_context", True))
    
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
        args = self.args
        
        # Extract current prompt and message history from messages
        # This handles multiple formats: list of dicts, DataFrame with role/content, or legacy DataFrame
        current_prompt, message_history = self._extract_current_prompt_and_history(messages, args)
        logger.info(f"PydanticAIAgent._get_completion_stream: Extracted prompt and {len(message_history)} history messages")
        
        # Create agent
        agent = self._create_pydantic_agent()
        
        # Yield start chunk
        yield self._add_chunk_metadata({"type": "start", "prompt": current_prompt})
        
        # Stream agent response
        try:
            # Use run_stream to capture all text output and final result
            def run_stream_sync():
                """Run async stream in sync context"""
                async def stream_agent():
                    collected_text = ""
                    async with agent.run_stream(
                        current_prompt, 
                        message_history=message_history if message_history else None
                    ) as run:
                        # Stream text output as it comes
                        async for text in run.stream_text():
                            collected_text += text
                            # Yield text chunks for a2a compatibility (expects 'text' or 'output' field)
                            yield self._add_chunk_metadata({
                                "type": "text",
                                "content": text,
                                "text": text,  # For a2a compatibility
                                "output": text  # Alternative field name
                            })
                        
                        # Get final output to ensure we have the complete response
                        try:
                            final_output = await run.get_output()
                            final_output_str = str(final_output) if final_output else collected_text
                            
                            # If final output is different from collected text, yield it
                            if final_output_str and final_output_str != collected_text:
                                yield self._add_chunk_metadata({
                                    "type": "text",
                                    "content": final_output_str,
                                    "text": final_output_str,
                                    "output": final_output_str
                                })
                            elif final_output_str:
                                # Ensure we yield the final output even if it matches
                                yield self._add_chunk_metadata({
                                    "type": "text",
                                    "content": final_output_str,
                                    "text": final_output_str,
                                    "output": final_output_str
                                })
                        except Exception as e:
                            logger.debug(f"Could not get final output, using collected text: {e}")
                            # Fallback: yield collected text if we couldn't get final output
                            if collected_text:
                                yield self._add_chunk_metadata({
                                    "type": "text",
                                    "content": collected_text,
                                    "text": collected_text,
                                    "output": collected_text
                                })
                
                # Get or create event loop
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                # Run async generator
                async_gen = stream_agent()
                while True:
                    try:
                        chunk = loop.run_until_complete(async_gen.__anext__())
                        yield chunk
                    except StopAsyncIteration:
                        break
            
            # Yield chunks from sync wrapper
            final_output_received = False
            for chunk in run_stream_sync():
                final_output_received = True
                yield chunk
            
            # Ensure we have a final output chunk if nothing was streamed
            if not final_output_received:
                logger.warning("No output chunks received from agent stream")
                yield self._add_chunk_metadata({
                    "type": "text",
                    "content": "",
                    "text": "",
                    "output": ""
                })
            
            # Yield context if needed
            return_context = args.get("return_context", True)
            if return_context:
                yield self._add_chunk_metadata({"type": "context", "content": []})
            
            # Yield end chunk
            yield self._add_chunk_metadata({"type": "end"})
            
            # End span
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

