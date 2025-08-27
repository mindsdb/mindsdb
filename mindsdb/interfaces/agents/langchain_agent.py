import json
from concurrent.futures import as_completed, TimeoutError
from typing import Dict, Iterable, List, Optional
from uuid import uuid4
import queue
import re
import threading
import numpy as np
import pandas as pd
import logging

from langchain.agents import AgentExecutor
from langchain.agents.initialize import initialize_agent
from langchain.chains.conversation.memory import ConversationSummaryBufferMemory
from langchain_community.chat_models import ChatLiteLLM, ChatOllama
from langchain_writer import ChatWriter
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.agents import AgentAction, AgentStep
from langchain_core.callbacks.base import BaseCallbackHandler
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from langchain_nvidia_ai_endpoints import ChatNVIDIA
from langchain_core.messages.base import BaseMessage
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import Tool

from mindsdb.integrations.libs.llm.utils import get_llm_config
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.integrations.utilities.rag.settings import DEFAULT_RAG_PROMPT_TEMPLATE
from mindsdb.interfaces.agents.event_dispatch_callback_handler import (
    EventDispatchCallbackHandler,
)
from mindsdb.interfaces.agents.constants import AGENT_CHUNK_POLLING_INTERVAL_SECONDS
from mindsdb.utilities import log
from mindsdb.utilities.context_executor import ContextThreadPoolExecutor
from mindsdb.interfaces.storage import db
from mindsdb.utilities.context import context as ctx

from .mindsdb_chat_model import ChatMindsdb
from .callback_handlers import LogCallbackHandler, ContextCaptureCallback
from .langfuse_callback_handler import LangfuseCallbackHandler, get_skills
from .safe_output_parser import SafeOutputParser

from mindsdb.interfaces.agents.constants import (
    OPEN_AI_CHAT_MODELS,
    DEFAULT_AGENT_TIMEOUT_SECONDS,
    DEFAULT_AGENT_TYPE,
    DEFAULT_EMBEDDINGS_MODEL_PROVIDER,
    DEFAULT_MAX_ITERATIONS,
    DEFAULT_MAX_TOKENS,
    DEFAULT_TIKTOKEN_MODEL_NAME,
    SUPPORTED_PROVIDERS,
    ANTHROPIC_CHAT_MODELS,
    GOOGLE_GEMINI_CHAT_MODELS,
    OLLAMA_CHAT_MODELS,
    NVIDIA_NIM_CHAT_MODELS,
    USER_COLUMN,
    ASSISTANT_COLUMN,
    CONTEXT_COLUMN,
    TRACE_ID_COLUMN,
    DEFAULT_AGENT_SYSTEM_PROMPT,
    WRITER_CHAT_MODELS,
    MINDSDB_PREFIX,
    EXPLICIT_FORMAT_INSTRUCTIONS,
)
from mindsdb.interfaces.skills.skill_tool import skill_tool, SkillData
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI

from mindsdb.utilities.langfuse import LangfuseClientWrapper

_PARSING_ERROR_PREFIXES = [
    "An output parsing error occurred",
    "Could not parse LLM output",
]

logger = log.getLogger(__name__)


def get_llm_provider(args: Dict) -> str:
    # If provider is explicitly specified, use that
    if "provider" in args:
        return args["provider"]

    # Check for known model names from other providers first
    if args["model_name"] in ANTHROPIC_CHAT_MODELS:
        return "anthropic"
    if args["model_name"] in OPEN_AI_CHAT_MODELS:
        return "openai"
    if args["model_name"] in OLLAMA_CHAT_MODELS:
        return "ollama"
    if args["model_name"] in NVIDIA_NIM_CHAT_MODELS:
        return "nvidia_nim"
    if args["model_name"] in GOOGLE_GEMINI_CHAT_MODELS:
        return "google"
    # Check for writer models
    if args["model_name"] in WRITER_CHAT_MODELS:
        return "writer"

    # For vLLM, require explicit provider specification
    raise ValueError("Invalid model name. Please define a supported llm provider")


def get_embedding_model_provider(args: Dict) -> str:
    """Get the embedding model provider from args.

    For VLLM, this will use our custom VLLMEmbeddings class from langchain_embedding_handler.
    """
    # Check for explicit embedding model provider
    if "embedding_model_provider" in args:
        provider = args["embedding_model_provider"]
        if provider == "vllm":
            if not (args.get("openai_api_base") and args.get("model")):
                raise ValueError(
                    "VLLM embeddings configuration error:\n"
                    "- Missing required parameters: 'openai_api_base' and/or 'model'\n"
                    "- Example: openai_api_base='http://localhost:8003/v1', model='your-model-name'"
                )
            logger.info("Using custom VLLMEmbeddings class")
            return "vllm"
        return provider

    # Check if LLM provider is vLLM
    llm_provider = args.get("provider", DEFAULT_EMBEDDINGS_MODEL_PROVIDER)
    if llm_provider == "vllm":
        if not (args.get("openai_api_base") and args.get("model")):
            raise ValueError(
                "VLLM embeddings configuration error:\n"
                "- Missing required parameters: 'openai_api_base' and/or 'model'\n"
                "- When using VLLM as LLM provider, you must specify the embeddings server location and model\n"
                "- Example: openai_api_base='http://localhost:8003/v1', model='your-model-name'"
            )
        logger.info("Using custom VLLMEmbeddings class")
        return "vllm"

    # Default to LLM provider
    return llm_provider


def get_chat_model_params(args: Dict) -> Dict:
    model_config = args.copy()
    # Include API keys.
    model_config["api_keys"] = {p: get_api_key(p, model_config, None, strict=False) for p in SUPPORTED_PROVIDERS}
    llm_config = get_llm_config(args.get("provider", get_llm_provider(args)), model_config)
    config_dict = llm_config.model_dump(by_alias=True)
    config_dict = {k: v for k, v in config_dict.items() if v is not None}

    # If provider is writer, ensure the API key is passed as 'api_key'
    if args.get("provider") == "writer" and "writer_api_key" in config_dict:
        config_dict["api_key"] = config_dict.pop("writer_api_key")

    return config_dict


def create_chat_model(args: Dict):
    model_kwargs = get_chat_model_params(args)

    if args["provider"] == "anthropic":
        return ChatAnthropic(**model_kwargs)
    if args["provider"] == "openai" or args["provider"] == "vllm":
        chat_open_ai = ChatOpenAI(**model_kwargs)
        # Some newer GPT models (e.g. gpt-4o when released) don't have token counting support yet.
        # By setting this manually in ChatOpenAI, we count tokens like compatible GPT models.
        try:
            chat_open_ai.get_num_tokens_from_messages([])
        except NotImplementedError:
            chat_open_ai.tiktoken_model_name = DEFAULT_TIKTOKEN_MODEL_NAME
        return chat_open_ai
    if args["provider"] == "litellm":
        return ChatLiteLLM(**model_kwargs)
    if args["provider"] == "ollama":
        return ChatOllama(**model_kwargs)
    if args["provider"] == "nvidia_nim":
        return ChatNVIDIA(**model_kwargs)
    if args["provider"] == "google":
        return ChatGoogleGenerativeAI(**model_kwargs)
    if args["provider"] == "writer":
        return ChatWriter(**model_kwargs)
    if args["provider"] == "mindsdb":
        return ChatMindsdb(**model_kwargs)
    raise ValueError(f"Unknown provider: {args['provider']}")


def prepare_prompts(df, base_template, input_variables, user_column=USER_COLUMN):
    empty_prompt_ids = np.where(df[input_variables].isna().all(axis=1).values)[0]

    # Combine system prompt with user-provided template
    base_template = f"{DEFAULT_AGENT_SYSTEM_PROMPT}\n\n{base_template}"

    base_template = base_template.replace("{{", "{").replace("}}", "}")
    prompts = []

    for i, row in df.iterrows():
        if i not in empty_prompt_ids:
            prompt = PromptTemplate(input_variables=input_variables, template=base_template)
            kwargs = {col: row[col] if row[col] is not None else "" for col in input_variables}
            prompts.append(prompt.format(**kwargs))
        elif row.get(user_column):
            prompts.append(row[user_column])

    return prompts, empty_prompt_ids


def prepare_callbacks(self, args):
    context_callback = ContextCaptureCallback()
    callbacks = self._get_agent_callbacks(args)
    callbacks.append(context_callback)
    return callbacks, context_callback


def handle_agent_error(e, error_message=None):
    if error_message is None:
        error_message = f"An error occurred during agent execution: {str(e)}"
    logger.error(error_message, exc_info=True)
    return error_message


def process_chunk(chunk):
    if isinstance(chunk, dict):
        return {k: process_chunk(v) for k, v in chunk.items()}
    elif isinstance(chunk, list):
        return [process_chunk(item) for item in chunk]
    elif isinstance(chunk, (str, int, float, bool, type(None))):
        return chunk
    else:
        return str(chunk)


class LangchainAgent:
    def __init__(self, agent: db.Agents, model: dict = None, llm_params: dict = None):
        self.agent = agent
        self.model = model

        self.run_completion_span: Optional[object] = None
        self.llm: Optional[object] = None
        self.embedding_model: Optional[object] = None

        self.log_callback_handler: Optional[object] = None
        self.langfuse_callback_handler: Optional[object] = None  # native langfuse callback handler
        self.mdb_langfuse_callback_handler: Optional[object] = None  # custom (see langfuse_callback_handler.py)

        self.langfuse_client_wrapper = LangfuseClientWrapper()
        self.args = self._initialize_args(llm_params)

        # Back compatibility for old models
        self.provider = self.args.get("provider", get_llm_provider(self.args))

    def _initialize_args(self, llm_params: dict = None) -> dict:
        """
        Initialize the arguments for agent execution.

        Takes the parameters passed during execution and sets necessary defaults.
        The params are already merged with defaults by AgentsController.get_agent_llm_params.

        Args:
            llm_params: Parameters for agent execution (already merged with defaults)

        Returns:
            dict: Final parameters for agent execution
        """
        # Use the parameters passed to the method (already merged with defaults by AgentsController)
        # No fallback needed as AgentsController.get_agent_llm_params already handles this
        args = self.agent.params.copy()
        if llm_params:
            args.update(llm_params)

        # Set model name and provider if given in create agent otherwise use global llm defaults
        # AgentsController.get_agent_llm_params
        if self.agent.model_name is not None:
            args["model_name"] = self.agent.model_name
        if self.agent.provider is not None:
            args["provider"] = self.agent.provider

        args["embedding_model_provider"] = args.get("embedding_model", get_embedding_model_provider(args))

        # agent is using current langchain model
        if self.agent.provider == "mindsdb":
            args["model_name"] = self.agent.model_name

            # get prompt
            prompt_template = self.model["problem_definition"].get("using", {}).get("prompt_template")
            if prompt_template is not None:
                # only update prompt_template if it is set on the model
                args["prompt_template"] = prompt_template

        # Set default prompt template if not provided
        if args.get("prompt_template") is None:
            # Default prompt template depends on agent mode
            if args.get("mode") == "retrieval":
                args["prompt_template"] = DEFAULT_RAG_PROMPT_TEMPLATE
                logger.info(f"Using default retrieval prompt template: {DEFAULT_RAG_PROMPT_TEMPLATE[:50]}...")
            else:
                # Set a default prompt template for non-retrieval mode
                default_prompt = "you are an assistant, answer using the tables connected"
                args["prompt_template"] = default_prompt
                logger.info(f"Using default prompt template: {default_prompt}")

        if "prompt_template" in args:
            logger.info(f"Using prompt template: {args['prompt_template'][:50]}...")

        if "model_name" not in args:
            raise ValueError(
                "No model name provided for agent. Provide it in the model parameter or in the default model setup."
            )

        return args

    def get_metadata(self) -> Dict:
        return {
            "provider": self.provider,
            "model_name": self.args["model_name"],
            "embedding_model_provider": self.args.get(
                "embedding_model_provider", get_embedding_model_provider(self.args)
            ),
            "skills": get_skills(self.agent),
            "user_id": ctx.user_id,
            "session_id": ctx.session_id,
            "company_id": ctx.company_id,
            "user_class": ctx.user_class,
            "email_confirmed": ctx.email_confirmed,
        }

    def get_tags(self) -> List:
        return [
            self.provider,
        ]

    def get_completion(self, messages, stream: bool = False, params: dict | None = None):
        # Get metadata and tags to be used in the trace
        metadata = self.get_metadata()
        tags = self.get_tags()

        # Set up trace for the API completion in Langfuse
        self.langfuse_client_wrapper.setup_trace(
            name="api-completion",
            input=messages,
            tags=tags,
            metadata=metadata,
            user_id=ctx.user_id,
            session_id=ctx.session_id,
        )

        # Set up trace for the run completion in Langfuse
        self.run_completion_span = self.langfuse_client_wrapper.start_span(name="run-completion", input=messages)

        if stream:
            return self._get_completion_stream(messages)

        args = {}
        args.update(self.args)
        args.update(params or {})

        df = pd.DataFrame(messages)
        logger.info(f"LangchainAgent.get_completion: Received {len(messages)} messages")
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Messages DataFrame shape: {df.shape}")
            logger.debug(f"Messages DataFrame columns: {df.columns.tolist()}")
            logger.debug(f"Messages DataFrame content: {df.to_dict('records')}")

        # Back compatibility for old models
        self.provider = args.get("provider", get_llm_provider(args))

        df = df.reset_index(drop=True)
        agent = self.create_agent(df)
        # Keep conversation history for context - don't nullify previous messages

        # Only use the last message as the current prompt, but preserve history for agent memory
        response = self.run_agent(df, agent, args)

        # End the run completion span and update the metadata with tool usage
        self.langfuse_client_wrapper.end_span(span=self.run_completion_span, output=response)

        return response

    def _get_completion_stream(self, messages: List[dict]) -> Iterable[Dict]:
        """Gets a completion as a stream of chunks from given messages.

        Args:
            messages (List[dict]): Messages to get completion chunks for

        Returns:
            chunks (Iterable[object]): Completion chunks
        """

        args = self.args

        df = pd.DataFrame(messages)
        logger.info(f"LangchainAgent._get_completion_stream: Received {len(messages)} messages")
        # Check if we have the expected columns for conversation history
        if "question" in df.columns and "answer" in df.columns:
            logger.debug("DataFrame has question/answer columns for conversation history")
        else:
            logger.warning("DataFrame missing question/answer columns! Available columns: {df.columns.tolist()}")

        self.embedding_model_provider = args.get("embedding_model_provider", get_embedding_model_provider(args))
        # Back compatibility for old models
        self.provider = args.get("provider", get_llm_provider(args))

        df = df.reset_index(drop=True)
        agent = self.create_agent(df)
        # Keep conversation history for context - don't nullify previous messages
        # Only use the last message as the current prompt, but preserve history for agent memory
        return self.stream_agent(df, agent, args)

    def create_agent(self, df: pd.DataFrame) -> AgentExecutor:
        # Set up tools.

        args = self.args

        llm = create_chat_model(args)
        self.llm = llm

        # Don't set embedding model for retrieval mode - let the knowledge base handle it
        if args.get("mode") == "retrieval":
            self.args.pop("mode")

        tools = self._langchain_tools_from_skills(llm)

        # Prefer prediction prompt template over original if provided.
        prompt_template = args["prompt_template"]

        # Modern LangChain approach: Use memory but populate it correctly
        # Create memory and populate with conversation history
        memory = ConversationSummaryBufferMemory(
            llm=llm,
            input_key="input",
            output_key="output",
            max_token_limit=args.get("max_tokens", DEFAULT_MAX_TOKENS),
            memory_key="chat_history",
        )

        # Add system message first
        memory.chat_memory.messages.insert(0, SystemMessage(content=prompt_template))

        user_column = args.get("user_column", USER_COLUMN)
        assistant_column = args.get("assistant_column", ASSISTANT_COLUMN)

        logger.info(f"Processing conversation history: {len(df)} total messages, {len(df[:-1])} history messages")
        logger.debug(f"User column: {user_column}, Assistant column: {assistant_column}")

        # Process history messages (all except the last one which is current message)
        history_df = df[:-1]
        if len(history_df) == 0:
            logger.debug("No history rows to process - this is normal for first message")

        history_count = 0
        for i, row in enumerate(history_df.to_dict("records")):
            question = row.get(user_column)
            answer = row.get(assistant_column)
            logger.debug(f"Converting history row {i}: question='{question}', answer='{answer}'")

            # Add messages directly to memory's chat_memory.messages list (modern approach)
            if isinstance(question, str) and len(question) > 0:
                memory.chat_memory.messages.append(HumanMessage(content=question))
                history_count += 1
                logger.debug(f"Added HumanMessage to memory: {question}")
            if isinstance(answer, str) and len(answer) > 0:
                memory.chat_memory.messages.append(AIMessage(content=answer))
                history_count += 1
                logger.debug(f"Added AIMessage to memory: {answer}")

        logger.info(f"Built conversation history with {history_count} history messages + system message")
        logger.debug(f"Final memory messages count: {len(memory.chat_memory.messages)}")

        # Store memory for agent use
        self._conversation_memory = memory

        agent_type = args.get("agent_type", DEFAULT_AGENT_TYPE)
        agent_executor = initialize_agent(
            tools,
            llm,
            agent=agent_type,
            # Use custom output parser to handle flaky LLMs that don't ALWAYS conform to output format.
            agent_kwargs={
                "output_parser": SafeOutputParser(),
                "prefix": MINDSDB_PREFIX,  # Override default "Assistant is a large language model..." text
                "format_instructions": EXPLICIT_FORMAT_INSTRUCTIONS,  # More explicit tool calling instructions
                "ai_prefix": "AI",
            },
            # Calls the agent's LLM Chain one final time to generate a final answer based on the previous steps
            early_stopping_method="generate",
            handle_parsing_errors=self._handle_parsing_errors,
            # Timeout per agent invocation.
            max_execution_time=args.get(
                "timeout_seconds",
                args.get("timeout_seconds", DEFAULT_AGENT_TIMEOUT_SECONDS),
            ),
            max_iterations=args.get("max_iterations", args.get("max_iterations", DEFAULT_MAX_ITERATIONS)),
            memory=memory,
            verbose=args.get("verbose", args.get("verbose", False)),
        )
        return agent_executor

    def _langchain_tools_from_skills(self, llm):
        # Makes Langchain compatible tools from a skill
        skills_data = [
            SkillData(
                name=rel.skill.name,
                type=rel.skill.type,
                params=rel.skill.params,
                project_id=rel.skill.project_id,
                agent_tables_list=(rel.parameters or {}).get("tables"),
            )
            for rel in self.agent.skills_relationships
        ]

        tools_groups = skill_tool.get_tools_from_skills(skills_data, llm, self.embedding_model)

        all_tools = []
        for skill_type, tools in tools_groups.items():
            for tool in tools:
                if isinstance(tool, dict):
                    tool = Tool(
                        name=tool["name"],
                        func=tool["func"],
                        description=tool["description"],
                    )
                all_tools.append(tool)
        return all_tools

    def _get_agent_callbacks(self, args: Dict) -> List:
        all_callbacks = []

        if self.log_callback_handler is None:
            self.log_callback_handler = LogCallbackHandler(logger, verbose=args.get("verbose", True))

        all_callbacks.append(self.log_callback_handler)

        if self.langfuse_client_wrapper.trace is None:
            # Get metadata and tags to be used in the trace
            metadata = self.get_metadata()
            tags = self.get_tags()

            trace_name = "NativeTrace-MindsDB-AgentExecutor"

            # Set up trace for the API completion in Langfuse
            self.langfuse_client_wrapper.setup_trace(
                name=trace_name,
                tags=tags,
                metadata=metadata,
                user_id=ctx.user_id,
                session_id=ctx.session_id,
            )

        if self.langfuse_callback_handler is None:
            self.langfuse_callback_handler = self.langfuse_client_wrapper.get_langchain_handler()

        # custom tracer
        if self.mdb_langfuse_callback_handler is None:
            trace_id = self.langfuse_client_wrapper.get_trace_id()

            span_id = None
            if self.run_completion_span is not None:
                span_id = self.run_completion_span.id

            observation_id = args.get("observation_id", span_id or uuid4().hex)

            self.mdb_langfuse_callback_handler = LangfuseCallbackHandler(
                langfuse=self.langfuse_client_wrapper.client,
                trace_id=trace_id,
                observation_id=observation_id,
            )

        # obs: we may want to unify these; native langfuse handler provides details as a tree on a sub-step of the overarching custom one  # noqa
        if self.langfuse_callback_handler is not None:
            all_callbacks.append(self.langfuse_callback_handler)

        if self.mdb_langfuse_callback_handler:
            all_callbacks.append(self.mdb_langfuse_callback_handler)

        return all_callbacks

    def _handle_parsing_errors(self, error: Exception) -> str:
        response = str(error)
        for p in _PARSING_ERROR_PREFIXES:
            if response.startswith(p):
                # As a somewhat dirty workaround, we accept the output formatted incorrectly and use it as a response.
                #
                # Ideally, in the future, we would write a parser that is more robust and flexible than the one Langchain uses.
                # Response is wrapped in ``
                logger.info("Handling parsing error, salvaging response...")
                response_output = response.split("`")
                if len(response_output) >= 2:
                    response = response_output[-2]

                # Wrap response in Langchain conversational react format.
                langchain_react_formatted_response = f"""Thought: Do I need to use a tool? No
AI: {response}"""
                return langchain_react_formatted_response
        return f"Agent failed with error:\n{str(error)}..."

    def run_agent(self, df: pd.DataFrame, agent: AgentExecutor, args: Dict) -> pd.DataFrame:
        base_template = args.get("prompt_template", args["prompt_template"])
        return_context = args.get("return_context", True)
        input_variables = re.findall(r"{{(.*?)}}", base_template)

        prompts, empty_prompt_ids = prepare_prompts(
            df, base_template, input_variables, args.get("user_column", USER_COLUMN)
        )

        def _invoke_agent_executor_with_prompt(agent_executor, prompt):
            if not prompt:
                return {CONTEXT_COLUMN: [], ASSISTANT_COLUMN: ""}
            try:
                callbacks, context_callback = prepare_callbacks(self, args)

                # Modern LangChain approach: Include conversation history + current message
                if hasattr(self, "_conversation_messages") and self._conversation_messages:
                    # Add current user message to conversation history
                    full_messages = self._conversation_messages + [HumanMessage(content=prompt)]
                    logger.critical(f"🔍 INVOKING AGENT with {len(full_messages)} messages (including history)")
                    logger.debug(
                        f"Full conversation messages: {[type(msg).__name__ + ': ' + msg.content[:100] + '...' for msg in full_messages]}"
                    )

                    # For agents, we need to pass the input in the expected format
                    # The agent expects 'input' key with the current question, but conversation history should be in memory
                    result = agent_executor.invoke({"input": prompt}, config={"callbacks": callbacks})
                else:
                    logger.warning("No conversation messages found - using simple prompt")
                    result = agent_executor.invoke({"input": prompt}, config={"callbacks": callbacks})
                captured_context = context_callback.get_contexts()
                output = result["output"] if isinstance(result, dict) and "output" in result else str(result)
                return {CONTEXT_COLUMN: captured_context, ASSISTANT_COLUMN: output}
            except Exception as e:
                error_message = str(e)
                # Special handling for API key errors
                if "API key" in error_message and ("not found" in error_message or "missing" in error_message):
                    # Format API key error more clearly
                    logger.error(f"API Key Error: {error_message}")
                    error_message = f"API Key Error: {error_message}"
                return {
                    CONTEXT_COLUMN: [],
                    ASSISTANT_COLUMN: handle_agent_error(e, error_message),
                }

        completions = []
        contexts = []

        max_workers = args.get("max_workers", None)
        agent_timeout_seconds = args.get("timeout", DEFAULT_AGENT_TIMEOUT_SECONDS)

        with ContextThreadPoolExecutor(max_workers=max_workers) as executor:
            # Only process the last prompt (current question), not all prompts
            # The previous prompts are conversation history and should only be used for context
            if prompts:
                current_prompt = prompts[-1]  # Last prompt is the current question
                futures = [executor.submit(_invoke_agent_executor_with_prompt, agent, current_prompt)]
            else:
                logger.error("No prompts found to process")
                futures = []
            try:
                for future in as_completed(futures, timeout=agent_timeout_seconds):
                    result = future.result()
                    if result is None:
                        result = {
                            CONTEXT_COLUMN: [],
                            ASSISTANT_COLUMN: "No response generated",
                        }

                    completions.append(result[ASSISTANT_COLUMN])
                    contexts.append(result[CONTEXT_COLUMN])
            except TimeoutError:
                timeout_message = (
                    f"I'm sorry! I couldn't generate a response within the allotted time ({agent_timeout_seconds} seconds). "
                    "If you need more time for processing, you can adjust the timeout settings. "
                    "Please refer to the documentation for instructions on how to change the timeout value. "
                    "Feel free to try your request again."
                )
                logger.warning(f"Agent execution timed out after {agent_timeout_seconds} seconds")
                for _ in range(len(futures) - len(completions)):
                    completions.append(timeout_message)
                    contexts.append([])

        # Add null completion for empty prompts
        for i in sorted(empty_prompt_ids)[:-1]:
            completions.insert(i, None)
            contexts.insert(i, [])

        # Create DataFrame with completions and context if required
        pred_df = pd.DataFrame(
            {
                ASSISTANT_COLUMN: completions,
                CONTEXT_COLUMN: [json.dumps(ctx) for ctx in contexts],  # Serialize context to JSON string
                TRACE_ID_COLUMN: self.langfuse_client_wrapper.get_trace_id(),
            }
        )

        if not return_context:
            pred_df = pred_df.drop(columns=[CONTEXT_COLUMN])

        return pred_df

    def add_chunk_metadata(self, chunk: Dict) -> Dict:
        logger.debug(f"Adding metadata to chunk: {chunk}")
        logger.debug(f"Trace ID: {self.langfuse_client_wrapper.get_trace_id()}")
        chunk["trace_id"] = self.langfuse_client_wrapper.get_trace_id()
        return chunk

    def _stream_agent_executor(
        self,
        agent_executor: AgentExecutor,
        prompt: str,
        callbacks: List[BaseCallbackHandler],
    ):
        chunk_queue = queue.Queue()
        # Add event dispatch callback handler only to streaming completions.
        event_dispatch_callback_handler = EventDispatchCallbackHandler(chunk_queue)
        callbacks.append(event_dispatch_callback_handler)
        stream_iterator = agent_executor.stream(prompt, config={"callbacks": callbacks})

        agent_executor_finished_event = threading.Event()

        def stream_worker(context: dict):
            try:
                ctx.load(context)
                for chunk in stream_iterator:
                    chunk_queue.put(chunk)
            finally:
                # Wrap in try/finally to always set the thread event even if there's an exception.
                agent_executor_finished_event.set()

        # Enqueue Langchain agent streaming chunks in a separate thread to not block event chunks.
        executor_stream_thread = threading.Thread(
            target=stream_worker,
            daemon=True,
            args=(ctx.dump(),),
            name="LangchainAgent.stream_worker",
        )
        executor_stream_thread.start()

        while not agent_executor_finished_event.is_set():
            try:
                chunk = chunk_queue.get(block=True, timeout=AGENT_CHUNK_POLLING_INTERVAL_SECONDS)
            except queue.Empty:
                continue
            logger.debug(f"Processing streaming chunk {chunk}")
            processed_chunk = self.process_chunk(chunk)
            logger.info(f"Processed chunk: {processed_chunk}")
            yield self.add_chunk_metadata(processed_chunk)
            chunk_queue.task_done()

    def stream_agent(self, df: pd.DataFrame, agent_executor: AgentExecutor, args: Dict) -> Iterable[Dict]:
        base_template = args.get("prompt_template", args["prompt_template"])
        input_variables = re.findall(r"{{(.*?)}}", base_template)
        return_context = args.get("return_context", True)

        prompts, _ = prepare_prompts(df, base_template, input_variables, args.get("user_column", USER_COLUMN))

        callbacks, context_callback = prepare_callbacks(self, args)

        # Use last prompt (current question) instead of first prompt (history)
        current_prompt = prompts[-1] if prompts else ""
        yield self.add_chunk_metadata({"type": "start", "prompt": current_prompt})

        if not hasattr(agent_executor, "stream") or not callable(agent_executor.stream):
            raise AttributeError("The agent_executor does not have a 'stream' method")

        stream_iterator = self._stream_agent_executor(agent_executor, current_prompt, callbacks)
        for chunk in stream_iterator:
            yield chunk

        if return_context:
            # Yield context if required
            captured_context = context_callback.get_contexts()
            if captured_context:
                yield {"type": "context", "content": captured_context}

        if self.log_callback_handler.generated_sql:
            # Yield generated SQL if available
            yield self.add_chunk_metadata({"type": "sql", "content": self.log_callback_handler.generated_sql})

        # End the run completion span and update the metadata with tool usage
        self.langfuse_client_wrapper.end_span_stream(span=self.run_completion_span)

    @staticmethod
    def process_chunk(chunk):
        if isinstance(chunk, dict):
            return {k: LangchainAgent.process_chunk(v) for k, v in chunk.items()}
        if isinstance(chunk, list):
            return [LangchainAgent.process_chunk(item) for item in chunk]
        if isinstance(chunk, AgentAction):
            # Format agent actions properly for streaming.
            return {
                "tool": LangchainAgent.process_chunk(chunk.tool),
                "tool_input": LangchainAgent.process_chunk(chunk.tool_input),
                "log": LangchainAgent.process_chunk(chunk.log),
            }
        if isinstance(chunk, AgentStep):
            # Format agent steps properly for streaming.
            return {
                "action": LangchainAgent.process_chunk(chunk.action),
                "observation": LangchainAgent.process_chunk(chunk.observation) if chunk.observation else "",
            }
        if issubclass(chunk.__class__, BaseMessage):
            # Extract content from message subclasses properly for streaming.
            return {"content": chunk.content}
        if isinstance(chunk, (str, int, float, bool, type(None))):
            return chunk
        return str(chunk)
