import json
from concurrent.futures import as_completed, TimeoutError
from typing import Dict, Iterable, List
from uuid import uuid4
import os
import re
import numpy as np
import pandas as pd

from langchain.agents import AgentExecutor
from langchain.agents.initialize import initialize_agent
from langchain.chains.conversation.memory import ConversationSummaryBufferMemory
from langchain.schema import SystemMessage
from langchain_community.chat_models import (
    ChatAnthropic,
    ChatOpenAI,
    ChatAnyscale,
    ChatLiteLLM,
    ChatOllama,
)
from langchain_core.agents import AgentAction, AgentStep
from langchain_core.embeddings import Embeddings
from langchain_nvidia_ai_endpoints import ChatNVIDIA
from langchain_core.messages.base import BaseMessage
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import Tool
from langfuse import Langfuse
from langfuse.api.resources.commons.errors.not_found_error import NotFoundError as TraceNotFoundError
from langfuse.callback import CallbackHandler

from mindsdb.integrations.handlers.openai_handler.constants import (
    CHAT_MODELS as OPEN_AI_CHAT_MODELS,
)
from mindsdb.integrations.libs.llm.utils import get_llm_config
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.integrations.handlers.langchain_embedding_handler.langchain_embedding_handler import (
    construct_model_from_args,
)
from mindsdb.utilities import log
from mindsdb.utilities.context_executor import ContextThreadPoolExecutor
from mindsdb.interfaces.storage import db
from mindsdb.utilities.context import context as ctx


from .mindsdb_chat_model import ChatMindsdb
from .callback_handlers import LogCallbackHandler, ContextCaptureCallback
from .langfuse_callback_handler import LangfuseCallbackHandler, get_metadata, get_tags, get_tool_usage, get_skills
from .safe_output_parser import SafeOutputParser


from .constants import (
    DEFAULT_AGENT_TIMEOUT_SECONDS,
    DEFAULT_AGENT_TYPE,
    DEFAULT_EMBEDDINGS_MODEL_PROVIDER,
    DEFAULT_MAX_ITERATIONS,
    DEFAULT_MAX_TOKENS,
    SUPPORTED_PROVIDERS,
    ANTHROPIC_CHAT_MODELS,
    OLLAMA_CHAT_MODELS,
    NVIDIA_NIM_CHAT_MODELS,
    USER_COLUMN,
    ASSISTANT_COLUMN,
    CONTEXT_COLUMN,
)
from mindsdb.interfaces.skills.skill_tool import skill_tool
from mindsdb.integrations.utilities.rag.settings import DEFAULT_RAG_PROMPT_TEMPLATE

_PARSING_ERROR_PREFIXES = [
    "An output parsing error occurred",
    "Could not parse LLM output",
]

logger = log.getLogger(__name__)


def get_llm_provider(args: Dict) -> str:
    if "provider" in args:
        return args["provider"]
    if args["model_name"] in ANTHROPIC_CHAT_MODELS:
        return "anthropic"
    if args["model_name"] in OPEN_AI_CHAT_MODELS:
        return "openai"
    if args["model_name"] in OLLAMA_CHAT_MODELS:
        return "ollama"
    if args["model_name"] in NVIDIA_NIM_CHAT_MODELS:
        return "nvidia_nim"
    raise ValueError("Invalid model name. Please define a supported llm provider")


def get_embedding_model_provider(args: Dict) -> str:
    if "embedding_model_provider" in args:
        return args["embedding_model_provider"]
    if "embedding_model_provider" not in args:
        logger.warning(
            "No embedding model provider specified. trying to use llm provider."
        )
        llm_provider = get_llm_provider(args)
        if llm_provider == 'mindsdb':
            # We aren't an embeddings provider, so use the default instead.
            llm_provider = DEFAULT_EMBEDDINGS_MODEL_PROVIDER
        return args.get("embedding_model_provider", llm_provider)
    raise ValueError("Invalid model name. Please define provider")


def get_chat_model_params(args: Dict) -> Dict:
    model_config = args.copy()
    # Include API keys.
    model_config["api_keys"] = {
        p: get_api_key(p, model_config, None, strict=False) for p in SUPPORTED_PROVIDERS
    }
    llm_config = get_llm_config(
        args.get("provider", get_llm_provider(args)), model_config
    )
    config_dict = llm_config.model_dump()
    config_dict = {k: v for k, v in config_dict.items() if v is not None}
    return config_dict


def build_embedding_model(args) -> Embeddings:
    """
    Build an embeddings model from the given arguments.
    """
    # Set up embeddings model if needed.
    embeddings_args = args.pop("embedding_model_args", {})

    # no embedding model args provided, use default provider.
    if not embeddings_args:
        embeddings_provider = get_embedding_model_provider(args)
        logger.warning(
            "'embedding_model_args' not found in input params, "
            f"Trying to use LLM provider: {embeddings_provider}"
        )
        embeddings_args["class"] = embeddings_provider
        # Include API keys if present.
        embeddings_args.update({k: v for k, v in args.items() if "api_key" in k})

    return construct_model_from_args(embeddings_args)


def create_chat_model(args: Dict):
    model_kwargs = get_chat_model_params(args)

    def _get_tiktoken_model_name(model: str) -> str:
        if model.startswith("gpt-4"):
            return "gpt-4"
        return model

    if args["provider"] == "anthropic":
        return ChatAnthropic(**model_kwargs)
    if args["provider"] == "openai":
        # Some newer GPT models (e.g. gpt-4o when released) don't have token counting support yet.
        # By setting this manually in ChatOpenAI, we count tokens like compatible GPT models.
        model_kwargs["tiktoken_model_name"] = _get_tiktoken_model_name(
            model_kwargs.get("model_name")
        )
        return ChatOpenAI(**model_kwargs)
    if args["provider"] == "anyscale":
        return ChatAnyscale(**model_kwargs)
    if args["provider"] == "litellm":
        return ChatLiteLLM(**model_kwargs)
    if args["provider"] == "ollama":
        return ChatOllama(**model_kwargs)
    if args["provider"] == "nvidia_nim":
        return ChatNVIDIA(**model_kwargs)
    if args["provider"] == "mindsdb":
        return ChatMindsdb(**model_kwargs)
    raise ValueError(f'Unknown provider: {args["provider"]}')


def prepare_prompts(df, base_template, input_variables, user_column=USER_COLUMN):
    empty_prompt_ids = np.where(df[input_variables].isna().all(axis=1).values)[0]
    base_template = base_template.replace('{{', '{').replace('}}', '}')
    prompts = []

    for i, row in df.iterrows():
        if i not in empty_prompt_ids:
            prompt = PromptTemplate(input_variables=input_variables, template=base_template)
            kwargs = {col: row[col] if row[col] is not None else '' for col in input_variables}
            prompts.append(prompt.format(**kwargs))
        elif row.get(user_column):
            prompts.append(row[user_column])

    return prompts, empty_prompt_ids


def prepare_callbacks(self, args):
    context_callback = ContextCaptureCallback()
    callbacks = self._get_agent_callbacks(args)
    callbacks.append(context_callback)
    return callbacks, context_callback


def handle_agent_error(e):
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
    def __init__(self, agent: db.Agents, model):

        self.llm = None
        self.embedding_model = None
        self.agent = agent
        args = agent.params.copy()
        args["model_name"] = agent.model_name
        args["provider"] = agent.provider
        args["embedding_model_provider"] = args.get(
            "embedding_model", get_embedding_model_provider(args)
        )

        self.langfuse = None
        if os.getenv('LANGFUSE_PUBLIC_KEY') is not None:
            self.langfuse = Langfuse(
                public_key=os.getenv('LANGFUSE_PUBLIC_KEY'),
                secret_key=os.getenv('LANGFUSE_SECRET_KEY'),
                host=os.getenv('LANGFUSE_HOST'),
                release=os.getenv('LANGFUSE_RELEASE', 'local'),
            )

        # agent is using current langchain model
        if agent.provider == "mindsdb":
            args["model_name"] = agent.model_name

            # get prompt
            prompt_template = (
                model["problem_definition"].get("using", {}).get("prompt_template")
            )
            if prompt_template is not None:
                # only update prompt_template if it is set on the model
                args["prompt_template"] = prompt_template

        if args.get("prompt_template") is None:
            if args.get("mode") == "retrieval":
                args["prompt_template"] = DEFAULT_RAG_PROMPT_TEMPLATE
            else:
                raise ValueError(
                    "Please provide a `prompt_template` or set `mode=retrieval`"
                )

        self.args = args
        self.trace_id = None
        self.observation_id = None
        self.log_callback_handler = None
        self.langfuse_callback_handler = None  # native langfuse callback handler
        self.mdb_langfuse_callback_handler = (
            None  # custom (see langfuse_callback_handler.py)
        )

    def get_completion(self, messages, stream: bool = False):

        self.run_completion_span = None
        self.api_trace = None
        if self.langfuse:

            # todo we need to fix this as this assumes that the model is always langchain
            # since decoupling the model from langchain, we need to find a way to get the model name
            # this breaks retrieval agents

            # metadata retrieval
            trace_metadata = {
                'provider': self.args["provider"],
                'model_name': self.args["model_name"],
                'embedding_model_provider': self.args.get('embedding_model_provider', get_embedding_model_provider(self.args))
            }
            trace_metadata['skills'] = get_skills(self.agent)
            trace_tags = get_tags(trace_metadata)

            # Set our user info to pass into langfuse trace, with fault tolerance in each individual one just incase on purpose
            trace_metadata['user_id'] = ctx.user_id
            trace_metadata['session_id'] = ctx.session_id
            trace_metadata['company_id'] = ctx.company_id
            trace_metadata['user_class'] = ctx.user_class
            trace_metadata['email_confirmed'] = ctx.email_confirmed

            self.api_trace = self.langfuse.trace(
                name='api-completion',
                input=messages,
                tags=trace_tags,
                metadata=trace_metadata,
                user_id=ctx.user_id,
                session_id=ctx.session_id,
            )

            self.run_completion_span = self.api_trace.span(name='run-completion', input=messages)
            trace_id = self.api_trace.id
            observation_id = self.run_completion_span.id

            self.trace_id = trace_id
            self.observation_id = observation_id
            logger.info(f"Langfuse trace created with ID: {trace_id}")

        if stream:
            return self._get_completion_stream(messages)

        args = self.args

        df = pd.DataFrame(messages)

        # Back compatibility for old models
        self.provider = args.get("provider", get_llm_provider(args))

        df = df.reset_index(drop=True)
        agent = self.create_agent(df, args)
        # Use last message as prompt, remove other questions.
        user_column = args.get("user_column", USER_COLUMN)
        df.iloc[:-1, df.columns.get_loc(user_column)] = None
        response = self.run_agent(df, agent, args)

        if self.run_completion_span is not None and self.api_trace is not None:
            self.run_completion_span.end(output=response)
            self.api_trace.update(output=response)

            # update metadata with tool usage
            try:
                # Ensure all batched traces are sent before fetching.
                self.langfuse.flush()
                trace = self.langfuse.get_trace(self.trace_id)
                trace_metadata['tool_usage'] = get_tool_usage(trace)
                self.api_trace.update(metadata=trace_metadata)
            except TraceNotFoundError:
                logger.warning(f'Langfuse trace {self.trace_id} not found')
            except Exception as e:
                logger.error(f'Something went wrong while processing Langfuse trace {self.trace_id}: {str(e)}')

        return response

    def _get_completion_stream(
        self, messages: List[dict]
    ) -> Iterable[Dict]:
        """
        Gets a completion as a stream of chunks from given messages.

        Args:
            messages (List[dict]): Messages to get completion chunks for
            trace_id (str): Langfuse trace ID to use
            observation_id (str): Langfuse parent observation Id to use

        Returns:
            chunks (Iterable[object]): Completion chunks
        """

        args = self.args

        df = pd.DataFrame(messages)

        # Back compatibility for old models
        self.provider = args.get("provider", get_llm_provider(args))

        self.embedding_model_provider = args.get('embedding_model_provider', get_embedding_model_provider(args))

        df = df.reset_index(drop=True)
        agent = self.create_agent(df, args)
        # Use last message as prompt, remove other questions.
        user_column = args.get("user_column", USER_COLUMN)
        df.iloc[:-1, df.columns.get_loc(user_column)] = None
        return self.stream_agent(df, agent, args)

    def set_embedding_model(self, args):
        """
        Set the embedding model for the agent.
        """
        self.embedding_model = build_embedding_model(args)

    def create_agent(self, df: pd.DataFrame, args: Dict = None) -> AgentExecutor:
        # Set up tools.
        llm = create_chat_model(args)
        self.llm = llm
        if args.get("mode") == "retrieval":
            self.set_embedding_model(args)
            self.args.pop("mode")

        tools = []
        skills = self.agent.skills or []
        tools += self.langchain_tools_from_skills(skills, llm)

        # Prefer prediction prompt template over original if provided.
        prompt_template = args["prompt_template"]

        # Set up memory.
        memory = ConversationSummaryBufferMemory(
            llm=llm,
            input_key="input",
            output_key="output",
            max_token_limit=args.get("max_tokens", DEFAULT_MAX_TOKENS),
            memory_key="chat_history",
        )

        memory.chat_memory.messages.insert(0, SystemMessage(content=prompt_template))
        # User - Assistant conversation. All except the last message.
        user_column = args.get("user_column", USER_COLUMN)
        assistant_column = args.get("assistant_column", ASSISTANT_COLUMN)
        for row in df[:-1].to_dict("records"):
            question = row[user_column]
            answer = row[assistant_column]
            if question:
                memory.chat_memory.add_user_message(question)
            if answer:
                memory.chat_memory.add_ai_message(answer)

        agent_type = args.get("agent_type", DEFAULT_AGENT_TYPE)
        agent_executor = initialize_agent(
            tools,
            llm,
            agent=agent_type,
            # Use custom output parser to handle flaky LLMs that don't ALWAYS conform to output format.
            agent_kwargs={"output_parser": SafeOutputParser()},
            # Calls the agent’s LLM Chain one final time to generate a final answer based on the previous steps
            early_stopping_method="generate",
            handle_parsing_errors=self._handle_parsing_errors,
            # Timeout per agent invocation.
            max_execution_time=args.get(
                "timeout_seconds",
                args.get("timeout_seconds", DEFAULT_AGENT_TIMEOUT_SECONDS),
            ),
            max_iterations=args.get(
                "max_iterations", args.get("max_iterations", DEFAULT_MAX_ITERATIONS)
            ),
            memory=memory,
            verbose=args.get("verbose", args.get("verbose", True)),
        )
        return agent_executor

    def langchain_tools_from_skills(self, skills, llm):
        # Makes Langchain compatible tools from a skill
        tools_groups = skill_tool.get_tools_from_skills(skills, llm, self.embedding_model)

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

        if self.log_callback_handler is None:
            self.log_callback_handler = LogCallbackHandler(logger)

        all_callbacks = [self.log_callback_handler]

        langfuse_public_key = args.get(
            "langfuse_public_key", os.getenv("LANGFUSE_PUBLIC_KEY")
        )
        langfuse_secret_key = args.get(
            "langfuse_secret_key", os.getenv("LANGFUSE_SECRET_KEY")
        )
        langfuse_host = args.get("langfuse_host", os.getenv("LANGFUSE_HOST"))
        are_langfuse_args_present = (
            bool(langfuse_public_key)
            and bool(langfuse_secret_key)
            and bool(langfuse_host)
        )

        if are_langfuse_args_present:
            if self.langfuse_callback_handler is None:
                trace_name = args.get(
                    "trace_id",
                    (
                        f"NativeTrace-...{self.trace_id[-7:]}"
                        if self.trace_id is not None
                        else "NativeTrace-MindsDB-AgentExecutor"
                    ),
                )
                metadata = get_metadata(args)
                self.langfuse_callback_handler = CallbackHandler(
                    public_key=langfuse_public_key,
                    secret_key=langfuse_secret_key,
                    host=langfuse_host,
                    trace_name=trace_name,
                    tags=get_tags(metadata),
                    metadata=metadata,
                )
                try:
                    # This try is critical to catch fatal errors which would otherwise prevent the agent from running properly
                    if not self.langfuse_callback_handler.auth_check():
                        logger.error(
                            f"Incorrect Langfuse credentials provided to Langchain handler. Full args: {args}"
                        )
                except Exception as e:
                    logger.error(f'Something went wrong while running langfuse_callback_handler.auth_check {str(e)}')

            # custom tracer
            if self.mdb_langfuse_callback_handler is None:
                trace_id = args.get("trace_id", self.trace_id or None)
                observation_id = args.get(
                    "observation_id", self.observation_id or uuid4().hex
                )
                langfuse = Langfuse(
                    host=langfuse_host,
                    public_key=langfuse_public_key,
                    secret_key=langfuse_secret_key,
                )
                self.mdb_langfuse_callback_handler = LangfuseCallbackHandler(
                    langfuse=langfuse,
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
        base_template = args.get('prompt_template', args['prompt_template'])
        return_context = args.get('return_context', True)
        input_variables = re.findall(r"{{(.*?)}}", base_template)

        prompts, empty_prompt_ids = prepare_prompts(df, base_template, input_variables, args.get('user_column', USER_COLUMN))

        def _invoke_agent_executor_with_prompt(agent_executor, prompt):
            if not prompt:
                return {CONTEXT_COLUMN: [], ASSISTANT_COLUMN: ""}
            try:
                callbacks, context_callback = prepare_callbacks(self, args)
                result = agent_executor.invoke(prompt, config={'callbacks': callbacks})
                captured_context = context_callback.get_contexts()
                output = result['output'] if isinstance(result, dict) and 'output' in result else str(result)
                return {CONTEXT_COLUMN: captured_context, ASSISTANT_COLUMN: output}
            except Exception as e:
                return {CONTEXT_COLUMN: [], ASSISTANT_COLUMN: handle_agent_error(e)}

        completions = []
        contexts = []

        max_workers = args.get("max_workers", None)
        agent_timeout_seconds = args.get("timeout", DEFAULT_AGENT_TIMEOUT_SECONDS)

        with ContextThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_invoke_agent_executor_with_prompt, agent, prompt)
                for prompt in prompts
            ]
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
                timeout_message = "I'm sorry! I couldn't come up with a response in time. Please try again."
                logger.warning(
                    f"Agent execution timed out after {agent_timeout_seconds} seconds"
                )
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
                CONTEXT_COLUMN: [
                    json.dumps(ctx) for ctx in contexts
                ],  # Serialize context to JSON string
            }
        )

        if not return_context:
            pred_df = pred_df.drop(columns=[CONTEXT_COLUMN])

        return pred_df

    def stream_agent(self, df: pd.DataFrame, agent_executor: AgentExecutor, args: Dict) -> Iterable[Dict]:
        base_template = args.get('prompt_template', args['prompt_template'])
        input_variables = re.findall(r"{{(.*?)}}", base_template)
        return_context = args.get('return_context', True)

        prompts, _ = prepare_prompts(df, base_template, input_variables, args.get('user_column', USER_COLUMN))

        callbacks, context_callback = prepare_callbacks(self, args)

        yield {"type": "start", "prompt": prompts[0]}

        if not hasattr(agent_executor, 'stream') or not callable(agent_executor.stream):
            raise AttributeError("The agent_executor does not have a 'stream' method")

        stream_iterator = agent_executor.stream(prompts[0], config={'callbacks': callbacks})

        if not hasattr(stream_iterator, '__iter__'):
            raise TypeError("The stream method did not return an iterable")

        for chunk in stream_iterator:
            logger.info(f'Processing streaming chunk {chunk}')
            processed_chunk = self.process_chunk(chunk)
            logger.info(f'Processed chunk: {processed_chunk}')
            yield processed_chunk

        if return_context:
            # Yield context if required
            captured_context = context_callback.get_contexts()
            if captured_context:
                yield {"type": "context", "content": captured_context}

        if self.log_callback_handler.generated_sql:
            # Yield generated SQL if available
            yield {"type": "sql", "content": self.log_callback_handler.generated_sql}

        if self.run_completion_span is not None:
            self.run_completion_span.end()
            self.api_trace.update()
            logger.info("Langfuse trace updated")

    @staticmethod
    def process_chunk(chunk):
        if isinstance(chunk, dict):
            return {k: LangchainAgent.process_chunk(v) for k, v in chunk.items()}
        if isinstance(chunk, list):
            return [LangchainAgent.process_chunk(item) for item in chunk]
        if isinstance(chunk, AgentAction):
            # Format agent actions properly for streaming.
            return {
                'tool': LangchainAgent.process_chunk(chunk.tool),
                'tool_input': LangchainAgent.process_chunk(chunk.tool_input),
                'log': LangchainAgent.process_chunk(chunk.log)
            }
        if isinstance(chunk, AgentStep):
            # Format agent steps properly for streaming.
            return {
                'action': LangchainAgent.process_chunk(chunk.action),
                'observation': LangchainAgent.process_chunk(chunk.observation) if chunk.observation else ''
            }
        if issubclass(chunk.__class__, BaseMessage):
            # Extract content from message subclasses properly for streaming.
            return {
                'content': chunk.content
            }
        if isinstance(chunk, (str, int, float, bool, type(None))):
            return chunk
        return str(chunk)
