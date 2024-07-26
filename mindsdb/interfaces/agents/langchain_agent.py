from concurrent.futures import as_completed, TimeoutError
from typing import Dict, Iterable, List
from uuid import uuid4
import os
import re

from langchain.agents import AgentExecutor
from langchain.agents.initialize import initialize_agent
from langchain.chains.conversation.memory import ConversationSummaryBufferMemory
from langchain.schema import SystemMessage
from langchain_community.chat_models import ChatAnthropic, ChatOpenAI, ChatAnyscale, ChatLiteLLM, ChatOllama
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import Tool
from langfuse import Langfuse
from langfuse.callback import CallbackHandler

import numpy as np
import pandas as pd

from mindsdb.integrations.handlers.openai_handler.constants import CHAT_MODELS as OPEN_AI_CHAT_MODELS
from mindsdb.integrations.libs.llm.utils import get_llm_config
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.utilities import log
from mindsdb.utilities.context_executor import ContextThreadPoolExecutor
from mindsdb.interfaces.storage import db

from .mindsdb_chat_model import ChatMindsdb
from .log_callback_handler import LogCallbackHandler
from .langfuse_callback_handler import LangfuseCallbackHandler, get_metadata, get_tags
from .tools import _build_retrieval_tool
from .safe_output_parser import SafeOutputParser

from .constants import (
    DEFAULT_AGENT_TIMEOUT_SECONDS,
    DEFAULT_AGENT_TYPE,
    DEFAULT_MAX_ITERATIONS,
    DEFAULT_MAX_TOKENS,
    SUPPORTED_PROVIDERS,
    ANTHROPIC_CHAT_MODELS,
    OLLAMA_CHAT_MODELS,
    USER_COLUMN,
    ASSISTANT_COLUMN
)
from ..skills.skill_tool import skill_tool, SkillType
from ...integrations.utilities.rag.settings import DEFAULT_RAG_PROMPT_TEMPLATE

_PARSING_ERROR_PREFIXES = ['An output parsing error occurred', 'Could not parse LLM output']

logger = log.getLogger(__name__)


def get_llm_provider(args: Dict) -> str:
    if 'provider' in args:
        return args['provider']
    if args['model_name'] in ANTHROPIC_CHAT_MODELS:
        return 'anthropic'
    if args['model_name'] in OPEN_AI_CHAT_MODELS:
        return 'openai'
    if args['model_name'] in OLLAMA_CHAT_MODELS:
        return 'ollama'
    raise ValueError("Invalid model name. Please define a supported llm provider")


def get_embedding_model_provider(args: Dict) -> str:
    if 'embedding_model_provider' in args:
        return args['embedding_model_provider']
    if 'embedding_model_provider' not in args:
        logger.warning('No embedding model provider specified. trying to use llm provider.')
        return args.get('embedding_model_provider', get_llm_provider(args))
    raise ValueError("Invalid model name. Please define provider")


def get_chat_model_params(args: Dict) -> Dict:
    model_config = args.copy()
    # Include API keys.
    model_config['api_keys'] = {
        p: get_api_key(p, model_config, None, strict=False) for p in SUPPORTED_PROVIDERS
    }
    llm_config = get_llm_config(args.get('provider', get_llm_provider(args)), model_config)
    config_dict = llm_config.model_dump()
    config_dict = {k: v for k, v in config_dict.items() if v is not None}
    return config_dict


def create_chat_model(args: Dict):
    model_kwargs = get_chat_model_params(args)

    def _get_tiktoken_model_name(model: str) -> str:
        if model.startswith('gpt-4'):
            return 'gpt-4'
        return model

    if args['provider'] == 'anthropic':
        return ChatAnthropic(**model_kwargs)
    if args['provider'] == 'openai':
        # Some newer GPT models (e.g. gpt-4o when released) don't have token counting support yet.
        # By setting this manually in ChatOpenAI, we count tokens like compatible GPT models.
        model_kwargs['tiktoken_model_name'] = _get_tiktoken_model_name(model_kwargs.get('model_name'))
        return ChatOpenAI(**model_kwargs)
    if args['provider'] == 'anyscale':
        return ChatAnyscale(**model_kwargs)
    if args['provider'] == 'litellm':
        return ChatLiteLLM(**model_kwargs)
    if args['provider'] == 'ollama':
        return ChatOllama(**model_kwargs)
    if args['provider'] == 'mindsdb':
        return ChatMindsdb(**model_kwargs)
    raise ValueError(f'Unknown provider: {args["provider"]}')


class LangchainAgent:
    def __init__(self, agent: db.Agents, model):

        self.llm = None
        self.embedding_model = None
        self.agent = agent
        args = agent.params.copy()
        args['model_name'] = agent.model_name
        args['provider'] = agent.provider
        args['embedding_model_provider'] = args.get('embedding_model', get_embedding_model_provider(args))

        # agent is using current langchain model
        if agent.provider == 'mindsdb':
            args['model_name'] = agent.model_name

            # get prompt
            prompt_template = model['problem_definition'].get('using', {}).get('prompt_template')
            if prompt_template is not None:
                # only update prompt_template if it is set on the model
                args['prompt_template'] = prompt_template

        if args.get('prompt_template') is None:
            if args.get('mode') == 'retrieval':
                args['prompt_template'] = DEFAULT_RAG_PROMPT_TEMPLATE
            else:
                raise ValueError('Please provide a `prompt_template` or set `mode=retrieval`')

        self.args = args
        self.trace_id = None
        self.observation_id = None
        self.log_callback_handler = None
        self.langfuse_callback_handler = None  # native langfuse callback handler
        self.mdb_langfuse_callback_handler = None  # custom (see langfuse_callback_handler.py)

    def get_completion(self, messages, trace_id, observation_id, stream: bool = False):
        if stream:
            return self._get_completion_stream(messages, trace_id, observation_id)
        self.trace_id = trace_id
        self.observation_id = observation_id

        args = self.args

        df = pd.DataFrame(messages)

        # Back compatibility for old models
        self.provider = args.get('provider', get_llm_provider(args))

        df = df.reset_index(drop=True)
        agent = self.create_agent(df, args)
        # Use last message as prompt, remove other questions.
        user_column = args.get('user_column', USER_COLUMN)
        df.iloc[:-1, df.columns.get_loc(user_column)] = None
        return self.run_agent(df, agent, args)

    def _get_completion_stream(self, messages: List[dict], trace_id: str, observation_id: str) -> Iterable[Dict]:
        '''
        Gets a completion as a stream of chunks from given messages.

        Args:
            messages (List[dict]): Messages to get completion chunks for
            trace_id (str): Langfuse trace ID to use
            observation_id (str): Langfuse parent observation Id to use

        Returns:
            chunks (Iterable[object]): Completion chunks
        '''
        self.trace_id = trace_id
        self.observation_id = observation_id

        args = self.args

        df = pd.DataFrame(messages)

        # Back compatibility for old models
        self.provider = args.get('provider', get_llm_provider(args))

        self.embedding_model_provider = args.get('embedding_model', get_embedding_model_provider(args))

        df = df.reset_index(drop=True)
        agent = self.create_agent(df, args)
        # Use last message as prompt, remove other questions.
        user_column = args.get('user_column', USER_COLUMN)
        df.iloc[:-1, df.columns.get_loc(user_column)] = None
        return self.stream_agent(df, agent, args)

    def create_agent(self, df: pd.DataFrame, args: Dict = None) -> AgentExecutor:
        # Set up tools.
        llm = create_chat_model(args)
        self.llm = llm
        if args.get('mode') == 'retrieval':
            self.set_embedding_model(args)
            self.args.pop('mode')

        tools = []
        skills = self.agent.skills or []
        for skill in skills:
            tools += self.langchain_tools_from_skill(skill, {}, llm)

        # Prefer prediction prompt template over original if provided.
        prompt_template = args['prompt_template']

        # Set up memory.
        memory = ConversationSummaryBufferMemory(llm=llm,
                                                 input_key='input',
                                                 output_key='output',
                                                 max_token_limit=args.get('max_tokens', DEFAULT_MAX_TOKENS),
                                                 memory_key='chat_history')
        memory.chat_memory.messages.insert(0, SystemMessage(content=prompt_template))
        # User - Assistant conversation. All except the last message.
        user_column = args.get('user_column', USER_COLUMN)
        assistant_column = args.get('assistant_column', ASSISTANT_COLUMN)
        for row in df[:-1].to_dict('records'):
            question = row[user_column]
            answer = row[assistant_column]
            if question:
                memory.chat_memory.add_user_message(question)
            if answer:
                memory.chat_memory.add_ai_message(answer)

        agent_type = args.get('agent_type', DEFAULT_AGENT_TYPE)
        agent_executor = initialize_agent(
            tools,
            llm,
            agent=agent_type,
            # Use custom output parser to handle flaky LLMs that don't ALWAYS conform to output format.
            agent_kwargs={'output_parser': SafeOutputParser()},
            # Calls the agent’s LLM Chain one final time to generate a final answer based on the previous steps
            early_stopping_method='generate',
            handle_parsing_errors=self._handle_parsing_errors,
            # Timeout per agent invocation.
            max_execution_time=args.get('timeout_seconds', args.get('timeout_seconds', DEFAULT_AGENT_TIMEOUT_SECONDS)),
            max_iterations=args.get('max_iterations', args.get('max_iterations', DEFAULT_MAX_ITERATIONS)),
            memory=memory,
            verbose=args.get('verbose', args.get('verbose', True))
        )
        return agent_executor

    def langchain_tools_from_skill(self, skill, pred_args, llm):
        # Makes Langchain compatible tools from a skill
        tools = skill_tool.get_tools_from_skill(skill, llm)

        all_tools = []
        for tool in tools:
            if skill.type == SkillType.RETRIEVAL.value:
                pred_args['embedding_model'] = self.embedding_model
                pred_args['llm'] = self.llm
                all_tools.append(_build_retrieval_tool(tool, pred_args, skill))
                continue
            if isinstance(tool, dict):
                all_tools.append(Tool(
                    name=tool['name'],
                    func=tool['func'],
                    description=tool['description'],
                    return_direct=True
                ))
                continue
            all_tools.append(tool)
        return all_tools

    def _get_agent_callbacks(self, args: Dict) -> List:

        if self.log_callback_handler is None:
            self.log_callback_handler = LogCallbackHandler(logger)

        all_callbacks = [self.log_callback_handler]

        langfuse_public_key = args.get('langfuse_public_key', os.getenv('LANGFUSE_PUBLIC_KEY'))
        langfuse_secret_key = args.get('langfuse_secret_key', os.getenv('LANGFUSE_SECRET_KEY'))
        langfuse_host = args.get('langfuse_host', os.getenv('LANGFUSE_HOST'))
        are_langfuse_args_present = bool(langfuse_public_key) and bool(langfuse_secret_key) and bool(langfuse_host)

        if are_langfuse_args_present:
            if self.langfuse_callback_handler is None:
                trace_name = args.get('trace_id',
                                      f'NativeTrace-...{self.trace_id[-7:]}' if self.trace_id is not None
                                      else 'NativeTrace-MindsDB-AgentExecutor')
                metadata = get_metadata(args)
                self.langfuse_callback_handler = CallbackHandler(
                    public_key=langfuse_public_key,
                    secret_key=langfuse_secret_key,
                    host=langfuse_host,
                    trace_name=trace_name,
                    tags=get_tags(metadata),
                    metadata=metadata,
                )
                if not self.langfuse_callback_handler.auth_check():
                    logger.error(f'Incorrect Langfuse credentials provided to Langchain handler. Full args: {args}')

            # custom tracer
            if self.mdb_langfuse_callback_handler is None:
                trace_id = args.get('trace_id', self.trace_id or None)
                observation_id = args.get('observation_id', self.observation_id or uuid4().hex)
                langfuse = Langfuse(
                    host=langfuse_host,
                    public_key=langfuse_public_key,
                    secret_key=langfuse_secret_key
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
                logger.info('Handling parsing error, salvaging response...')
                response_output = response.split('`')
                if len(response_output) >= 2:
                    response = response_output[-2]

                # Wrap response in Langchain conversational react format.
                langchain_react_formatted_response = f'''Thought: Do I need to use a tool? No
AI: {response}'''
                return langchain_react_formatted_response
        return f'Agent failed with error:\n{str(error)}...'

    def run_agent(self, df: pd.DataFrame, agent: AgentExecutor, args: Dict) -> pd.DataFrame:
        # Prefer prediction time prompt template, if available.
        base_template = args.get('prompt_template', args['prompt_template'])

        input_variables = []
        matches = list(re.finditer("{{(.*?)}}", base_template))

        for m in matches:
            input_variables.append(m[0].replace('{', '').replace('}', ''))
        empty_prompt_ids = np.where(df[input_variables].isna().all(axis=1).values)[0]

        base_template = base_template.replace('{{', '{').replace('}}', '}')
        prompts = []

        user_column = args.get('user_column', USER_COLUMN)
        for i, row in df.iterrows():
            if i not in empty_prompt_ids:
                prompt = PromptTemplate(input_variables=input_variables, template=base_template)
                kwargs = {}
                for col in input_variables:
                    kwargs[col] = row[col] if row[col] is not None else ''  # add empty quote if data is missing
                prompts.append(prompt.format(**kwargs))
            elif row.get(user_column):
                # Just add prompt
                prompts.append(row[user_column])

        def _invoke_agent_executor_with_prompt(agent_executor, prompt):
            if not prompt:
                return ''
            try:
                # Handle callbacks per run.
                answer = agent_executor.invoke(prompt, config={'callbacks': self._get_agent_callbacks(args)})
            except Exception as e:
                answer = str(e)
                if not answer.startswith("Could not parse LLM output: `"):
                    raise e
                answer = {'output': answer.removeprefix("Could not parse LLM output: `").removesuffix("`")}

            if 'output' not in answer:
                # This should never happen unless Langchain changes invoke output format, but just in case.
                return agent_executor.run(prompt)
            return answer['output']

        completions = []
        # max_workers defaults to number of processors on the machine multiplied by 5.
        # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
        max_workers = args.get('max_workers', None)
        agent_timeout_seconds = args.get('timeout', DEFAULT_AGENT_TIMEOUT_SECONDS)
        executor = ContextThreadPoolExecutor(max_workers=max_workers)
        futures = [executor.submit(_invoke_agent_executor_with_prompt, agent, prompt) for prompt in prompts]
        try:
            for future in as_completed(futures, timeout=agent_timeout_seconds):
                completions.append(future.result())
        except TimeoutError:
            completions.append("I'm sorry! I couldn't come up with a response in time. Please try again.")
        # Can't use ThreadPoolExecutor as context manager since we need wait=False.
        executor.shutdown(wait=False)

        # Add null completion for empty prompts
        for i in sorted(empty_prompt_ids)[:-1]:
            completions.insert(i, None)

        pred_df = pd.DataFrame(completions, columns=[ASSISTANT_COLUMN])

        return pred_df

    def stream_agent(self, df: pd.DataFrame, agent_executor: AgentExecutor, args: Dict) -> Iterable[Dict]:
        '''
        Streams completion chunks for an agent.

        Args:
            df (pd.DataFrame): DataFrame to use as messages input
            agent_executor (AgentExecutor): Executor to use for streaming agent
            args (Dict): Args to pass to agent

        Returns:
            chunks (Iterable[Dict]): Completion chunks for agent
        '''
        # Prefer prediction time prompt template, if available.
        base_template = args.get('prompt_template', args['prompt_template'])
        input_variables = []
        matches = list(re.finditer("{{(.*?)}}", base_template))

        for m in matches:
            input_variables.append(m[0].replace('{', '').replace('}', ''))

        base_template = base_template.replace('{{', '{').replace('}}', '}')
        first_row = df.iloc[0]
        prompt_template = PromptTemplate(input_variables=input_variables, template=base_template)
        kwargs = {}
        for col in input_variables:
            kwargs[col] = first_row[col] if first_row[col] is not None else ''  # add empty quote if data is missing
        prompt = prompt_template.format(**kwargs)
        if not prompt:
            return
        for chunk in agent_executor.stream(prompt, config={'callbacks': self._get_agent_callbacks(args)}):
            yield chunk
