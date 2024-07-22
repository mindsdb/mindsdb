from concurrent.futures import as_completed, TimeoutError
from typing import Dict, List
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
from mindsdb.integrations.handlers.langchain_embedding_handler.langchain_embedding_handler import construct_model_from_args
from mindsdb.utilities import log
from mindsdb.utilities.context_executor import ContextThreadPoolExecutor
from mindsdb.interfaces.storage import db

from .mindsdb_chat_model import ChatMindsdb
from .log_callback_handler import LogCallbackHandler
from .langfuse_callback_handler import LangfuseCallbackHandler
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
        self.langfuse_callback_handler = None

    def get_completion(self, messages, trace_id, observation_id):
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

    def set_embedding_model(self, args):
        # Set up embeddings model if needed.

        embeddings_args = args.pop('embedding_model_args', {})

        # no embedding model args provided, use default provider.
        if not embeddings_args:
            embeddings_provider = get_embedding_model_provider(args)
            logger.warning("'embedding_model_args' not found in input params, "
                           f"Trying to use LLM provider: {embeddings_provider}"
                           )
            embeddings_args['class'] = embeddings_provider
            # Include API keys if present.
            embeddings_args.update({k: v for k, v in args.items() if 'api_key' in k})

        # create embeddings model
        self.embedding_model = construct_model_from_args(embeddings_args)

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
        all_callbacks = [LogCallbackHandler(logger)]
        are_langfuse_args_present = 'langfuse_public_key' in args and 'langfuse_secret_key' in args and 'langfuse_host' in args
        if self.langfuse_callback_handler is None and are_langfuse_args_present:
            self.langfuse_callback_handler = CallbackHandler(
                args['langfuse_public_key'],
                args['langfuse_secret_key'],
                host=args['langfuse_host']
            )
            # Check credentials.
            if not self.langfuse_callback_handler.auth_check():
                logger.error(f'Incorrect Langfuse credentials provided to Langchain handler. Full args: {args}')
        if self.langfuse_callback_handler is not None:
            all_callbacks.append(self.langfuse_callback_handler)
        if self.trace_id or self.observation_id is None:
            return all_callbacks
        # Trace LLM chains & tools using Langfuse.
        langfuse = Langfuse(
            public_key=os.getenv('LANGFUSE_PUBLIC_KEY'),
            secret_key=os.getenv('LANGFUSE_SECRET_KEY'),
            host=os.getenv('LANGFUSE_HOST')
        )
        langfuse_cb_handler = LangfuseCallbackHandler(langfuse, self.trace_id, self.observation_id)
        all_callbacks.append(langfuse_cb_handler)
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

    def run_agent(self, df: pd.DataFrame, agent: AgentExecutor, args: Dict) -> pd.DataFrame():
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
