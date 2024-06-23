from concurrent.futures import as_completed, TimeoutError
from typing import Optional, Dict, List
import os
import re

from langchain.agents import AgentExecutor
from langchain.agents.initialize import initialize_agent
from langchain.chains.conversation.memory import ConversationSummaryBufferMemory
from langchain.schema import SystemMessage
from langchain_community.chat_models import ChatAnthropic, ChatOpenAI, ChatAnyscale, ChatLiteLLM, ChatOllama
from langchain_core.prompts import PromptTemplate
from langfuse import Langfuse
from langfuse.callback import CallbackHandler

import numpy as np
import pandas as pd

from mindsdb.integrations.handlers.langchain_handler.constants import (
    ANTHROPIC_CHAT_MODELS,
    DEFAULT_AGENT_TIMEOUT_SECONDS,
    DEFAULT_AGENT_TOOLS,
    DEFAULT_AGENT_TYPE,
    DEFAULT_EMBEDDINGS_MODEL_PROVIDER,
    DEFAULT_MAX_ITERATIONS,
    DEFAULT_MAX_TOKENS,
    DEFAULT_MODEL_NAME,
    OLLAMA_CHAT_MODELS,
    SUPPORTED_PROVIDERS,
    DEFAULT_USER_COLUMN,
    DEFAULT_ASSISTANT_COLUMN
)
from mindsdb.integrations.handlers.langchain_handler.log_callback_handler import LogCallbackHandler
from mindsdb.integrations.handlers.langchain_handler.langfuse_callback_handler import LangfuseCallbackHandler
from mindsdb.integrations.utilities.rag.settings import DEFAULT_RAG_PROMPT_TEMPLATE
from mindsdb.integrations.handlers.langchain_handler.tools import setup_tools
from mindsdb.integrations.handlers.openai_handler.constants import CHAT_MODELS as OPEN_AI_CHAT_MODELS
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.llm.utils import get_llm_config
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.interfaces.storage.model_fs import HandlerStorage, ModelStorage
from mindsdb.integrations.handlers.langchain_embedding_handler.langchain_embedding_handler import construct_model_from_args
from mindsdb.utilities import log
from mindsdb.utilities.context_executor import ContextThreadPoolExecutor

from .mindsdb_chat_model import ChatMindsdb

_PARSING_ERROR_PREFIX = 'An output parsing error occured'

logger = log.getLogger(__name__)


class LangChainHandler(BaseMLEngine):
    """
    This is a MindsDB integration for the LangChain library, which provides a unified interface for interacting with
    various large language models (LLMs).

    Supported LLM providers:
        - OpenAI
        - Anthropic
        - Anyscale
        - LiteLLM
        - Ollama

    Supported standard tools:
        - python_repl
        - serper.dev search
    """
    name = 'langchain'

    def __init__(
            self,
            model_storage: ModelStorage,
            engine_storage: HandlerStorage,
            log_callback_handler: LogCallbackHandler = None,
            langfuse_callback_handler: CallbackHandler = None,
            **kwargs):
        super().__init__(model_storage, engine_storage, **kwargs)
        # if True, the target column name does not have to be specified at creation time.
        self.generative = True
        self.default_agent_tools = DEFAULT_AGENT_TOOLS
        self.log_callback_handler = log_callback_handler
        self.langfuse_callback_handler = langfuse_callback_handler
        if self.log_callback_handler is None:
            self.log_callback_handler = LogCallbackHandler(logger)

    def _get_llm_provider(self, args: Dict) -> str:
        if 'provider' in args:
            return args['provider']
        if args['model_name'] in ANTHROPIC_CHAT_MODELS:
            return 'anthropic'
        if args['model_name'] in OPEN_AI_CHAT_MODELS:
            return 'openai'
        if args['model_name'] in OLLAMA_CHAT_MODELS:
            return 'ollama'
        raise ValueError(f"Invalid model name. Please define provider")

    def _get_embedding_model_provider(self, args: Dict) -> str:
        if 'embedding_model_provider' in args:
            return args['embedding_model_provider']
        if 'embedding_model_provider' not in args:
            logger.warning('No embedding model provider specified. trying to use llm provider.')
            return args.get('embedding_model_provider', self._get_llm_provider(args))
        raise ValueError(f"Invalid model name. Please define provider")

    def _get_chat_model_params(self, args: Dict, pred_args: Dict) -> Dict:
        model_config = args.copy()
        # Override with prediction args.
        model_config.update(pred_args)
        # Include API keys.
        model_config['api_keys'] = {
            p: get_api_key(p, model_config, self.engine_storage, strict=False) for p in SUPPORTED_PROVIDERS
        }
        llm_config = get_llm_config(args.get('provider', self._get_llm_provider(args)), model_config)
        config_dict = llm_config.model_dump()
        config_dict = {k: v for k, v in config_dict.items() if v is not None}
        return config_dict

    def _get_agent_callbacks(self, args: Dict) -> List:
        all_callbacks = [self.log_callback_handler]
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
        if 'trace_id' not in args or 'observation_id' not in args:
            return all_callbacks
        # Trace LLM chains & tools using Langfuse.
        langfuse = Langfuse(
            public_key=os.getenv('LANGFUSE_PUBLIC_KEY'),
            secret_key=os.getenv('LANGFUSE_SECRET_KEY'),
            host=os.getenv('LANGFUSE_HOST')
        )
        langfuse_cb_handler = LangfuseCallbackHandler(langfuse, args['trace_id'], args['observation_id'])
        all_callbacks.append(langfuse_cb_handler)
        return all_callbacks

    def _create_chat_model(self, args: Dict, pred_args: Dict):
        model_kwargs = self._get_chat_model_params(args, pred_args)

        if args['provider'] == 'anthropic':
            return ChatAnthropic(**model_kwargs)
        if args['provider'] == 'openai':
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

    def _create_embeddings_model(self, args: Dict):
        return construct_model_from_args(args)

    def _handle_parsing_errors(self, error: Exception) -> str:
        response = str(error)
        if not response.startswith(_PARSING_ERROR_PREFIX):
            return f'Agent failed with error:\n{str(error)}...'
        else:
            # As a somewhat dirty workaround, we accept the output formatted incorrectly and use it as a response.
            #
            # Ideally, in the future, we would write a parser that is more robust and flexible than the one Langchain uses.
            # Response is wrapped in ``
            logger.info('Handling parsing error, salvaging response...')
            response_output = response.split('`')
            if len(response_output) >= 2:
                response = response_output[-2]
            return response

    def create(self, target: str, args: Dict = None, **kwargs):
        self.default_agent_tools = args.get('tools', self.default_agent_tools)

        args = args['using']
        args['target'] = target
        args['model_name'] = args.get('model_name', DEFAULT_MODEL_NAME)
        args['provider'] = args.get('provider', self._get_llm_provider(args))
        args['embedding_model_provider'] = args.get('embedding_model', self._get_embedding_model_provider(args))
        if args.get('mode') == 'retrieval':
            # use default prompt template for retrieval i.e. RAG if not provided
            if "prompt_template" not in args:
                args["prompt_template"] = DEFAULT_RAG_PROMPT_TEMPLATE

        self.model_storage.json_set('args', args)

    @staticmethod
    def create_validation(_, args: Dict=None, **kwargs):
        if 'using' not in args:
            raise Exception("LangChain engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']
        if 'prompt_template' not in args:
            if not args.get('mode') == 'retrieval':
                raise ValueError('Please provide a `prompt_template` for this engine.')

    def predict(self, df: pd.DataFrame, args: Dict=None) -> pd.DataFrame:
        """
        Dispatch is performed depending on the underlying model type. Currently, only the default text completion
        is supported.
        """
        pred_args = args['predict_params'] if args else {}
        args = self.model_storage.json_get('args')
        if 'prompt_template' not in args and 'prompt_template' not in pred_args:
            raise ValueError(f"This model expects a `prompt_template`, please provide one.")
        # Back compatibility for old models
        args['provider'] = args.get('provider', self._get_llm_provider(args))
        args['embedding_model_provider'] = args.get('embedding_model', self._get_embedding_model_provider(args))

        df = df.reset_index(drop=True)
        agent = self.create_agent(df, args, pred_args)
        # Use last message as prompt, remove other questions.
        user_column = args.get('user_column', DEFAULT_USER_COLUMN)
        df.iloc[:-1, df.columns.get_loc(user_column)] = None
        return self.run_agent(df, agent, args, pred_args)

    def create_agent(self, df: pd.DataFrame, args: Dict=None, pred_args: Dict=None) -> AgentExecutor:
        pred_args = pred_args if pred_args else {}

        # Set up tools.
        model_kwargs = self._get_chat_model_params(args, pred_args)
        llm = self._create_chat_model(args, pred_args)

        # Set up embeddings model if needed.
        if args.get('mode') == 'retrieval':
            embeddings_args = args.pop('embedding_model_args', {})

            # no embedding model args provided, use default provider.
            if not embeddings_args:
                embeddings_provider = self._get_embedding_model_provider(args)
                logger.warning("'embedding_model_args' not found in input params, "
                               f"Trying to use LLM provider: {embeddings_provider}"
                               )
                embeddings_args['class'] = embeddings_provider
                # Include API keys if present.
                embeddings_args.update({k: v for k, v in args.items() if 'api_key' in k})

            # create embeddings model
            pred_args['embeddings_model'] = self._create_embeddings_model(embeddings_args)
            pred_args['llm'] = llm

        tools = setup_tools(llm,
                            model_kwargs,
                            pred_args,
                            self.default_agent_tools)

        # Prefer prediction prompt template over original if provided.
        prompt_template = pred_args.get('prompt_template', args['prompt_template'])
        if 'context' in pred_args:
            prompt_template += '\n\n' + 'Useful information:\n' + pred_args['context'] + '\n'

        # Set up memory.
        memory = ConversationSummaryBufferMemory(llm=llm,
                                                 max_token_limit=model_kwargs.get('max_tokens', DEFAULT_MAX_TOKENS),
                                                 memory_key='chat_history')
        memory.chat_memory.messages.insert(0, SystemMessage(content=prompt_template))
        # User - Assistant conversation. All except the last message.
        user_column = args.get('user_column', DEFAULT_USER_COLUMN)
        assistant_column = args.get('assistant_column', DEFAULT_ASSISTANT_COLUMN)
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
            # Calls the agent’s LLM Chain one final time to generate a final answer based on the previous steps
            early_stopping_method='generate',
            handle_parsing_errors=self._handle_parsing_errors,
            # Timeout per agent invocation.
            max_execution_time=pred_args.get('timeout_seconds', args.get('timeout_seconds', DEFAULT_AGENT_TIMEOUT_SECONDS)),
            max_iterations=pred_args.get('max_iterations', args.get('max_iterations', DEFAULT_MAX_ITERATIONS)),
            memory=memory,
            verbose=pred_args.get('verbose', args.get('verbose', True))
        )
        return agent_executor

    def run_agent(self, df: pd.DataFrame, agent: AgentExecutor, args: Dict, pred_args: Dict) -> str:
        # Prefer prediction time prompt template, if available.
        base_template = pred_args.get('prompt_template', args['prompt_template'])

        input_variables = []
        matches = list(re.finditer("{{(.*?)}}", base_template))

        for m in matches:
            input_variables.append(m[0].replace('{', '').replace('}', ''))
        empty_prompt_ids = np.where(df[input_variables].isna().all(axis=1).values)[0]

        base_template = base_template.replace('{{', '{').replace('}}', '}')
        prompts = []

        user_column = args.get('user_column', DEFAULT_USER_COLUMN)
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
                all_args = args.copy()
                all_args.update(pred_args)
                answer = agent_executor.invoke(prompt, config={ 'callbacks': self._get_agent_callbacks(all_args) })
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

        pred_df = pd.DataFrame(completions, columns=[args['target']])

        return pred_df

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        tables = ['info']
        return pd.DataFrame(tables, columns=['tables'])

    def finetune(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        raise NotImplementedError('Fine-tuning is not supported for LangChain models')
