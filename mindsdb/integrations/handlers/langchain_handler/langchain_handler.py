import re
import os
from typing import Optional, Dict

from concurrent.futures import as_completed, TimeoutError
import numpy as np
import pandas as pd

from langchain.schema import SystemMessage
from langchain.agents import AgentType
from langchain_community.llms import OpenAI
from langchain_community.chat_models import ChatAnthropic, ChatOpenAI, ChatAnyscale, ChatLiteLLM
from langchain.agents import initialize_agent, create_sql_agent  # TODO: initialize_agent is deprecated, replace with e.g. `create_react_agent`  # noqa
from langchain.prompts import PromptTemplate
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain.chains.conversation.memory import ConversationSummaryBufferMemory

from langfuse.callback import CallbackHandler

from mindsdb.integrations.handlers.openai_handler.constants import CHAT_MODELS as OPEN_AI_CHAT_MODELS
from mindsdb.integrations.handlers.langchain_handler.mindsdb_database_agent import MindsDBSQL
from mindsdb.integrations.handlers.langchain_handler.tools import setup_tools
from mindsdb.integrations.handlers.langchain_handler.log_callback_handler import LogCallbackHandler
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.interfaces.storage.model_fs import HandlerStorage, ModelStorage
from mindsdb.utilities import log
from mindsdb.utilities.context_executor import ContextThreadPoolExecutor



# Default to latest GPT-4 model (https://platform.openai.com/docs/models/gpt-4-and-gpt-4-turbo)
_DEFAULT_MODEL = 'gpt-4-0125-preview'
_DEFAULT_MAX_ITERATIONS = 10
_DEFAULT_MAX_TOKENS = 2048  # requires more than vanilla OpenAI due to ongoing summarization and 3rd party input
# 2 minutes should be more than enough time to complete chains.
_DEFAULT_AGENT_TIMEOUT_SECONDS = 120
_DEFAULT_AGENT_MODEL = 'zero-shot-react-description'
_DEFAULT_AGENT_TOOLS = ['wikipedia']  # these require no additional arguments
_ANTHROPIC_CHAT_MODELS = {'claude-2', 'claude-instant-1'}
_PARSING_ERROR_PREFIX = 'Could not parse LLM output'

logger = log.getLogger(__name__)


class LangChainHandler(BaseMLEngine):
    """
    This is a MindsDB integration for the LangChain library, which provides a unified interface for interacting with
    various large language models (LLMs).

    Currently, this integration supports exposing OpenAI and Anthrophic's LLMs with normal text completion support. They are then
    wrapped in a zero shot react description agent that offers a few third party tools out of the box, with support
    for additional ones if an API key is provided. Ongoing memory is also provided.

    Full tool support list:
        - wikipedia
        - python_repl
        - serper.dev search
    """
    name = 'langchain'

    def __init__(
            self,
            model_storage: ModelStorage,
            engine_storage: HandlerStorage,
            callback_handler: LogCallbackHandler = None,
            **kwargs):
        super().__init__(model_storage, engine_storage, **kwargs)
        self.generative = True
        self.stops = []
        self.default_mode = 'default'  # can also be 'conversational' or 'conversational-full'
        self.engine_to_supported_modes = {
            'openai': ['default', 'conversational', 'conversational-full', 'image'],
            'anthropic': ['default', 'conversational', 'conversational-full'],
            'anyscale': ['default', 'conversational'],
            'litellm': ['default', 'conversational'],
        }
        self.default_model = _DEFAULT_MODEL
        self.default_max_tokens = _DEFAULT_MAX_TOKENS
        self.default_agent_model = _DEFAULT_AGENT_MODEL
        self.default_agent_tools = _DEFAULT_AGENT_TOOLS
        self.log_callback_handler = callback_handler
        if self.log_callback_handler is None:
            self.log_callback_handler = LogCallbackHandler(logger)

    # TODO (ref #7496): modify handler_utils.get_api_key to check for prefix in all sources, update usage in all handlers, deprecate  # noqa
    def _get_serper_api_key(self, args, strict=True):
        if 'serper_api_key' in args:
            return args['serper_api_key']
        # 2
        connection_args = self.engine_storage.get_connection_args()
        if 'serper_api_key' in connection_args:
            return connection_args['serper_api_key']
        # 3
        api_key = os.getenv('SERPER_API_KEY')  # e.g. "OPENAI_API_KEY"
        if api_key is not None:
            return api_key

        if strict:
            raise Exception(f'Missing API key serper_api_key. Either re-create this ML_ENGINE specifying the `serper_api_key` parameter,\
                 or re-create this model and pass the API key with `USING` syntax.')  # noqa

    def create(self, target, args=None, **kwargs):
        self.default_agent_tools = args.get('tools', self.default_agent_tools)

        args = args['using']
        args['target'] = target
        
        if not args.get('model_name'):
            args['model_name'] = self.default_model

        if not args.get('mode'):
            args['mode'] = self.default_mode

        if args.get('provider') is None:
            if args['model_name'] in _ANTHROPIC_CHAT_MODELS:
                args['provider'] = 'anthropic'
            elif args['model_name'] in OPEN_AI_CHAT_MODELS:
                args['provider'] = 'openai'
            else:
                raise Exception(f"Invalid provider name. Please define provider")

        supported_modes = self.engine_to_supported_modes[args.get('provider')]

        if args['mode'] not in supported_modes:
            raise Exception(f"Invalid operation mode. Please use one of {supported_modes}")

        self.model_storage.json_set('args', args)

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'using' not in args:
            raise Exception("LangChain engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

        if args.get('mode') != 'conversational':
            if len(set(args.keys()) & {'prompt_template'}) == 0:
                raise Exception('Please provide a `prompt_template` for this engine.')

    def predict(self, df, args=None):
        """
        Dispatch is performed depending on the underlying model type. Currently, only the default text completion
        is supported.
        """
        pred_args = args['predict_params'] if args else {}
        args = self.model_storage.json_get('args')

        # back compatibility for old models
        if args.get('provider') is None:
            if args['model_name'] in _ANTHROPIC_CHAT_MODELS:
                args['provider'] = 'anthropic'
            else:
                args['provider'] = 'openai'

        df = df.reset_index(drop=True)

        if args.get('mode') != 'conversational':
            if 'prompt_template' not in args and 'prompt_template' not in pred_args:
                raise Exception(f"This model expects a prompt template, please provide one.")

        if 'stops' in pred_args:
            self.stops = pred_args['stops']

        # TODO: offload creation to the `create` method instead for faster inference?

        modal_dispatch = {
            'default': 'default_completion',
            'sql_agent': 'sql_agent_completion',
        }

        agent_creation_method = modal_dispatch.get(args.get('modal_dispatch', 'default'), 'default_completion')

        if args.get('mode') == 'conversational':
            agent_creation_method = 'conversational_completion'

        # input dataframe can be modified
        agent, df = getattr(self, agent_creation_method)(df, args, pred_args)
        return self.run_agent(df, agent, args, pred_args)

    def _get_chat_model_params(self, args, pred_args):
        model_name = args.get('model_name', self.default_model)
        # Params shared by all models.
        temperature = min(1.0, max(0.0, args.get('temperature', 0.0)))
        max_tokens = pred_args.get('max_tokens', args.get('max_tokens', self.default_max_tokens))
        top_p = pred_args.get('top_p', None)
        timeout = pred_args.get('request_timeout', None)
        serper_api_key = self._get_serper_api_key(args, strict=False)
        model_kwargs = {}
        provider = args.get('provider', 'openai')
        if provider == 'anthropic':
            model_kwargs['model'] = model_name
            model_kwargs['temperature'] = temperature
            model_kwargs['max_tokens_to_sample'] = max_tokens
            model_kwargs['top_p'] = top_p
            model_kwargs['timeout'] = timeout
            model_kwargs['stop_sequences'] = pred_args.get('stop_sequences', None)
            model_kwargs['serper_api_key'] = serper_api_key
            model_kwargs['anthropic_api_key'] = get_api_key('anthropic', args, self.engine_storage)
        elif provider == 'litellm':
            model_kwargs['model_name'] = model_name
            model_kwargs['temperature'] = temperature
            model_kwargs['max_tokens'] = max_tokens
            model_kwargs['serper_api_key'] = serper_api_key
            model_kwargs['n'] = pred_args.get('n', None)
            model_kwargs['api_base'] = args.get('base_url', None)
            model_kwargs['best_of'] = pred_args.get('best_of', None)
            model_kwargs['custom_llm_provider'] = 'openai'
            model_kwargs['model_kwargs'] = {
                'api_key': get_api_key(provider, args, self.engine_storage),
                'top_p': top_p,
                'request_timeout': timeout,
                'frequency_penalty': pred_args.get('frequency_penalty', None),
                'presence_penalty': pred_args.get('presence_penalty', None),
                'logit_bias': pred_args.get('logit_bias', None),
            }
        elif provider in ('openai', 'anyscale'):
            # OpenAI compatible
            base_url = args.get('base_url', None)
            if provider == 'anyscale' and base_url is None:
                base_url = "https://api.endpoints.anyscale.com/v1"
            model_kwargs['model_name'] = model_name
            model_kwargs['temperature'] = temperature
            model_kwargs['max_tokens'] = max_tokens
            model_kwargs['top_p'] = top_p
            model_kwargs['request_timeout'] = timeout
            model_kwargs['serper_api_key'] = serper_api_key
            model_kwargs['frequency_penalty'] = pred_args.get('frequency_penalty', None)
            model_kwargs['presence_penalty'] = pred_args.get('presence_penalty', None)
            model_kwargs['n'] = pred_args.get('n', None)
            model_kwargs['best_of'] = pred_args.get('best_of', None)
            model_kwargs['logit_bias'] = pred_args.get('logit_bias', None)
            model_kwargs[f'{provider}_api_base'] = base_url
            model_kwargs[f'{provider}_api_key'] = get_api_key(provider, args, self.engine_storage)
            model_kwargs[f'{provider}_organization'] = args.get('api_organization', None)

        model_kwargs = {k: v for k, v in model_kwargs.items() if v is not None}  # filter out None values
        return model_kwargs

    def _create_chat_model(self, args, pred_args):
        model_kwargs = self._get_chat_model_params(args, pred_args)

        if args['provider'] == 'anthropic':
            return ChatAnthropic(**model_kwargs)
        elif args['provider'] == 'openai':
            return ChatOpenAI(**model_kwargs)
        elif args['provider'] == 'anyscale':
            return ChatAnyscale(**model_kwargs)
        elif args['provider'] == 'litellm':
            return ChatLiteLLM(**model_kwargs)

    def conversational_completion(self, df, args=None, pred_args=None):
        pred_args = pred_args if pred_args else {}

        # langchain tool setup
        model_kwargs = self._get_chat_model_params(args, pred_args)
        llm = self._create_chat_model(args, pred_args)
        max_tokens = pred_args.get('max_tokens', args.get('max_tokens', self.default_max_tokens))
        tools = setup_tools(llm,
                            model_kwargs,
                            pred_args,
                            self.default_agent_tools)

        memory = ConversationSummaryBufferMemory(llm=llm,
                                                 max_token_limit=max_tokens,
                                                 memory_key="chat_history")

        # fill memory

        # system prompt
        prompt = args['prompt_template']
        if 'prompt_template' in pred_args and pred_args['prompt_template'] is not None:
            prompt = pred_args['prompt_template']
        if 'context' in pred_args:
            prompt += '\n\n' + 'Useful information:\n' + pred_args['context'] + '\n'
        memory.chat_memory.messages.insert(0, SystemMessage(content=prompt))

        # user - assistant conversation. get all except the last message
        for row in df[:-1].to_dict('records'):
            question = row[args['user_column']]
            answer = row[args['assistant_column']]

            if question:
                memory.chat_memory.add_user_message(question)
            if answer:
                memory.chat_memory.add_ai_message(answer)

        # use last message as prompt, remove other questions
        df.iloc[:-1, df.columns.get_loc(args['user_column'])] = ''

        agent_name = AgentType.CONVERSATIONAL_REACT_DESCRIPTION
        agent_executor = initialize_agent(
            tools,
            llm,
            memory=memory,
            agent=agent_name,
            callbacks=[self.log_callback_handler],
            # Calls the agent’s LLM Chain one final time to generate a final answer based on the previous steps
            early_stopping_method='generate',
            handle_parsing_errors=self._handle_parsing_errors,
            # Timeout per agent invocation.
            max_execution_time=pred_args.get('timeout_seconds', args.get('timeout_seconds', _DEFAULT_AGENT_TIMEOUT_SECONDS)),
            max_iterations=pred_args.get('max_iterations', args.get('max_iterations', _DEFAULT_MAX_ITERATIONS)),
            verbose=pred_args.get('verbose', args.get('verbose', False)),
        )

        # setup model description
        description = {
            'allowed_tools': [agent_executor.agent.allowed_tools],  # packed as list to avoid additional rows
            'agent_type': agent_name,
            'max_iterations': agent_executor.max_iterations,
            'memory_type': memory.__class__.__name__,
        }

        description = {**description, **model_kwargs}
        description.pop('openai_api_key', None)
        self.model_storage.json_set('description', description)

        return agent_executor, df

    def default_completion(self, df, args=None, pred_args=None):
        """
        Mostly follows the logic of the OpenAI handler, but with a few additions:
            - setup the langchain toolkit
            - setup the langchain agent (memory included)
            - setup information to be published when describing the model

        Ref link from the LangChain documentation on how to accomplish the first two items:
            - python.langchain.com/en/latest/modules/agents/agents/custom_agent.html
        """
        pred_args = pred_args if pred_args else {}
        pred_args['tools'] = args.get('tools') if 'tools' not in pred_args else pred_args.get('tools', [])

        # langchain tool setup
        llm = self._create_chat_model(args, pred_args)
        max_tokens = pred_args.get('max_tokens', args.get('max_tokens', self.default_max_tokens))
        model_kwargs = self._get_chat_model_params(args, pred_args)
        tools = setup_tools(llm,
                            model_kwargs,
                            pred_args,
                            self.default_agent_tools)

        # langchain agent setup
        memory = ConversationSummaryBufferMemory(llm=llm, max_token_limit=max_tokens)
        agent_name = pred_args.get('agent_name', args.get('agent_name', self.default_agent_model))
        agent_executor = initialize_agent(
            tools,
            llm,
            memory=memory,
            agent=agent_name,
        )

        # setup model description
        description = {
            'allowed_tools': [agent_executor.agent.allowed_tools],   # packed as list to avoid additional rows
            'agent_type': agent_name,
            'max_iterations': agent_executor.max_iterations,
            'memory_type': memory.__class__.__name__,
        }
        description = {**description, **model_kwargs}
        description.pop('openai_api_key', None)
        self.model_storage.json_set('description', description)

        return agent_executor, df
    
    def _handle_parsing_errors(self, error: Exception) -> str:
        response = str(error)
        if not response.startswith(_PARSING_ERROR_PREFIX):
            return f'Agent failed with error:\n{str(error)}...'
        else:
            # Some OpenAI models always output a response formatted correctly. Anthropic, and others, sometimes just output
            # the answer as text (when not using tools), even when prompted to output in a specific format.
            # As a somewhat dirty workaround, we accept the output formatted incorrectly and use it as a response.
            #
            # Ideally, in the future, we would write a parser that is more robust and flexible than the one Langchain uses.
            # Response is wrapped in ``
            logger.info('Handling parsing error, salvaging response...')
            response_output = response.split('`')
            if len(response_output) >= 2:
                response = response_output[-2]
            return response

    def run_agent(self, df, agent, args, pred_args):
        # TODO abstract prompt templating into a common utility method, this is also used in vanilla OpenAI
        if 'prompt_template' in pred_args:
            base_template = pred_args['prompt_template']   # override with predict-time template if available
        elif 'prompt_template' in args:
            base_template = args['prompt_template']  # use create-time template if not
        else:
            base_template = '{{question}}'  # default template otherwise

        input_variables = []
        matches = list(re.finditer("{{(.*?)}}", base_template))

        for m in matches:
            input_variables.append(m[0].replace('{', '').replace('}', ''))
        empty_prompt_ids = np.where(df[input_variables].isna().all(axis=1).values)[0]

        base_template = base_template.replace('{{', '{').replace('}}', '}')
        prompts = []

        for i, row in df.iterrows():
            if i not in empty_prompt_ids:
                prompt = PromptTemplate(input_variables=input_variables, template=base_template)
                kwargs = {}
                for col in input_variables:
                    kwargs[col] = row[col] if row[col] is not None else ''  # add empty quote if data is missing
                prompts.append(prompt.format(**kwargs))
            elif row.get(args['user_column']):
                # just add prompt
                prompts.append(row[args['user_column']])

        def _invoke_agent_executor_with_prompt(agent_executor, prompt):
            # TODO: ensure that agent completion plus prompt match the maximum allowed by the user
            if not prompt:
                return ''

            callbacks = []
            if 'langfuse_public_key' in args and 'langfuse_secret_key' in args and 'langfuse_host' in args:
                callbacks.append(
                    CallbackHandler(
                        args['langfuse_public_key'],
                        args['langfuse_secret_key'],
                        host=args['langfuse_host'],
                    )
                )
            answer = agent_executor.invoke(prompt, callbacks=callbacks)
            if 'output' not in answer:
                # This should never happen unless Langchain changes invoke output format, but just in case.
                return agent_executor.run(prompt)
            return answer['output']

        completions = []
        # max_workers defaults to number of processors on the machine multiplied by 5.
        # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
        max_workers = args.get('max_workers', None)
        agent_timeout_seconds = args.get('timeout', _DEFAULT_AGENT_TIMEOUT_SECONDS)
        executor = ContextThreadPoolExecutor(max_workers=max_workers)
        futures = [executor.submit(_invoke_agent_executor_with_prompt, agent, prompt) for prompt in prompts]
        try:
            for future in as_completed(futures, timeout=agent_timeout_seconds):
                completions.append(future.result())
        except TimeoutError:
            completions.append("I'm sorry! I couldn't come up with a response in time. Please try again.")
        # Can't use ThreadPoolExecutor as context manager since we need wait=False.
        executor.shutdown(wait=False)

        # add null completion for empty prompts
        for i in sorted(empty_prompt_ids):
            completions.insert(i, None)

        pred_df = pd.DataFrame(completions, columns=[args['target']])

        return pred_df

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        info = self.model_storage.json_get('description')

        if attribute == 'info':
            if info is None:
                # we do this due to the huge amount of params that can be changed
                #  at prediction time to customize behavior.
                # for them, we report the last observed value
                raise Exception('This model needs to be used before it can be described.')

            description = pd.DataFrame(info)
            return description
        else:
            tables = ['info']
            return pd.DataFrame(tables, columns=['tables'])

    def finetune(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        raise NotImplementedError('Fine-tuning is not supported for LangChain models')

    def sql_agent_completion(self, df, args=None, pred_args=None):
        """This completion will be used to answer based on information passed by any MindsDB DB or API engine."""
        db = MindsDBSQL()
        model_name = args.get('model_name', self.default_model)
        llm = OpenAI(temperature=0) if model_name not in OPEN_AI_CHAT_MODELS else self._create_chat_model(args, pred_args)  # noqa
        toolkit = SQLDatabaseToolkit(db=db, llm=llm)
        agent = create_sql_agent(
            llm=llm,
            toolkit=toolkit,
            verbose=pred_args.get('verbose', args.get('verbose', False))
        )
        return agent, df
