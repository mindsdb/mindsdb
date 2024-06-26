from typing import Optional, Dict, List
import dill
import dspy


# from langchain_community.chat_models import ChatAnthropic, ChatOpenAI, ChatAnyscale, ChatLiteLLM, ChatOllama
from dspy import ColBERTv2
from dspy.teleprompt import BootstrapFewShot
from dspy.evaluate import Evaluate

from mindsdb.interfaces.llm.llm_controller import LLMDataController
import pandas as pd


from mindsdb.integrations.handlers.dspy_handler.constants import (
    ANTHROPIC_CHAT_MODELS,
    DEFAULT_MODEL_NAME,
    OLLAMA_CHAT_MODELS,
)
from mindsdb.integrations.handlers.dspy_handler.log_callback_handler import LogCallbackHandler
from mindsdb.integrations.utilities.rag.settings import DEFAULT_RAG_PROMPT_TEMPLATE
from mindsdb.integrations.handlers.openai_handler.constants import CHAT_MODELS as OPEN_AI_CHAT_MODELS
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.llm.utils import get_llm_config
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.interfaces.storage.model_fs import HandlerStorage, ModelStorage
from mindsdb.integrations.handlers.langchain_embedding_handler.langchain_embedding_handler import construct_model_from_args
from mindsdb.utilities import log

# from .mindsdb_chat_model import ChatMindsdb

_PARSING_ERROR_PREFIX = 'An output parsing error occured'

logger = log.getLogger(__name__)


class DSPyHandler(BaseMLEngine):
    """
    This is a MindsDB integration for the DSPy library, which provides a unified interface for interacting with
    various large language models (LLMs) and self improving prompts for these LLMs.

    Supported LLM providers:
        - OpenAI

    Supported standard tools:
        - python_repl
        - serper.dev search
    """
    name = 'dspy'

    def __init__(
            self,
            model_storage: ModelStorage,
            engine_storage: HandlerStorage,
            log_callback_handler: LogCallbackHandler = None,
            **kwargs):
        super().__init__(model_storage, engine_storage, **kwargs)
        # if True, the target column name does not have to be specified at creation time.
        self.generative = True
        self.log_callback_handler = log_callback_handler
        self.llm_data_controller = LLMDataController()
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

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Dict = None, **kwargs):

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
        self.model_storage.file_set("cold_start_df", dill.dumps(df.to_dict()))
            # TODO: temporal workaround: serialize df and args, instead. And recreate chain (with training) every inference call.
            # ideally, we serialize the chain itself to avoid duplicate training.

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

        # retrives llm and pass it around as context
        model = args.get('model_name')
        api_key = get_api_key('openai', args, self.engine_storage, strict=False)
        # llm = dspy.OpenAI(model=model, api_key=api_key, api_base = 'https://llm.mdb.ai')
        llm = dspy.OpenAI(model=model, api_key=api_key)

        df = df.reset_index(drop=True)

        # self.llm_data_controller.delete_llm_data(0)
        cold_start_df = pd.DataFrame(dill.loads(self.model_storage.file_get("cold_start_df")))  # fixed in "training"  # noqa

        # gets larger as agent is used more
        self_improvement_df = pd.DataFrame(self.llm_data_controller.list_all_llm_data(2))
        self_improvement_df = self_improvement_df.rename(columns={
            'output': args['target'],
            'input': args['user_column']
        })
        self_improvement_df = self_improvement_df.tail(25)

        # add cold start DF
        self_improvement_df = pd.concat([cold_start_df, self_improvement_df]).reset_index(drop=True)

        chain = self.setup_dspy(self_improvement_df, args)
        output = self.predict_dspy(df, args, chain, llm)  # this stores new traces for self-improvement
        return output

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        tables = ['info']
        return pd.DataFrame(tables, columns=['tables'])

    def finetune(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        raise NotImplementedError('Fine-tuning is not supported for LangChain models')
    
    def setup_dspy(self, df, args):
        # This is the default language model and retrieval model in DSPy
        colbertv2 = ColBERTv2(url='http://20.102.90.50:2017/wiki17_abstracts')
        dspy.configure(rm=colbertv2)
        dspy_chain = self.create_dspy_chain(df, args)
        return dspy_chain
    
    def create_dspy_chain(self, df, args):
        # Initialize the LLM with the API key
        model = args.get('model_name')
        api_key = get_api_key(model, args, self.engine_storage, strict=False)
        # llm = dspy.OpenAI(model=model, api_key=api_key, api_base = 'https://llm.mdb.ai')
        llm = dspy.OpenAI(model=model, api_key=api_key)

        # Convert to DSPy Module
        with dspy.context(lm=llm):
            dspy_module = dspy.ReAct(f'{args["user_column"]} -> {args["target"]}')

        # create a list of DSPy examples
        dspy_examples = []

        # TODO: maybe random choose a fixed set of rows
        for i, row in df.iterrows():
            example = dspy.Example(
                question=row[args["user_column"]],
                answer=row[args["target"]]
            ).with_inputs(args["user_column"])
            dspy_examples.append(example)

        # TODO: add the optimizer, maybe the metric
        config = dict(max_bootstrapped_demos=4, max_labeled_demos=4)
        metric = dspy.evaluate.metrics.answer_exact_match  # TODO: passage match requires context from prediction... we'll probably modify the signature of ReAct
        teleprompter = BootstrapFewShot(metric=metric, **config)  # TODO: maybe it's better to have this persisted so that the internal state does a better job at optimizing RAG
        with dspy.context(lm=llm):
            optimized = teleprompter.compile(dspy_module, trainset=dspy_examples)  # TODO: check columns have the right name
        return optimized
    
    def generate_dspy_response(self, question, chain, llm):
        input_dict = {"question": question}
        with dspy.context(lm=llm):
            response = chain(question=input_dict['question'])
        return response.answer

    def predict_dspy(self,
                     df: pd.DataFrame,
                     args: Dict,
                     chain,  # TODO: specify actual type
                     llm,
            ) -> pd.DataFrame:
        responses = []
        for index, row in df.iterrows():
            question = row[args['user_column']]
            answer = self.generate_dspy_response(question, chain, llm)
            responses.append({'answer': answer, 'question': question})  # TODO: check that columns are right here
            # TODO: check this only adds new incoming rows
            self.llm_data_controller.add_llm_data(question, answer, 2)  # stores new traces for use in new calls

        # Set up the evaluator, which can be used multiple times.
        # TODO: use this in the EVALUATE command
        # evaluate = Evaluate(devset=gsm8k_devset, metric=gsm8k_metric, num_threads=4, display_progress=True, display_table=0)

        # Evaluate our `optimized_cot` program.
        # evaluate(optimized_cot)

        return pd.DataFrame(responses)
