from typing import Optional, Dict
import dill
import dspy


from dspy import ColBERTv2
from dspy.teleprompt import BootstrapFewShot

from mindsdb.interfaces.llm.llm_controller import LLMDataController
import pandas as pd
from mindsdb.interfaces.agents.constants import (
    DEFAULT_MODEL_NAME
)
from mindsdb.interfaces.agents.langchain_agent import (
    get_llm_provider, get_embedding_model_provider
)
from mindsdb.integrations.utilities.rag.settings import DEFAULT_RAG_PROMPT_TEMPLATE
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.interfaces.storage.model_fs import HandlerStorage, ModelStorage
from mindsdb.utilities import log


_PARSING_ERROR_PREFIX = 'An output parsing error occured'

logger = log.getLogger(__name__)

START_URL = 'http://20.102.90.50:2017/wiki17_abstracts'
DEMOS = 4
DF_EXAMPLES = 25
PRIME_MULTIPLIER = 31


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
            **kwargs):
        super().__init__(model_storage, engine_storage, **kwargs)
        # if True, the target column name does not have to be specified at creation time.
        self.generative = True
        self.llm_data_controller = LLMDataController()
        self.model_id = 0

    def calculate_model_id(self, model_name):
        '''
        Based on the model name calculate a unique number to be used as model_id
        '''
        sum_chars = sum((i + 1) * ord(char) * PRIME_MULTIPLIER for i, char in enumerate(model_name))
        return sum_chars

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Dict = None, **kwargs):
        """
        Create a model by initializing the parameters and setting up a DSPy chain with cold start data

        Args:
            target (str): Type of engine
            df (DataFrame): cold start df for DSPy
            args (Dict): Parameters for the model

        Returns:
            None
        """

        args = args['using']
        args['target'] = target
        args['model_name'] = args.get('model_name', DEFAULT_MODEL_NAME)
        args['provider'] = args.get('provider', get_llm_provider(args))
        args['embedding_model_provider'] = args.get('embedding_model', get_embedding_model_provider(args))
        if args.get('mode') == 'retrieval':
            # use default prompt template for retrieval i.e. RAG if not provided
            if "prompt_template" not in args:
                args["prompt_template"] = DEFAULT_RAG_PROMPT_TEMPLATE

        self.model_storage.json_set('args', args)
        if not isinstance(df, type(None)):
            self.model_storage.file_set('cold_start_df', dill.dumps(df.to_dict()))
        else:
            # no cold start df if data not provided
            self.model_storage.file_set('cold_start_df', dill.dumps({}))
        # TODO: temporal workaround: serialize df and args, instead. And recreate chain (with training) every inference call.
        # ideally, we serialize the chain itself to avoid duplicate training.

    def predict(self, df: pd.DataFrame, args: Dict = None) -> pd.DataFrame:
        """
        Predicts a response using DSPy

        Args:
            df (DataFrame): input for the model
            args (Dict): Parameters for the model

        Returns:
            df (DataFrame): response from the model
        """
        pred_args = args['predict_params'] if args else {}
        args = self.model_storage.json_get('args')
        if 'prompt_template' not in args and 'prompt_template' not in pred_args:
            raise ValueError("This model expects a `prompt_template`, please provide one.")
        # Back compatibility for old models
        args['provider'] = args.get('provider', get_llm_provider(args))
        args['embedding_model_provider'] = args.get('embedding_model', get_embedding_model_provider(args))

        # retrieves llm and pass it around as context
        model = args.get('model_name')
        self.model_id = self.calculate_model_id(model)
        api_key = get_api_key('openai', args, self.engine_storage, strict=False)
        llm = dspy.OpenAI(model=model, api_key=api_key)

        df = df.reset_index(drop=True)

        cold_start_df = pd.DataFrame(dill.loads(self.model_storage.file_get("cold_start_df")))  # fixed in "training"  # noqa
        # gets larger as agent is used more
        self_improvement_df = pd.DataFrame(self.llm_data_controller.list_all_llm_data(self.model_id))
        if self_improvement_df.empty:
            self_improvement_df = pd.DataFrame([{'input': 'dummy_input', 'output': 'dummy_output'}])

        self_improvement_df = self_improvement_df.rename(columns={
            'output': args['target'],
            'input': args['user_column']
        })
        self_improvement_df = self_improvement_df.tail(DF_EXAMPLES)

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
        """
        Use the default DSPy parameters to set up the chain

        Args:
            df (DataFrame): input to the model
            args (Dict): Parameters for the model

        Returns:
            DSPy chain
        """
        # This is the default language model and retrieval model in DSPy
        colbertv2 = ColBERTv2(url=START_URL)
        dspy.configure(rm=colbertv2)
        dspy_chain = self.create_dspy_chain(df, args)
        return dspy_chain

    def create_dspy_chain(self, df, args):
        """
        Iniialize chain with the llm, add the cold start examples and bootstrap some examples

        Args:
            df (DataFrame): input to the model
            args (Dict): Parameters for the model

        Returns:
            Optimized DSPy chain
        """

        # Initialize the LLM with the API key
        model = args.get('model_name')
        api_key = get_api_key(model, args, self.engine_storage, strict=False)
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
        config = dict(max_bootstrapped_demos=DEMOS, max_labeled_demos=DEMOS)
        metric = dspy.evaluate.metrics.answer_exact_match  # TODO: passage match requires context from prediction... we'll probably modify the signature of ReAct
        teleprompter = BootstrapFewShot(metric=metric, **config)  # TODO: maybe it's better to have this persisted so that the internal state does a better job at optimizing RAG
        with dspy.context(lm=llm):
            optimized = teleprompter.compile(dspy_module, trainset=dspy_examples)  # TODO: check columns have the right name
        return optimized

    def generate_dspy_response(self, question, chain, llm):
        """
        Generate response using DSPy

        Args:
            question (str): question asked as input to the model
            chain: DSPy chain created
            llm: OpenAI model

        Returns:
            Answer to the prompt
        """
        input_dict = {"question": question}
        with dspy.context(lm=llm):
            response = chain(question=input_dict['question'])
        return response.answer

    def predict_dspy(self, df: pd.DataFrame, args: Dict, chain, llm) -> pd.DataFrame:
        """
        Generate response using DSPy

        Args:
            df (DataFrame): contains the input to the model
            args (Dict): parameters of the model
            chain: DSPy chain created
            llm: OpenAI model

        Returns:
            df (DataFrame): Dataframe of responses
        """
        responses = []
        for index, row in df.iterrows():
            question = row[args['user_column']]
            answer = self.generate_dspy_response(question, chain, llm)
            responses.append({'answer': answer, 'question': question})
            # TODO: check this only adds new incoming rows
            self.llm_data_controller.add_llm_data(question, answer, self.model_id)  # stores new traces for use in new calls

        # Set up the evaluator, which can be used multiple times.
        # TODO: use this in the EVALUATE command
        # evaluate = Evaluate(devset=gsm8k_devset, metric=gsm8k_metric, num_threads=4, display_progress=True, display_table=0)

        return pd.DataFrame(responses)
