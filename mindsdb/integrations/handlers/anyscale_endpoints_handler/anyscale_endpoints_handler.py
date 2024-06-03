import os
import json
import pandas as pd
from typing import Text, Optional, Dict

from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.integrations.libs.llm.utils import ft_jsonl_validation, ft_formatter
from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler
from mindsdb.integrations.handlers.anyscale_endpoints_handler.settings import anyscale_handler_config
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class AnyscaleEndpointsHandler(OpenAIHandler):
    """
    This handler handles connection and inference with the Anyscale Endpoints API.
    """

    name = 'anyscale_endpoints'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.all_models = []
        self.chat_completion_models = []
        self.supported_ft_models = []
        self.default_model = anyscale_handler_config.DEFAULT_MODEL
        self.api_base = anyscale_handler_config.ANYSCALE_API_BASE
        self.default_mode = anyscale_handler_config.DEFAULT_MODE
        self.supported_modes = anyscale_handler_config.SUPPORTED_MODES
        self.rate_limit = anyscale_handler_config.RATE_LIMIT
        self.max_batch_size = anyscale_handler_config.MAX_BATCH_SIZE
        self.default_max_tokens = anyscale_handler_config.DEFAULT_MAX_TOKENS

    def create_engine(self, connection_args: Dict) -> None:
        """
        Validate the Anyscale Endpoints credentials on engine creation.

        Args:
            connection_args (Dict): Connection arguments.

        Raises:
            Exception: If the handler is not configured with valid API credentials.

        Returns:
            None
        """

        connection_args = {k.lower(): v for k, v in connection_args.items()}
        api_key = connection_args.get('anyscale_endpoints_api_key')
        if api_key is not None:
            org = connection_args.get('api_organization')
            api_base = connection_args.get('api_base') or os.environ.get('ANYSCALE_API_BASE', anyscale_handler_config.ANYSCALE_API_BASE)
            client = self._get_client(api_key=api_key, base_url=api_base, org=org)
            OpenAIHandler._check_client_connection(client)

    @staticmethod
    def create_validation(target: Text, args: Optional[Dict] = None, **kwargs: Optional[Dict]) -> None:
        """
        Validate the Anyscale Endpoints credentials on model creation.

        Args:
            target (Text): Target column, not required for LLMs.
            args (Dict): Handler arguments.
            kwargs (Dict): Handler keyword arguments.

        Raises:
            Exception: If the handler is not configured with valid API credentials.

        Returns:
            None
        """

        if 'using' not in args:
            raise Exception(
                "Anyscale Endpoints engine requires a USING clause! Refer to its documentation for more details."
            )
        else:
            args = args['using']

        engine_storage = kwargs['handler_storage']
        connection_args = engine_storage.get_connection_args()
        api_key = get_api_key('anyscale_endpoints', args, engine_storage=engine_storage)
        api_base = connection_args.get('api_base') or args.get('api_base') or os.environ.get('ANYSCALE_API_BASE', anyscale_handler_config.ANYSCALE_API_BASE)

        client = OpenAIHandler._get_client(api_key=api_key, base_url=api_base)
        OpenAIHandler._check_client_connection(client)

    def create(self, target: Text, args: Optional[Dict] = None, **kwargs: Optional[Dict]) -> None:
        """
        Create a model via an engine.

        Args:
            target (Text): Target column.
            args (Dict): Model arguments.
            kwargs (Dict): Other arguments.

        Returns:
            None
        """

        # Set the base and fine-tuned models and call the parent method
        self._set_models_from_args(args)
        super().create(target, args, **kwargs)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Make a prediction using a model.

        Args:
            df (pd.DataFrame): Input data.
            args (Dict): Handler arguments.

        Returns:
            pd.DataFrame: Predicted data
        """

        # Set the base and fine-tuned models and call the parent method
        self._set_models_from_args(args)
        return super().predict(df, args)

    def finetune(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Fine-tune a supported model.

        Args:
            df (pd.DataFrame): Input data.
            args (Dict): Handler arguments.

        Returns:
            None
        """

        using_args = args.get('using', {})
        self._set_models(using_args)
        super().finetune(df, args)

        # Rewrite chat_completion_models to include the newly fine-tuned model
        args = self.model_storage.json_get('args')
        args['chat_completion_models'] = list(self.chat_completion_models) + [args['model_name']]
        self.model_storage.json_set('args', args)

    def describe(self, attribute: Optional[Text] = None) -> pd.DataFrame:
        """
        Describe a model or its metadata.

        Args:
            attribute (Text): Attribute to describe.

        Returns:
            pd.DataFrame: Model or metadata description.
        """

        args = self.model_storage.json_get('args')

        # Remove keys from args to display
        for arg in ('api_key', 'openai_api_key'):
            if arg in args:
                del args[arg]

        if attribute == 'args':
            return pd.DataFrame(args.items(), columns=['key', 'value'])
        elif attribute == 'metadata':
            # The URL is used as some models require completing a form to access their artifacts
            model_name = args.get('model_name', self.default_model)
            model_card_url = 'https://huggingface.co/' + model_name
            return pd.DataFrame({'model_name': [model_name], 'model_card': [model_card_url]})
        else:
            tables = ['args', 'metadata']
            return pd.DataFrame(tables, columns=['tables'])

    def _set_models(self, args: Dict) -> None:
        """
        Set the base and fine-tuned models.

        Args:
            args (Dict): Model arguments.

        Returns:
            None
        """

        api_key = get_api_key('anyscale_endpoints', args, self.engine_storage)
        client = OpenAIHandler._get_client(api_key=api_key, base_url=self.api_base)
        self.all_models = [m.id for m in client.models.list()]
        self.chat_completion_models = [m.id for m in client.models.list() if m.rayllm_metadata['engine_config']['model_type'] == 'text-generation']  # noqa
        # Set base models compatible with fine-tuning
        self.supported_ft_models = self.chat_completion_models

    def _set_models_from_args(self, args: Dict) -> None:
        """
        Set the base and fine-tuned models from the arguments, if specified. Otherwise, use the default list.

        Args:
            args (Dict): Model arguments.

        Returns:
            None
        """

        self._set_models(args.get('using', {}))

        # Update the models if they are specified in the arguments
        model_args = self.model_storage.json_get('args')
        if model_args and 'chat_completion_models' in model_args:
            self.chat_completion_models = model_args.get('chat_completion_models')

    @staticmethod
    def _prepare_ft_jsonl(df, temp_storage_path: Text, temp_filename: Text, _, test_size: Optional[float] = 0.2) -> Dict:
        """
        Prepare the data for fine-tuning.

        Args:
            df (pd.DataFrame): Input data.
            temp_storage_path (Text): Temporary storage path.
            temp_filename (Text): Temporary filename.
            _: Unused.
            test_size (float): Test size.

        Returns:
            dict: File names mapped to the prepared data.
        """

        # 1. Format data
        chats = ft_formatter(df)

        # 2. Split chats in training and validation subsets
        series = pd.Series(chats)
        if len(series) < anyscale_handler_config.MIN_FT_DATASET_LEN:
            raise Exception(f"Dataset is too small to finetune. Please include at least {anyscale_handler_config.MIN_FT_DATASET_LEN} samples (complete chats).")
        val_size = max(anyscale_handler_config.MIN_FT_VAL_LEN, int(len(series) * test_size))  # at least as many samples as required by Anyscale
        train = series.iloc[:-val_size]
        val = series.iloc[-val_size:]

        # 3. Write as JSONL files
        file_names = {
            'train': f'{temp_filename}_prepared_train.jsonl',
            'val': f'{temp_filename}_prepared_valid.jsonl',
        }
        train.to_json(os.path.join(temp_storage_path, file_names['train']), orient='records', lines=True)
        val.to_json(os.path.join(temp_storage_path, file_names['val']), orient='records', lines=True)

        # 4. Validate and return
        with open(os.path.join(temp_storage_path, file_names['train']), 'r', encoding='utf-8') as f:
            ft_jsonl_validation([json.loads(line) for line in f])

        with open(os.path.join(temp_storage_path, file_names['val']), 'r', encoding='utf-8') as f:
            ft_jsonl_validation([json.loads(line) for line in f])

        return file_names

    def _get_ft_model_type(self, model_name: Text) -> Text:
        """
        Get the fine-tuning model type.

        Args:
            model_name (Text): Model name.

        Returns:
            Text: Model type.
        """

        for base_model in self.chat_completion_models:
            if base_model.lower() in model_name.lower():
                return base_model
        logger.warning(f'Cannot recognize model {model_name}. Finetuning may fail.')
        return model_name.lower()

    @staticmethod
    def _add_extra_ft_params(ft_params: Dict, using_args: Dict) -> Dict:
        """
        Add extra fine-tuning parameters.

        Args:
            ft_params (Dict): Fine-tuning parameters.
            using_args (Dict): Model arguments.

        Returns:
            Dict: Fine-tuning parameters with extra parameters.
        """

        hyperparameters = {}
        # Populate separately because keys with `None` break the API
        for key in ('n_epochs', 'context_length'):
            if using_args.get(key, None):
                hyperparameters[key] = using_args[key]
        if hyperparameters:
            return {**ft_params, **{'hyperparameters': hyperparameters}}
        else:
            return ft_params
