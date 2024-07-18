import numpy as np
import pandas as pd
from typing import Text, Dict, List, Optional, Any

from mindsdb.utilities import log

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.llm.utils import get_completed_prompts
from mindsdb.integrations.libs.api_handler_exceptions import MissingConnectionParams
from mindsdb.integrations.handlers.bedrock_handler.utilities import create_amazon_bedrock_client
from mindsdb.integrations.handlers.bedrock_handler.settings import AmazonBedrockHandlerEngineConfig, AmazonBedrockHandlerModelConfig


logger = log.getLogger(__name__)


class AmazonBedrockHandler(BaseMLEngine):
    """
    This handler handles connection and inference with the Amazon Bedrock API.
    """

    name = 'bedrock'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True

    def create_engine(self, connection_args: Dict) -> None:
        """
        Validates the AWS credentials provided on engine creation.

        Args:
            connection_args (Dict): Parameters for the engine.

        Raises:
            Exception: If the handler is not configured with valid API credentials.
        """
        connection_args = {k.lower(): v for k, v in connection_args.items()}
        AmazonBedrockHandlerEngineConfig(**connection_args)

    def create(self, target, args: Dict = None, **kwargs: Any) -> None:
        """
        Creates a model by validating the model configuration and saving it to the storage.

        Args:
            target (Text): Target column name.
            args (Dict): Parameters for the model.
            kwargs (Any): Other keyword arguments.

        Raises:
            Exception: If the model is not configured with valid parameters.

        Returns:
            None
        """
        if 'using' not in args:
            raise MissingConnectionParams("Amazon Bedrock engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']
            handler_model_config = AmazonBedrockHandlerModelConfig(**args, connection_args=self.engine_storage.get_connection_args())

            # Save the model configuration to the storage.
            handler_model_params = handler_model_config.model_dump()
            logger.info(f"Saving model configuration to storage: {handler_model_params}")

            args['target'] = target
            args['handler_model_params'] = handler_model_params
            self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Make predictions using a model by invoking the Amazon Bedrock API.

        Args:
            df (pd.DataFrame): Input data to make predictions on.
            args (Dict): Parameters passed when making predictions.

        Raises:
            Exception: If the input does not match the configuration of the model.

        Returns:
            pd.DataFrame: Input data with the predicted values in a new column.
        """
        args = self.model_storage.json_get('args')
        handler_model_params = args['handler_model_params']
        mode = args['handler_model_params']['mode']
        model_id = handler_model_params['model_id']
        inference_config = handler_model_params.get('inference_config')
        target = args['target']

        # Run predict for the default mode.
        if mode == 'default':
            prompts = self._prepare_data_for_default_mode(df, args)
            predictions = self._predict_for_default_mode(model_id, prompts, inference_config)

        pred_df = pd.DataFrame(predictions, columns=[target])
        return pred_df

    def _prepare_data_for_default_mode(self, df: pd.DataFrame, args: Dict) -> List[Dict]:
        """
        Prepare the input data for the default mode of the Amazon Bedrock handler.

        Args:
            df (pd.DataFrame): Input data to make predictions on.
            args (Dict): Parameters passed when making predictions.

        Returns:
            List[Dict]: Prepared prompts for invoking the Amazon Bedrock API.
        """
        handler_model_params = args['handler_model_params']

        # Prepare the parameters + data for the prediction.
        # Question column.
        if handler_model_params.get('question_column') is not None:
            if handler_model_params['question_column'] not in df.columns:
                raise ValueError(f"Column {handler_model_params['question_column']} not found in the dataframe!")

            if handler_model_params.get('context_column'):
                empty_prompt_ids = np.where(
                    df[[args['context_column'], args['question_column']]]
                    .isna()
                    .all(axis=1)
                    .values
                )[0]
                contexts = list(df[args['context_column']].apply(lambda x: str(x)))
                questions_without_context = list(df[args['question_column']].apply(lambda x: str(x)))

                questions = [
                    f'Context: {c}\nQuestion: {q}\nAnswer: '
                    for c, q in zip(contexts, questions_without_context)
                ]

            else:
                questions = list(df[args['question_column']].apply(lambda x: str(x)))
                empty_prompt_ids = np.where(
                    df[[args['question_column']]].isna().all(axis=1).values
                )[0]

        # Prompt template.
        if handler_model_params.get('prompt_template') is not None:
            questions, empty_prompt_ids = get_completed_prompts(handler_model_params['prompt_template'], df)

        # Prepare the prompts.
        prompts = [{"role": "user", "content": [{"text": question}]} for question in questions]
        prompts = [j for i, j in enumerate(prompts) if i not in empty_prompt_ids]

        return prompts

    def _predict_for_default_mode(self, model_id: Text, prompts: List[Text], inference_config: Dict) -> Dict:
        """
        Make predictions for the default mode of the Amazon Bedrock handler.

        Args:
            model_id (Text): The ID of the model in Amazon Bedrock.
            prompts (List[Text]): Prepared prompts for invoking the Amazon Bedrock API.
            inference_config (Dict): Inference configuration supported by the Amazon Bedrock API.
        """
        predictions = []
        bedrock_runtime_client = create_amazon_bedrock_client(
            'bedrock-runtime',
            **self.engine_storage.get_connection_args()
        )

        for prompt in prompts:
            response = bedrock_runtime_client.converse(
                modelId=model_id,
                messages=[prompt],
                inferenceConfig=inference_config
            )
            predictions.append(
                response["output"]["message"]["content"][0]["text"]
            )

        return predictions

    def describe(self, attribute: Optional[Text] = None) -> pd.DataFrame:
        """
        Get the metadata or arguments of a model.

        Args:
            attribute (Optional[Text]): Attribute to describe. Can be 'args' or 'metadata'.

        Returns:
            pd.DataFrame: Model metadata or model arguments.
        """
        args = self.model_storage.json_get('args')

        if attribute == 'args':
            del args['handler_model_params']
            return pd.DataFrame(args.items(), columns=['key', 'value'])

        elif attribute == 'metadata':
            model_id = args.get('model_id')
            try:
                bedrock_client = create_amazon_bedrock_client(
                    'bedrock',
                    **self.engine_storage.get_connection_args()
                )
                meta = bedrock_client.get_foundation_model(modelIdentifier=model_id)['modelDetails']
            except Exception as e:
                meta = {'error': str(e)}
            return pd.DataFrame(dict(meta).items(), columns=['key', 'value'])

        else:
            tables = ['args', 'metadata']
            return pd.DataFrame(tables, columns=['tables'])
