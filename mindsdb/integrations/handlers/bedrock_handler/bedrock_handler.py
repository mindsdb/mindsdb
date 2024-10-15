import numpy as np
import pandas as pd
from typing import Text, Tuple, Dict, List, Optional, Any

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
        Validates the AWS credentials provided on creation of an engine.

        Args:
            connection_args (Dict): The parameters of the engine.

        Raises:
            Exception: If the handler is not configured with valid API credentials.
        """
        connection_args = {k.lower(): v for k, v in connection_args.items()}
        AmazonBedrockHandlerEngineConfig(**connection_args)

    def create(self, target, args: Dict = None, **kwargs: Any) -> None:
        """
        Creates a model by validating the model configuration and saving it to the storage.

        Args:
            target (Text): The target column name.
            args (Dict): The parameters of the model.
            kwargs (Any): Other keyword arguments.

        Raises:
            Exception: If the model is not configured with valid parameters.

        Returns:
            None
        """
        if 'using' not in args:
            raise MissingConnectionParams("Amazon Bedrock engine requires a USING clause! Refer to its documentation for more details.")
        else:
            model_args = args['using']
            # Replace 'model_id' with 'id' to match the Amazon Bedrock handler model configuration.
            # This is done to avoid the Pydantic warning regarding conflicts with the protected 'model_' namespace.
            if 'model_id' in model_args:
                model_args['id'] = model_args['model_id']
                del model_args['model_id']

            handler_model_config = AmazonBedrockHandlerModelConfig(**model_args, connection_args=self.engine_storage.get_connection_args())

            # Save the model configuration to the storage.
            handler_model_params = handler_model_config.model_dump()
            logger.info(f"Saving model configuration to storage: {handler_model_params}")

            args['target'] = target
            args['handler_model_params'] = handler_model_params
            self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Makes predictions using a model by invoking the Amazon Bedrock API.

        Args:
            df (pd.DataFrame): The input data to invoke the model with.
            args (Dict): The parameters passed when making predictions.

        Raises:
            ValueError: If the input data does not match the configuration of the model.

        Returns:
            pd.DataFrame: The input data with the predicted values in a new column.
        """
        args = self.model_storage.json_get('args')
        handler_model_params = args['handler_model_params']
        mode = handler_model_params['mode']
        model_id = handler_model_params['id']
        inference_config = handler_model_params.get('inference_config')
        target = args['target']

        if mode == 'default':
            prompts, empty_prompt_ids = self._prepare_data_for_default_mode(df, args)
            predictions = self._predict_for_default_mode(model_id, prompts, inference_config)

            # Fill the empty predictions with None.
            for i in sorted(empty_prompt_ids):
                predictions.insert(i, None)

        elif mode == 'conversational':
            prompt, total_questions = self._prepare_data_for_conversational_mode(df, args)
            prediction = self._predict_for_conversational_mode(model_id, prompt, inference_config)

            # Create a list of None values for the total number of questions and replace the last one with the prediction.
            predictions = [None] * total_questions
            predictions[-1] = prediction

        pred_df = pd.DataFrame(predictions, columns=[target])
        return pred_df

    def _prepare_data_for_default_mode(self, df: pd.DataFrame, args: Dict) -> List[Dict]:
        """
        Prepares the input data for the default mode of the Amazon Bedrock handler.
        A separate prompt is prepared for each question.

        Args:
            df (pd.DataFrame): The input data to invoke the model with.
            args (Dict): The parameters of the model.

        Returns:
            List[Dict]: The prepared prompts for invoking the Amazon Bedrock API. The model will be invoked for each prompt.
        """
        handler_model_params = args['handler_model_params']
        question_column = handler_model_params.get('question_column')
        context_column = handler_model_params.get('context_column')
        prompt_template = handler_model_params.get('prompt_template')

        if question_column is not None:
            questions, empty_prompt_ids = self._prepare_data_with_question_and_context_columns(
                df,
                question_column,
                context_column
            )

        elif prompt_template is not None:
            questions, empty_prompt_ids = self._prepare_data_with_prompt_template(df, prompt_template)

        # Prepare the prompts.
        questions = [question for i, question in enumerate(questions) if i not in empty_prompt_ids]
        prompts = [{"role": "user", "content": [{"text": question}]} for question in questions]

        return prompts, empty_prompt_ids

    def _prepare_data_for_conversational_mode(self, df: pd.DataFrame, args: Dict) -> Tuple[List[Dict], int]:
        """
        Prepares the input data for the conversational mode of the Amazon Bedrock handler.
        A single prompt is prepared for all the questions.

        Args:
            df (pd.DataFrame): The input data to invoke the model with.
            args (Dict): The parameters of the model.

        Returns:
            Tuple[List[Dict], int]: The prepared prompt for invoking the Amazon Bedrock API and the total number of questions.
            The model will be invoked once using this prompt which contains all the questions.
            The total number of questions is used to produce the final list of predictions.
        """
        handler_model_params = args['handler_model_params']
        question_column = handler_model_params.get('question_column')
        context_column = handler_model_params.get('context_column')
        prompt_template = handler_model_params.get('prompt_template')

        if question_column is not None:
            questions, empty_prompt_ids = self._prepare_data_with_question_and_context_columns(
                df,
                question_column,
                context_column
            )

        if prompt_template is not None:
            questions, empty_prompt_ids = self._prepare_data_with_prompt_template(df, prompt_template)

        # Prepare the prompts.
        questions = [question for i, question in enumerate(questions) if i not in empty_prompt_ids]
        prompt = [{"role": "user", "content": [{"text": question} for question in questions]}]

        # Get the total number of questions; including the empty ones.
        total_questions = len(df)

        return prompt, total_questions

    def _prepare_data_with_question_and_context_columns(self, df: pd.DataFrame, question_column: Text, context_column: Text = None) -> Tuple[List[Text], List[int]]:
        """
        Prepares the input data with question and context columns.

        Args:
            df (pd.DataFrame): The input data to invoke the model with.
            question_column (Text): The column containing the questions.
            context_column (Text): The column containing the context.

        Returns:
            Tuple[List[Text], List[int]]: The questions to build the prompts for invoking the Amazon Bedrock API and the empty prompt IDs.
        """
        if question_column not in df.columns:
            raise ValueError(f"Column {question_column} not found in the dataframe!")

        if context_column and context_column not in df.columns:
            raise ValueError(f"Column {context_column} not found in the dataframe!")

        if context_column:
            empty_prompt_ids = np.where(
                df[[context_column, question_column]]
                .isna()
                .all(axis=1)
                .values
            )[0]
            contexts = list(df[context_column].apply(lambda x: str(x)))
            questions_without_context = list(df[question_column].apply(lambda x: str(x)))

            questions = [
                f'Context: {c}\nQuestion: {q}\nAnswer: '
                for c, q in zip(contexts, questions_without_context)
            ]

        else:
            questions = list(df[question_column].apply(lambda x: str(x)))
            empty_prompt_ids = np.where(
                df[[question_column]].isna().all(axis=1).values
            )[0]

        return questions, empty_prompt_ids

    def _prepare_data_with_prompt_template(self, df: pd.DataFrame, prompt_template: Text) -> Tuple[List[Text], List[int]]:
        """
        Prepares the input data with a prompt template.

        Args:
            df (pd.DataFrame): The input data to invoke the model with.
            prompt_template (Text): The base prompt template to use.

        Returns:
            Tuple[List[Text], List[int]]: The questions to build the prompts for invoking the Amazon Bedrock API and the empty prompt IDs.
        """
        questions, empty_prompt_ids = get_completed_prompts(prompt_template, df)

        return questions, empty_prompt_ids

    def _predict_for_default_mode(self, model_id: Text, prompts: List[Text], inference_config: Dict) -> List[Text]:
        """
        Makes predictions for the default mode of the Amazon Bedrock handler using the prepared prompts.

        Args:
            model_id (Text): The ID of the model in Amazon Bedrock.
            prompts (List[Text]): The prepared prompts for invoking the Amazon Bedrock API.
            inference_config (Dict): The inference configuration supported by the Amazon Bedrock API.

        Returns:
            List[Text]: The predictions made by the Amazon Bedrock API.
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

    def _predict_for_conversational_mode(self, model_id: Text, prompt: List[Text], inference_config: Dict) -> Text:
        """
        Makes a prediction for the conversational mode of the Amazon Bedrock handler using the prepared prompt.

        Args:
            model_id (Text): The ID of the model in Amazon Bedrock.
            prompts (List[Text]): Prepared prompts for invoking the Amazon Bedrock API.
            inference_config (Dict): Inference configuration supported by the Amazon Bedrock API.

        Returns:
            Text: The prediction made by the Amazon Bedrock API.
        """
        bedrock_runtime_client = create_amazon_bedrock_client(
            'bedrock-runtime',
            **self.engine_storage.get_connection_args()
        )

        response = bedrock_runtime_client.converse(
            modelId=model_id,
            messages=prompt,
            inferenceConfig=inference_config
        )

        return response["output"]["message"]["content"][0]["text"]

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
            model_id = args['handler_model_params']['id']
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
