import pandas
import unittest
from collections import OrderedDict
from unittest.mock import patch, MagicMock

from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler


class TestOpenAI(unittest.TestCase):
    """
    Unit tests for the OpenAI handler.
    """

    dummy_connection_data = OrderedDict(
        openai_api_key='dummy_api_key',
    )

    def setUp(self):
        # Mock model storage and engine storage
        mock_engine_storage = MagicMock()
        mock_model_storage = MagicMock()

        # Define a return value for the `get_connection_args` method of the mock engine storage
        mock_engine_storage.get_connection_args.return_value = self.dummy_connection_data

        # Assign mock engine storage to instance variable for create validation tests
        self.mock_engine_storage = mock_engine_storage

        self.handler = OpenAIHandler(mock_model_storage, mock_engine_storage, connection_data={'connection_data': self.dummy_connection_data})

    def test_create_validation_without_using_clause_raises_exception(self):
        """
        Test if model creation raises an exception without a USING clause.
        """

        with self.assertRaisesRegex(Exception, "OpenAI engine requires a USING clause! Refer to its documentation for more details."):
            self.handler.create_validation('target', args={}, handler_storage=None)

    def test_create_validation_without_required_parameters_raises_exception(self):
        """
        Test if model creation raises an exception without required parameters.
        """

        with self.assertRaisesRegex(Exception, "One of `question_column`, `prompt_template` or `prompt` is required for this engine."):
            self.handler.create_validation('target', args={"using": {}}, handler_storage=self.mock_engine_storage)

    def test_create_validation_with_invalid_parameter_combinations_raises_exception(self):
        """
        Test if model creation raises an exception with invalid parameter combinations.
        """

        with self.assertRaisesRegex(Exception, "^Please provide one of"):
            self.handler.create_validation('target', args={"using": {'prompt_template': 'dummy_prompt_template', 'question_column': 'question'}}, handler_storage=self.mock_engine_storage)

    def test_create_validation_with_unknown_arguments_raises_exception(self):
        """
        Test if model creation raises an exception with unknown arguments.
        """

        with self.assertRaisesRegex(Exception, "^Unknown arguments:"):
            self.handler.create_validation('target', args={"using": {'prompt_template': 'dummy_prompt_template', 'unknown_arg': 'unknown_arg'}}, handler_storage=self.mock_engine_storage)

    def test_create_validation_with_invalid_api_key_raises_exception(self):
        """
        Test if model creation raises an exception with an invalid API key.
        """

        with self.assertRaisesRegex(Exception, "Invalid api key"):
            self.handler.create_validation('target', args={"using": {'prompt_template': 'dummy_prompt_template'}}, handler_storage=self.mock_engine_storage)

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_create_validation_with_valid_arguments_runs_no_errors(self, mock_openai):
        """
        Test if model creation is validated correctly with valid arguments.
        """

        # Mock the models.retrieve method of the OpenAI client
        mock_openai_client = MagicMock()
        mock_openai_client.models.retrieve.return_value = MagicMock()

        mock_openai.return_value = mock_openai_client

        self.handler.create_validation('target', args={'using': {'prompt_template': 'dummy_prompt_template'}}, handler_storage=self.mock_engine_storage)

    @patch('mindsdb.integrations.handlers.openai_handler.helpers.OpenAI')
    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_create_with_invalid_mode_raises_exception(self, mock_openai_handler_openai_client, mock_openai_helpers_openai_client):
        """
        Test if model creation raises an exception with an invalid mode.
        """

        # Mock the models.list method of the OpenAI client
        mock_models_list = MagicMock()
        mock_models_list.data = [
            MagicMock(id='dummy_model_name')
        ]

        mock_openai_handler_openai_client.return_value.models.list.return_value = mock_models_list
        mock_openai_helpers_openai_client.return_value.models.list.return_value = mock_models_list

        with self.assertRaisesRegex(Exception, "^Invalid operation mode."):
            self.handler.create('dummy_target', args={'using': {'prompt_template': 'dummy_prompt_template', 'mode': 'dummy_mode'}})

    @patch('mindsdb.integrations.handlers.openai_handler.helpers.OpenAI')
    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_create_with_unsupported_model_raises_exception(self, mock_openai_handler_openai_client, mock_openai_helpers_openai_client):
        """
        Test if model creation raises an exception with an invalid model name.
        """

        # Mock the models.list method of the OpenAI client
        mock_models_list = MagicMock()
        mock_models_list.data = [
            MagicMock(id='dummy_model_name')
        ]

        mock_openai_handler_openai_client.return_value.models.list.return_value = mock_models_list
        mock_openai_helpers_openai_client.return_value.models.list.return_value = mock_models_list

        with self.assertRaisesRegex(Exception, "^Invalid model name."):
            self.handler.create('dummy_target', args={'using': {'model_name': 'dummy_unsupported_model_name', 'prompt_template': 'dummy_prompt_template'}})

    @patch('mindsdb.integrations.handlers.openai_handler.helpers.OpenAI')
    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_create_with_valid_arguments_runs_no_errors(self, mock_openai_handler_openai_client, mock_openai_helpers_openai_client):
        """
        Test if model creation runs without errors with valid arguments.
        """

        # Mock the models.list method of the OpenAI client
        mock_models_list = MagicMock()
        mock_models_list.data = [
            MagicMock(id='dummy_model_name')
        ]

        mock_openai_handler_openai_client.return_value.models.list.return_value = mock_models_list
        mock_openai_helpers_openai_client.return_value.models.list.return_value = mock_models_list

        self.handler.create('dummy_target', args={'using': {'prompt_template': 'dummy_prompt_template'}})

    def test_predict_with_invalid_mode_raises_exception(self):
        """
        Test if model prediction raises an exception with an invalid mode.
        """

        # Create a dummy DataFrame
        df = pandas.DataFrame()

        with self.assertRaisesRegex(Exception, "^Invalid operation mode."):
            self.handler.predict(df=df, args={'predict_params': {'mode': 'dummy_mode'}})

    def test_predict_in_embedding_mode_without_question_column_raises_exception(self):
        """
        Test if model prediction raises an exception in embedding mode without a question column.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'mode': 'embedding',
        }

        # Create a dummy DataFrame
        df = pandas.DataFrame()

        with self.assertRaisesRegex(Exception, "Embedding mode needs a question_column"):
            self.handler.predict(df=df, args={'predict_params': {'mode': 'embedding'}})

    def test_predict_in_image_mode_without_question_column_or_prompt_template_raises_exception(self):
        """
        Test if model prediction raises an exception in image mode without a question column or prompt template.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'mode': 'image',
        }

        # Create a dummy DataFrame
        df = pandas.DataFrame()

        with self.assertRaisesRegex(Exception, "Image mode needs either `prompt_template` or `question_column`."):
            self.handler.predict(df=df, args={'predict_params': {'mode': 'image'}})

    def test_predict_in_default_mode_without_question_column_in_data_raises_exception(self):
        """
        Test if model prediction raises an exception in default mode without a question column in the DataFrame.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'mode': 'default',
            'question_column': 'question'
        }

        # Create a dummy DataFrame
        df = pandas.DataFrame()

        with self.assertRaisesRegex(Exception, "This model expects a question to answer in the 'question' column."):
            self.handler.predict(df=df, args={'predict_params': {'mode': 'default'}})

    def test_predict_in_default_mode_without_context_column_in_data_raises_exception(self):
        """
        Test if model prediction raises an exception in default mode without a context column in the DataFrame.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'mode': 'default',
            'question_column': 'question',
            'context_column': 'context'
        }

        # Create a dummy DataFrame
        df = pandas.DataFrame(columns=['question'])

        with self.assertRaisesRegex(Exception, "This model expects context in the 'context' column."):
            self.handler.predict(df=df, args={'predict_params': {'mode': 'default'}})

    def test_predict_in_conversational_modes_with_unsupported_model_raises_exception(self):
        """
        Test if model prediction raises an exception in conversational modes with an unsupported model.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'mode': 'conversational',
            'model_name': 'dummy_unsupported_model_name'
        }

        # Create a dummy DataFrame
        df = pandas.DataFrame()

        with self.assertRaisesRegex(Exception, "^Conversational modes are only available for the following models:"):
            self.handler.predict(df=df, args={'predict_params': {'mode': 'conversational'}})

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_predict_in_default_mode_with_question_column_using_valid_arguments_and_data_runs_no_errors(self, mock_openai_handler_openai_client):
        """
        Test if model prediction returns the expected result in default mode using a question column.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'target': 'answer',
            'mode': 'default',
            'model_name': 'gpt-3.5-turbo',
            'question_column': 'question'
        }

        # Mock the chat.completions.create method of the OpenAI client
        mock_openai_client = MagicMock()
        mock_openai_client.chat.completions.create.return_value = MagicMock(
            choices=[
                MagicMock(
                    message=MagicMock(
                        content='Sweden'
                    )
                )
            ]
        )

        mock_openai_handler_openai_client.return_value = mock_openai_client

        df = pandas.DataFrame({'question': ['Where is Stockholm located?']})
        result = self.handler.predict(df, args={})

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('answer' in result.columns)

        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'answer': ['Sweden']}))

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_predict_in_default_mode_with_prompt_template_using_valid_arguments_and_data_runs_no_errors(self, mock_openai_handler_openai_client):
        """
        Test if model prediction returns the expected result in default mode using a prompt template.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'target': 'answer',
            'mode': 'default',
            'model_name': 'gpt-3.5-turbo',
            'prompt_template': 'Answer the question: {{question}}'
        }

        # Mock the chat.completions.create method of the OpenAI client
        mock_openai_client = MagicMock()
        mock_openai_client.chat.completions.create.return_value = MagicMock(
            choices=[
                MagicMock(
                    message=MagicMock(
                        content='Sweden'
                    )
                )
            ]
        )

        mock_openai_handler_openai_client.return_value = mock_openai_client

        df = pandas.DataFrame({'question': ['Where is Stockholm located?']})
        result = self.handler.predict(df, args={})

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('answer' in result.columns)

        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'answer': ['Sweden']}))

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_predict_in_default_mode_with_question_column_and_completion_model_using_valid_arguments_and_data_runs_no_errors(self, mock_openai_handler_openai_client):
        """
        Test if model prediction returns the expected result in default mode using a question column and a completion model.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'target': 'answer',
            'mode': 'default',
            'model_name': 'babbage-002',
            'question_column': 'question'
        }

        # Mock the completions.create method of the OpenAI client
        mock_openai_client = MagicMock()
        mock_openai_client.completions.create.return_value = MagicMock(
            choices=[
                MagicMock(
                    text='Sweden'
                )
            ]
        )

        mock_openai_handler_openai_client.return_value = mock_openai_client

        df = pandas.DataFrame({'question': ['Where is Stockholm located?']})
        result = self.handler.predict(df, args={})

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('answer' in result.columns)

        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'answer': ['Sweden']}))

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_predict_in_default_mode_with_prompt_template_and_completion_model_using_valid_arguments_and_data_runs_no_errors(self, mock_openai_handler_openai_client):
        """
        Test if model prediction returns the expected result in default mode using a prompt template and a completion model.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'target': 'answer',
            'mode': 'default',
            'model_name': 'babbage-002',
            'prompt_template': 'Answer the question: {{question}}'
        }

        # Mock the completions.create method of the OpenAI client
        mock_openai_client = MagicMock()
        mock_openai_client.completions.create.return_value = MagicMock(
            choices=[
                MagicMock(
                    text='Sweden'
                )
            ]
        )

        mock_openai_handler_openai_client.return_value = mock_openai_client

        df = pandas.DataFrame({'question': ['Where is Stockholm located?']})
        result = self.handler.predict(df, args={})

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('answer' in result.columns)

        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'answer': ['Sweden']}))

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_predict_in_embedding_mode_using_valid_arguments_and_data_runs_no_errors(self, mock_openai_handler_openai_client):
        """
        Test if model prediction returns the expected result for an embeddings task.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'model_name': 'dummy_model_name',
            'question_column': 'text',
            'target': 'embeddings',
            'mode': 'embedding'
        }

        # Mock the embeddings.completions.create method of the OpenAI client
        mock_openai_client = MagicMock()
        mock_openai_client.embeddings.create.return_value = MagicMock(
            data=[
                MagicMock(
                    embedding=[0, 1]
                )
            ]
        )

        mock_openai_handler_openai_client.return_value = mock_openai_client

        df = pandas.DataFrame({'text': ['MindsDB']})
        result = self.handler.predict(df, args={})

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('embeddings' in result.columns)

        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'embeddings': [[0, 1]]}))

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_predict_in_image_mode_with_question_column_using_valid_arguments_and_data_runs_no_errors(self, mock_openai_handler_openai_client):
        """
        Test if model prediction returns the expected result for an image task using a question column.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'question_column': 'text',
            'target': 'image',
            'mode': 'image'
        }

        # Mock the images.generate method of the OpenAI client
        mock_openai_client = MagicMock()
        mock_openai_client.images.generate.return_value = MagicMock(
            data=[
                MagicMock(
                    url='dummy_image_url'
                )
            ]
        )

        mock_openai_handler_openai_client.return_value = mock_openai_client

        df = pandas.DataFrame({'text': ['Show me an image of two leapord cubs playing?']})
        result = self.handler.predict(df, args={})

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('image' in result.columns)

        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'image': ['dummy_image_url']}))

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_predict_in_image_mode_with_prompt_template_using_valid_arguments_and_data_runs_no_errors(self, mock_openai_handler_openai_client):
        """
        Test if model prediction returns the expected result for an image task using a prompt template.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'prompt_template': 'Generate an image of {{text}}',
            'target': 'image',
            'mode': 'image'
        }

        # Mock the images.generate method of the OpenAI client
        mock_openai_client = MagicMock()
        mock_openai_client.images.generate.return_value = MagicMock(
            data=[
                MagicMock(
                    url='dummy_image_url'
                )
            ]
        )

        mock_openai_handler_openai_client.return_value = mock_openai_client

        df = pandas.DataFrame({'text': ['Leopard cubs playing']})
        result = self.handler.predict(df, args={})

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('image' in result.columns)

        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'image': ['dummy_image_url']}))

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_predict_in_conversational_mode_with_using_arguments_and_data_runs_no_errors(self, mock_openai_handler_openai_client):
        """
        Test if model prediction returns the expected result for a conversational task.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'user_column': 'question',
            'prompt': 'you are a helpful assistant',
            'assistant_column': 'answer',
            'target': 'answer',
            'mode': 'conversational'
        }

        # Mock the chat.completions.create method of the OpenAI client
        mock_openai_client = MagicMock()
        mock_openai_client.chat.completions.create.return_value = MagicMock(
            choices=[
                MagicMock(
                    message=MagicMock(
                        content='Gamla Stan'
                    )
                )
            ]
        )

        mock_openai_handler_openai_client.return_value = mock_openai_client

        df = pandas.DataFrame({'question': ['What is the capital of Sweden?', 'What are some cool places to visit there?']})
        result = self.handler.predict(df, args={})

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('answer' in result.columns)

        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'answer': ['', 'Gamla Stan']}))

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_predict_in_conversational_full_mode_using_valid_arguments_and_data_runs_no_errors(self, mock_openai_handler_openai_client):
        """
        Test if model prediction returns the expected result for a conversational-full task.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'user_column': 'text',
            'prompt': 'you are a helpful assistant',
            'assistant_column': 'answer',
            'target': 'answer',
            'mode': 'conversational-full'
        }

        # Mock the chat.completions.create method of the OpenAI client
        mock_openai_client = MagicMock()
        mock_openai_client.chat.completions.create.side_effect = [
            MagicMock(
                choices=[
                    MagicMock(
                        message=MagicMock(
                            content='Stockholm'
                        )
                    )
                ]
            ),
            MagicMock(
                choices=[
                    MagicMock(
                        message=MagicMock(
                            content='Gamla Stan'
                        )
                    )
                ]
            )
        ]

        mock_openai_handler_openai_client.return_value = mock_openai_client

        df = pandas.DataFrame({'text': ['What is the capital of Sweden?', 'What are some cool places to visit there?']})
        result = self.handler.predict(df, args={})

        self.assertIsInstance(result, pandas.DataFrame)

        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'answer': ['Stockholm', 'Gamla Stan']}))

    def test_describe_runs_no_errors(self):
        """
        Test if model describe returns the expected result.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'user_column': 'text',
            'prompt': 'you are a helpful assistant',
            'assistant_column': 'answer',
            'target': 'answer',
            'mode': 'conversational'
        }

        result = self.handler.describe()

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('tables' in result.columns)

        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'tables': ['args', 'metadata']}))

    def test_describe_args_runs_no_errors(self):
        """
        Test if model describe returns the expected result.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'user_column': 'text',
            'prompt': 'you are a helpful assistant',
            'assistant_column': 'answer',
            'target': 'answer',
            'mode': 'conversational'
        }

        result = self.handler.describe('args')

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('key' in result.columns)
        self.assertTrue('value' in result.columns)

        pandas.testing.assert_frame_equal(
            result,
            pandas.DataFrame(
                {
                    'key': ['user_column', 'prompt', 'assistant_column', 'target', 'mode'],
                    'value': ['text', 'you are a helpful assistant', 'answer', 'answer', 'conversational']
                }
            )
        )

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_describe_metadata_runs_no_errors(self, mock_openai_handler_openai_client):
        """
        Test if model describe returns the expected result.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'user_column': 'text',
            'prompt': 'you are a helpful assistant',
            'assistant_column': 'answer',
            'target': 'answer',
            'mode': 'conversational'
        }

        # Mock the models.retrieve method of the OpenAI client: return a dict directly because the result is converted to a dict later
        mock_openai_client = MagicMock()
        mock_openai_client.models.retrieve.return_value = {
            'model': 'dummy_model_name',
            'id': 'dummy_model_id',
            'created_at': 'dummy_created_at',
            'owner': 'dummy_owner'
        }

        mock_openai_handler_openai_client.return_value = mock_openai_client

        result = self.handler.describe('metadata')

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('key' in result.columns)
        self.assertTrue('value' in result.columns)

        pandas.testing.assert_frame_equal(
            result,
            pandas.DataFrame(
                {
                    'key': ['model', 'id', 'created_at', 'owner'],
                    'value': ['dummy_model_name', 'dummy_model_id', 'dummy_created_at', 'dummy_owner']
                }
            )
        )

    def test_finetune_with_unsupported_model_raises_exception(self):
        """
        Test if model fine-tuning raises an exception with an unsupported model.
        """

        # Create a mock base model storage and assign it to the handler
        mock_base_model_storage = MagicMock()
        self.handler.base_model_storage = mock_base_model_storage

        # Mock the json_get method of the base model storage
        self.handler.base_model_storage.json_get.return_value = {
            'model_name': 'dummy_model_name'
        }

        with self.assertRaisesRegex(Exception, "^This model cannot be finetuned."):
            self.handler.finetune('dummy_target', args={'using': {'model_name': 'dummy_unsupported_model_name', 'prompt_template': 'dummy_prompt_template'}})

    # TODO: Add more unit tests for the finetune method


if __name__ == '__main__':
    unittest.main()
