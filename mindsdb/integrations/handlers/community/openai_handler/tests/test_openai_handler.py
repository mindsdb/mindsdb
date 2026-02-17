import os
import pytest
import pandas as pd
from unittest.mock import patch

from tests.unit.ml_handlers.base_ml_test import BaseMLAPITest


@pytest.mark.skipif(os.environ.get('MDB_TEST_MDB_OPENAI_API_KEY') is None, reason='Missing API key!')
class TestOpenAI(BaseMLAPITest):
    """
    Integration tests for the OpenAI handler.
    """

    def setup_method(self):
        """
        Setup test environment by creating a project and an OpenAI engine.
        """
        super().setup_method()
        self.run_sql("CREATE DATABASE proj")
        self.run_sql(
            f"""
            CREATE ML_ENGINE openai_engine
            FROM openai
            USING
            openai_api_key = '{self.get_api_key('MDB_TEST_MDB_OPENAI_API_KEY')}';
            """
        )

    def test_create_model_with_unsupported_model_raises_exception(self):
        """
        Test if CREATE MODEL raises an exception with an unsupported model.
        """
        self.run_sql(
            """
            CREATE MODEL proj.test_openaai_unsupported_model_model
            PREDICT answer
            USING
                engine='openai_engine',
                model_name='this-model-does-not-exist',
                prompt_template='dummy_prompt_template';
            """
        )
        with pytest.raises(Exception) as excinfo:
            self.wait_predictor("proj", "test_openaai_unsupported_model_model")

        assert "Invalid model name." in str(excinfo.value)

    def test_full_flow_in_default_mode_with_question_column_for_single_prediction_runs_no_errors(self):
        """
        Test the full flow in default mode with a question column for a single prediction.
        """
        self.run_sql(
            """
            CREATE MODEL proj.test_openai_single_full_flow_default_mode_question_column
            PREDICT answer
            USING
                engine='openai_engine',
                question_column='question';
            """
        )

        self.wait_predictor("proj", "test_openai_single_full_flow_default_mode_question_column")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_openai_single_full_flow_default_mode_question_column
            WHERE question='What is the capital of Sweden?'
            """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_full_flow_in_default_mode_with_question_column_for_bulk_predictions_runs_no_errors(self, mock_handler):
        """
        Test the full flow in default mode with a question column for bulk predictions.
        """
        df = pd.DataFrame.from_dict({"question": [
            "What is the capital of Sweden?",
            "What is the second planet of the solar system?"
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.test_openai_bulk_full_flow_default_mode_question_column
            PREDICT answer
            USING
                engine='openai_engine',
                question_column='question';
            """
        )

        self.wait_predictor("proj", "test_openai_bulk_full_flow_default_mode_question_column")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_openai_bulk_full_flow_default_mode_question_column as p;
            """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()
        assert "venus" in result_df["answer"].iloc[1].lower()

    def test_full_flow_in_default_mode_with_prompt_template_for_single_prediction_runs_no_errors(self):
        """
        Test the full flow in default mode with a prompt template for a single prediction.
        """
        self.run_sql(
            """
            CREATE MODEL proj.test_openai_single_full_flow_default_mode_prompt_template
            PREDICT answer
            USING
                engine='openai_engine',
                prompt_template='Answer this question and add "Boom!" to the end of the answer: {{{{question}}}}';
            """
        )

        self.wait_predictor("proj", "test_openai_single_full_flow_default_mode_prompt_template")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_openai_single_full_flow_default_mode_prompt_template
            WHERE question='What is the capital of Sweden?'
            """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()
        assert "boom!" in result_df["answer"].iloc[0].lower()

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_full_flow_in_default_mode_with_prompt_template_for_bulk_predictions_runs_no_errors(self, mock_handler):
        """
        Test the full flow in default mode with a prompt template for bulk predictions.
        """
        df = pd.DataFrame.from_dict({"question": [
            "What is the capital of Sweden?",
            "What is the second planet of the solar system?"
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.test_openai_bulk_full_flow_default_mode_prompt_template
            PREDICT answer
            USING
                engine='openai_engine',
                prompt_template='Answer this question and add "Boom!" to the end of the answer: {{{{question}}}}';
            """
        )

        self.wait_predictor("proj", "test_openai_bulk_full_flow_default_mode_prompt_template")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_openai_bulk_full_flow_default_mode_prompt_template as p;
            """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()
        assert "boom!" in result_df["answer"].iloc[0].lower()
        assert "venus" in result_df["answer"].iloc[1].lower()
        assert "boom!" in result_df["answer"].iloc[1].lower()

    def test_full_flow_in_embedding_mode_for_single_prediction_runs_no_errors(self):
        """
        Test the full flow in embedding mode for a single prediction.
        """
        self.run_sql(
            """
            CREATE MODEL proj.test_openai_single_full_flow_embedding_mode
            PREDICT answer
            USING
                engine='openai_engine',
                mode='embedding',
                model_name = 'text-embedding-ada-002',
                question_column = 'text';
            """
        )

        self.wait_predictor("proj", "test_openai_single_full_flow_embedding_mode")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_openai_single_full_flow_embedding_mode
            WHERE text='Sweden'
            """
        )
        assert type(result_df["answer"].iloc[0]) == list
        assert type(result_df["answer"].iloc[0][0]) == float

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_full_flow_in_embedding_mode_for_bulk_predictions_runs_no_errors(self, mock_handler):
        """
        Test the full flow in embedding mode for bulk predictions.
        """
        df = pd.DataFrame.from_dict({"text": [
            "Sweden",
            "Venus"
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.test_openai_bulk_full_flow_embedding_mode
            PREDICT answer
            USING
                engine='openai_engine',
                mode='embedding',
                model_name = 'text-embedding-ada-002',
                question_column = 'text';
            """
        )

        self.wait_predictor("proj", "test_openai_bulk_full_flow_embedding_mode")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_openai_bulk_full_flow_embedding_mode as p;
            """
        )
        assert type(result_df["answer"].iloc[0]) == list
        assert type(result_df["answer"].iloc[0][0]) == float
        assert type(result_df["answer"].iloc[1]) == list
        assert type(result_df["answer"].iloc[1][0]) == float

    def test_full_flow_in_image_mode_for_single_prediction_runs_no_errors(self):
        """
        Test the full flow in image mode for a single prediction.
        """
        self.run_sql(
            """
            CREATE MODEL proj.test_openai_single_full_flow_image_mode
            PREDICT answer
            USING
                engine='openai_engine',
                mode='image',
                prompt_template='Generate an image for: {{{{text}}}}'
            """
        )

        self.wait_predictor("proj", "test_openai_single_full_flow_image_mode")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_openai_single_full_flow_image_mode
            WHERE text='Leopard clubs playing in the jungle'
            """
        )
        assert type(result_df["answer"].iloc[0]) == str

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_full_flow_in_image_mode_for_bulk_predictions_runs_no_errors(self, mock_handler):
        """
        Test the full flow in image mode for bulk predictions.
        """
        df = pd.DataFrame.from_dict({"text": [
            "Leopard clubs playing in the jungle",
            "A beautiful sunset over the ocean"
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.test_openai_bulk_full_flow_image_mode
            PREDICT answer
            USING
                engine='openai_engine',
                mode='image',
                prompt_template='Generate an image for: {{{{text}}}}'
            """
        )

        self.wait_predictor("proj", "test_openai_bulk_full_flow_image_mode")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_openai_bulk_full_flow_image_mode as p;
            """
        )
        assert type(result_df["answer"].iloc[0]) == str
        assert type(result_df["answer"].iloc[1]) == str

    def test_full_flow_in_conversational_for_single_prediction_mode_runs_no_errors(self):
        """
        Test the full flow in conversational mode for a single prediction.
        """
        self.run_sql(
            """
            CREATE MODEL proj.test_openai_single_full_flow_conversational_mode
            PREDICT answer
            USING
                engine='openai_engine',
                mode='conversational',
                user_column='question',
                prompt='you are a helpful assistant',
                assistant_column='answer';
            """
        )

        self.wait_predictor("proj", "test_openai_single_full_flow_conversational_mode")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_openai_single_full_flow_conversational_mode
            WHERE question='What is the capital of Sweden?'
            """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_full_flow_in_conversational_mode_for_bulk_predictions_runs_no_errors(self, mock_handler):
        """
        Test the full flow in conversational mode for bulk predictions.
        """
        df = pd.DataFrame.from_dict({"question": [
            "What is the capital of Sweden?",
            "What are some cool places to visit there?"
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.test_openai_bulk_full_flow_conversational_mode
            PREDICT answer
            USING
                engine='openai_engine',
                mode='conversational',
                user_column='question',
                prompt='you are a helpful assistant',
                assistant_column='answer';
            """
        )

        self.wait_predictor("proj", "test_openai_bulk_full_flow_conversational_mode")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_openai_bulk_full_flow_conversational_mode as p;
            """
        )
        assert result_df["answer"].iloc[0] == ""
        assert "gamla stan" in result_df["answer"].iloc[1].lower()

    def test_full_flow_in_conversational_full_mode_for_single_prediction_runs_no_errors(self):
        """
        Test the full flow in conversational-full mode for a single prediction.
        """
        self.run_sql(
            """
            CREATE MODEL proj.test_openai_single_full_flow_conversational_full_mode
            PREDICT answer
            USING
                engine='openai_engine',
                mode='conversational-full',
                user_column='question',
                prompt='you are a helpful assistant',
                assistant_column='answer';
            """
        )

        self.wait_predictor("proj", "test_openai_single_full_flow_conversational_full_mode")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_openai_single_full_flow_conversational_full_mode
            WHERE question='What is the capital of Sweden?'
            """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_full_flow_in_conversational_full_mode_for_bulk_predictions_runs_no_errors(self, mock_handler):
        """
        Test the full flow in conversational-full mode for bulk predictions.
        """
        df = pd.DataFrame.from_dict({"question": [
            "What is the capital of Sweden?",
            "What are some cool places to visit there?"
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.test_openai_bulk_full_flow_conversational_full_mode
            PREDICT answer
            USING
                engine='openai_engine',
                mode='conversational-full',
                user_column='question',
                prompt='you are a helpful assistant',
                assistant_column='answer';
            """
        )

        self.wait_predictor("proj", "test_openai_bulk_full_flow_conversational_full_mode")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_openai_bulk_full_flow_conversational_full_mode as p;
            """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()
        assert "gamla stan" in result_df["answer"].iloc[1].lower()

    # TODO: Fix this test for fine-tuning
    # @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    # def test_full_flow_finetune_runs_no_errors(self, mock_handler):
    #     """
    #     Test the full flow for finetuning a model and making a prediction.
    #     """
    #     df = pd.DataFrame.from_dict(
    #         {
    #             "prompt": [
    #                 "What is the SQL syntax to connect a database to MindsDB?",
    #                 "What is the SQL command to connect to the demo postgres database for MindsDB learning hub examples?",
    #                 "What is the SQL syntax to create a MindsDB machine learning model?",
    #                 "What is the SQL syntax to join input data with predictions from a MindsDB machine learning model?"
    #             ],
    #             "completion": [
    #                 """
    #                 CREATE DATABASE datasource_name
    #                 [WITH] [ENGINE [=] engine_name] [,]
    #                 [PARAMETERS [=] {
    #                 "key": "value",
    #                 ...
    #                 }];
    #                 """,
    #                 """
    #                 CREATE DATABASE example_db
    #                 WITH
    #                 ENGINE = "postgres",
    #                 PARAMETERS = {
    #                     "user": "demo_user",
    #                     "password": "demo_password",
    #                     "host": "samples.mindsdb.com",
    #                     "port": "5432",
    #                     "database": "demo"
    #                 };
    #                 """,
    #                 """
    #                 CREATE MODEL
    #                 mindsdb.home_rentals_model
    #                 FROM example_db
    #                 (SELECT * FROM demo_data.home_rentals)
    #                 PREDICT rental_price;
    #                 """,
    #                 """
    #                 SELECT t.column_name, p.column_name, ...
    #                 FROM integration_name.table_name [AS] t
    #                 JOIN project_name.model_name [AS] p;
    #                 """
    #             ]

    #         }
    #     )
    #     self.set_handler(mock_handler, name="pg", tables={"df": df})

    #     self.run_sql(
    #         f"""
    #         CREATE MODEL proj.test_openai_full_flow_finetune
    #         PREDICT completion
    #         USING
    #             engine = 'openai_engine',
    #             model_name = 'davinci-002',
    #             prompt_template = 'Return a valid SQL string for the following question about MindsDB in-database machine learning: {{{{prompt}}}}';
    #         """
    #     )

    #     self.wait_predictor("proj", "test_openai_full_flow_finetune")

    #     self.run_sql(
    #         """
    #         FINETUNE proj.test_openai_full_flow_finetune
    #         FROM pg
    #             (SELECT prompt, completion FROM df);
    #         """
    #     )

    #     self.wait_predictor("proj", "test_openai_full_flow_finetune", finetune=True)

    #     result_df = self.run_sql(
    #         """
    #         SELECT prompt, completion
    #         FROM proj.test_openai_full_flow_finetune
    #         WHERE prompt = 'What is the SQL syntax to join input data with predictions from a MindsDB machine learning model?'
    #         USING max_tokens=400;
    #         """
    #     )
    #     assert "SELECT t.column_name, p.column_name, ..." in result_df["completion"].iloc[0].lower()


if __name__ == "__main__":
    pytest.main([__file__])
