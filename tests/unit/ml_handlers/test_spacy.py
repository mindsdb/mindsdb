from unittest.mock import patch
import pandas as pd

from mindsdb_sql import parse_sql

from unit.executor_test_base import BaseExecutorTest


class TestSpacy(BaseExecutorTest):
    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [col.alias if col.alias is not None else col.name for col in ret.columns]
            return pd.DataFrame(ret.data, columns=columns)

    def test_spacy_ner(self):
        self.run_sql("CREATE DATABASE proj")

        text = ["Apple is looking at buying U.K. startup for $1 billion"]
        df = pd.DataFrame(text, columns=['text'])
        self.set_data('df', df)

        self.run_sql(
            """
            CREATE MODEL proj.spacy__ner__model
            PREDICT recognition
            USING
              engine = 'spacy',
              linguistic_feature = 'ner',
              target_column = 'text';
            """
        )
        self.wait_for_predictor('proj', 'spacy__ner__model')

        result_df = self.run_sql(
            """
            SELECT recognition
            FROM proj.spacy__ner__model
            WHERE
            text='Apple is looking at buying U.K. startup for $1 billion';
            """
        )
        assert len(result_df["recognition"].iloc[0]) > 0

    def test_spacy_lemmatization(self):
        self.run_sql("CREATE DATABASE proj")

        text = ["Apple is looking at buying U.K. startup for $1 billion"]
        df = pd.DataFrame(text, columns=['text'])

        self.set_data('df', df)

        self.run_sql(
            """
            CREATE MODEL proj.spacy__lemmatization__model
            PREDICT recognition
            USING
              engine = 'spacy',
              linguistic_feature = 'lemmatization',
              target_column = 'text';
            """
        )

        self.wait_for_predictor('proj', 'spacy__lemmatization__model')

        result_df = self.run_sql(
            """
            SELECT recognition
            FROM proj.spacy__lemmatization__model
            WHERE
            text='Apple is looking at buying U.K. startup for $1 billion';
            """
        )

        assert len(result_df["recognition"].iloc[0]) > 0

    def test_spacy_dependency_parsing(self):
        self.run_sql("CREATE DATABASE proj")

        text = ["Apple is looking at buying U.K. startup for $1 billion"]
        df = pd.DataFrame(text, columns=['text'])

        self.set_data('df', df)

        self.run_sql(
            """
            CREATE MODEL proj.spacy__dependency_parsing__model
            PREDICT recognition
            USING
              engine = 'spacy',
              linguistic_feature = 'dependency-parsing',
              target_column = 'text';
            """
        )

        self.wait_for_predictor('proj', 'spacy__dependency_parsing__model')

        result_df = self.run_sql(
            """
            SELECT recognition
            FROM proj.spacy__dependency_parsing__model
            WHERE
            text='Apple is looking at buying U.K. startup for $1 billion';
            """
        )

        assert len(result_df["recognition"].iloc[0]) > 0

    def test_spacy_pos_tagging(self):
        self.run_sql("CREATE DATABASE proj")

        text = ["Apple is looking at buying U.K. startup for $1 billion"]
        df = pd.DataFrame(text, columns=['text'])

        self.set_data('df', df)

        self.run_sql(
            """
            CREATE MODEL proj.spacy__pos_tag__model
            PREDICT recognition
            USING
              engine = 'spacy',
              linguistic_feature = 'pos-tag',
              target_column = 'text';
            """
        )

        self.wait_for_predictor('proj', 'spacy__pos_tag__model')

        result_df = self.run_sql(
            """
            SELECT recognition
            FROM proj.spacy__pos_tag__model
            WHERE
            text='Apple is looking at buying U.K. startup for $1 billion';
            """
        )

        assert len(result_df["recognition"].iloc[0]) > 0

    def test_spacy_morphology(self):
        self.run_sql("CREATE DATABASE proj")

        text = ["Apple is looking at buying U.K. startup for $1 billion"]
        df = pd.DataFrame(text, columns=['text'])

        self.set_data('df', df)

        self.run_sql(
            """
            CREATE MODEL proj.spacy__morphology__model
            PREDICT recognition
            USING
              engine = 'spacy',
              linguistic_feature = 'morphology',
              target_column = 'text';
            """
        )

        self.wait_for_predictor('proj', 'spacy__morphology__model')

        result_df = self.run_sql(
            """
            SELECT recognition
            FROM proj.spacy__morphology__model
            WHERE
            text='Apple is looking at buying U.K. startup for $1 billion';
            """
        )

        assert len(result_df["recognition"].iloc[0]) > 0
