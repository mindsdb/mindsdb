from unittest.mock import patch
import pandas as pd

from mindsdb_sql import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest


class TestSpacy(BaseExecutorTest):
    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [col.alias if col.alias is not None else col.name for col in ret.columns]
            return pd.DataFrame(ret.data, columns=columns)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_spacy_ner(self, mock_handler):
        self.run_sql("CREATE DATABASE proj")

        text = ["Apple is looking at buying U.K. startup for $1 billion"]
        df = pd.DataFrame(text, columns=['text'])

        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.spacy__ner__model
            PREDICT recognition
            USING
              engine = 'spacy'
              linguistic_feature = 'ner',
              target_column = 'text';
            """
        )

        result_df = self.run_sql(
            """
            SELECT recognition
            FROM proj.spacy__ner__model
            WHERE
            text='Apple is looking at buying U.K. startup for $1 billion';
            """
        )

        assert "{(28, 32, 'GPE'), (45, 55, 'MONEY'), (1, 6, 'ORG')}" in result_df["recognition"].iloc[0]

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_spacy_lemmatization(self, mock_handler):
        self.run_sql("CREATE DATABASE proj")

        text = ["Apple is looking at buying U.K. startup for $1 billion"]
        df = pd.DataFrame(text, columns=['text'])

        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.spacy__lemmatization__model
            PREDICT recognition
            USING
              engine = 'spacy'
              linguistic_feature = 'lemmatization',
              target_column = 'text';
            """
        )

        result_df = self.run_sql(
            """
            SELECT recognition
            FROM proj.spacy__lemmatization__model
            WHERE
            text='Apple is looking at buying U.K. startup for $1 billion';
            """
        )

        assert "{'startup', 'Apple', '1', 'for', 'buy', '$', 'at', 'billion', '"', \'U.K.\', \'be\', \'look\'}' in result_df["recognition"].iloc[0]

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_spacy_dependency_parsing(self, mock_handler):
        self.run_sql("CREATE DATABASE proj")

        text = ["Apple is looking at buying U.K. startup for $1 billion"]
        df = pd.DataFrame(text, columns=['text'])

        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.spacy__dependency_parsing__model
            PREDICT recognition
            USING
              engine = 'spacy'
              linguistic_feature = 'dependency-parsing',
              target_column = 'text';
            """
        )

        result_df = self.run_sql(
            """
            SELECT recognition
            FROM proj.spacy__dependency_parsing__model
            WHERE
            text='Apple is looking at buying U.K. startup for $1 billion';
            """
        )

        assert '{('"', \'punct', 'looking', 'VERB', '[]'), ('for', 'prep', 'startup', 'NOUN', '[billion]'), ('1', 'compound', 'billion', 'NUM', '[]'), ('Apple', 'nsubj', 'looking', 'VERB', '[]'), ('buying', 'pcomp', 'at', 'ADP', '[U.K.]'), ('looking', 'ROOT', 'looking', 'VERB', '[\", Apple, is, at, startup, \"]'), ('$', 'quantmod', 'billion', 'NUM', '[]'), ('is', 'aux', 'looking', 'VERB', '[]'), ('billion', 'pobj', 'for', 'ADP', '[$, 1]'), ('startup', 'dep', 'looking', 'VERB', '[for]'), ('at', 'prep', 'looking', 'VERB', '[buying]'), ('U.K.', 'dobj', 'buying', 'VERB', '[]')}" in result_df["recognition"].iloc[0]

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_spacy_pos_tagging(self, mock_handler):
        self.run_sql("CREATE DATABASE proj")

        text = ["Apple is looking at buying U.K. startup for $1 billion"]
        df = pd.DataFrame(text, columns=['text'])

        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.spacy__pos_tag__model
            PREDICT recognition
            USING
              engine = 'spacy'
              linguistic_feature = 'pos-tag',
              target_column = 'text';
            """
        )

        result_df = self.run_sql(
            """
            SELECT recognition
            FROM proj.spacy__pos_tag__model
            WHERE
            text='Apple is looking at buying U.K. startup for $1 billion';
            """
        )

        assert "{('startup', 'startup', 'NOUN', 'NN', 'dep', 'xxxx', True, False), ('buying', 'buy', 'VERB', 'VBG', 'pcomp', 'xxxx', True, False), ('U.K.', 'U.K.', 'PROPN', 'NNP', 'dobj', 'X.X.', False, False), ('1', '1', 'NUM', 'CD', 'compound', 'd', False, False), ('for', 'for', 'ADP', 'IN', 'prep', 'xxx', True, True), ('$', '$', 'SYM', '$', 'quantmod', '$', False, False), ('Apple', 'Apple', 'PROPN', 'NNP', 'nsubj', 'Xxxxx', True, False), ('billion', 'billion', 'NUM', 'CD', 'pobj', 'xxxx', True, False), ('"', '"', 'PUNCT', "''", 'punct', '"', False, False), (\'looking\', \'look\', \'VERB\', \'VBG\', \'ROOT\', \'xxxx\', True, False), ('"', '"', \'PUNCT\', \'``\', \'punct\', '"', False, False), ('at', 'at', 'ADP', 'IN', 'prep', 'xx', True, True), ('is', 'be', 'AUX', 'VBZ', 'aux', 'xx', True, True)}" in result_df["recognition"].iloc[0]

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_spacy_morphology(self, mock_handler):
        self.run_sql("CREATE DATABASE proj")

        text = ["Apple is looking at buying U.K. startup for $1 billion"]
        df = pd.DataFrame(text, columns=['text'])

        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.spacy__morphology__model
            PREDICT recognition
            USING
              engine = 'spacy'
              linguistic_feature = 'morphology',
              target_column = 'text';
            """
        )

        result_df = self.run_sql(
            """
            SELECT recognition
            FROM proj.spacy__morphology__model
            WHERE
            text='Apple is looking at buying U.K. startup for $1 billion';
            """
        )

        assert "{('"', \'PunctSide=Ini|PunctType=Quot\'), (\'billion\', \'NumType=Card\'), (\'U.K.\', \'Number=Sing\'), (\'at\', ''), (\'Apple\', \'Number=Sing\'), (\'looking\', \'Aspect=Prog|Tense=Pres|VerbForm=Part\'), (\'buying\', \'Aspect=Prog|Tense=Pres|VerbForm=Part\'), ('"', 'PunctSide=Fin|PunctType=Quot'), ('is', 'Mood=Ind|Number=Sing|Person=3|Tense=Pres|VerbForm=Fin'), ('$', ''), ('1', 'NumType=Card'), ('for', ''), ('startup', 'Number=Sing')}" in result_df["recognition"].iloc[0]
