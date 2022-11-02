import time
from unittest.mock import patch
import pandas as pd

from mindsdb_sql import parse_sql

# How to run:
#  env PYTHONPATH=./ pytest tests/unit/test_ml_handlers.py
# Warning: a big huggingface models will be downloaded

from tests.unit.executor_test_base import BaseExecutorTest


class TestHuggingface(BaseExecutorTest):

    def run_sql(self, sql):
        return self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )


    def hf_test_run(self, mock_handler, model_name, create_sql, predict_sql):

        # prepare table
        text_spammy = [
            'It is the best time to launch the Robot to get more money. https:\\/\\/Gof.bode-roesch.de\\/Gof',
            'Start making thousands of dollars every week just using this robot. https:\\/\\/Gof.coronect.de\\/Gof'
            ]

        text_short = ['I want to dance', 'Baking is the best']

        text_long = [
            "Dance is a performing art form consisting of sequences of movement, either improvised or purposefully selected. This movement has aesthetic and often symbolic value.[nb 1] Dance can be categorized and described by its choreography, by its repertoire of movements, or by its historical period or place of origin.",
            "Baking is a method of preparing food that uses dry heat, typically in an oven, but can also be done in hot ashes, or on hot stones. The most common baked item is bread but many other types of foods can be baked. Heat is gradually transferred from the surface of cakes, cookies, and pieces of bread to their center. As heat travels through, it transforms batters and doughs into baked goods and more with a firm dry crust and a softer center. Baking can be combined with grilling to produce a hybrid barbecue variant by using both methods simultaneously, or one after the other. Baking is related to barbecuing because the concept of the masonry oven is similar to that of a smoke pit."
            ]

        df = pd.DataFrame(data=[text_spammy, text_short, text_long]).T
        df.columns = ['text_spammy', 'text_short', 'text_long']

        self.set_handler(mock_handler, name='pg', tables={'df': df})

        # create predictor
        ret = self.run_sql(create_sql)
        assert ret.error_code is None

        # wait
        done = False
        for attempt in range(900):
            ret = self.run_sql(
                f"select status from huggingface.predictors where name='{model_name}'"
            )
            if len(ret.data) > 0:
                if ret.data[0][0] == 'complete':
                    done = True
                    break
                elif ret.data[0][0] == 'error':
                    break
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor didn't created")

        # use predictor
        ret = self.command_executor.execute_command(parse_sql(predict_sql, dialect='mindsdb'))
        assert ret.error_code is None

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_hf_classification_bin(self, mock_handler):


        # create predictor
        create_sql = '''
            CREATE PREDICTOR huggingface.spam_classifier
            predict PRED
            USING
                task='text-classification',
                model_name= "mrm8488/bert-tiny-finetuned-sms-spam-detection",
                input_column = 'text_spammy',
                labels=['ham','spam']
        '''

        model_name = 'spam_classifier'

        predict_sql = '''
            SELECT h.*
            FROM pg.df as t 
            JOIN huggingface.spam_classifier as h
        '''
        self.hf_test_run(mock_handler, model_name, create_sql, predict_sql)

        # one line prediction
        predict_sql = '''
            SELECT * from huggingface.spam_classifier
            where text_spammy= 'It is the best time to launch the Robot to get more money. https:\\/\\/Gof.bode-roesch.de\\/Gof'
        '''
        # use predictor
        ret = self.command_executor.execute_command(parse_sql(predict_sql, dialect='mindsdb'))
        assert ret.error_code is None

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_hf_classification_multy(self, mock_handler):

        # create predictor
        create_sql = '''                
           CREATE PREDICTOR huggingface.sentiment_classifier
           predict PRED
           USING
                task='text-classification',
                model_name= "cardiffnlp/twitter-roberta-base-sentiment",
                input_column = 'text_short',
                labels=['neg','neu','pos']
        '''

        model_name = 'sentiment_classifier'

        predict_sql = '''
            SELECT h.*
            FROM pg.df as t 
            JOIN huggingface.sentiment_classifier as h
        '''
        self.hf_test_run(mock_handler, model_name, create_sql, predict_sql)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_hf_zero_shot(self, mock_handler):

        # create predictor
        create_sql = '''                
         CREATE PREDICTOR huggingface.zero_shot_tcd
            predict PREDZS
         USING
            task="zero-shot-classification",
            model_name= "facebook/bart-large-mnli",
            input_column = "text_short",
            candidate_labels=['travel', 'cooking', 'dancing']
        '''

        model_name = 'zero_shot_tcd'

        predict_sql = '''
            SELECT h.*
            FROM pg.df as t 
            JOIN huggingface.zero_shot_tcd as h
        '''
        self.hf_test_run(mock_handler, model_name, create_sql, predict_sql)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_hf_translation(self, mock_handler):

        # create predictor
        create_sql = '''                
         CREATE PREDICTOR huggingface.translator_en_fr
            predict TRANSLATION
        USING
            task = "translation",
            model_name = "t5-base",
            input_column = "text_short",
            lang_input = "en",
            lang_output = "fr"
        '''

        model_name = 'translator_en_fr'

        predict_sql = '''
            SELECT h.*
            FROM pg.df as t 
            JOIN huggingface.translator_en_fr as h
        '''
        self.hf_test_run(mock_handler, model_name, create_sql, predict_sql)


