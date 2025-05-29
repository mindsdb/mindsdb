from typing import Optional, Dict

import unify
import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine

from mindsdb.utilities import log

from mindsdb.integrations.utilities.handler_utils import get_api_key


logger = log.getLogger(__name__)


class UnifyHandler(BaseMLEngine):
    """
    Integration with the Unifyai Python Library
    """
    name = 'unify'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'using' not in args:
            raise Exception("Unify engine requires a USING clause! Refer to its documentation for more details.")

        self.generative = True
        self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:

        args = self.model_storage.json_get('args')

        input_keys = list(args.keys())

        logger.info(f"Input keys: {input_keys}!")

        if 'model' not in args['using']:
            raise Exception("Unify requires an model parameter in the USING clause! Refer to its documentation for more details.")
        model = args['using']['model']

        if 'provider' not in args['using']:
            raise Exception("Unify requires a provider parameter in the USING clause! Refer to its documentation for more details.")
        provider = args['using']['provider']

        self.endpoint = model + '@' + provider
        self.api_key = get_api_key('unify', args["using"], self.engine_storage, strict=False)
        available_endpoints = unify.utils.list_endpoints(api_key=self.api_key)
        if self.endpoint not in available_endpoints:
            raise Exception("The model, provider or their combination is not supported by Unify! The supported endpoints are: " + str(available_endpoints))

        question_column = args['using']['column']
        if question_column not in df.columns:
            raise RuntimeError(f'Column "{question_column}" not found in input data')

        result_df = pd.DataFrame()
        result_df['predictions'] = df[question_column].apply(self.predict_answer)
        result_df = result_df.rename(columns={'predictions': args['target']})

        return result_df

    def predict_answer(self, text):

        client = unify.Unify(self.endpoint, api_key=self.api_key)

        response = client.generate(text)

        return response
