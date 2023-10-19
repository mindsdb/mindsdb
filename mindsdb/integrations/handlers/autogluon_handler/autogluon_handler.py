from typing import Optional

import dill
import pandas as pd

from mindsdb.integrations.handlers.sentence_transformer_handler.settings import (
    Parameters,
    df_to_documents,
    load_embeddings_model,
)
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

logger = log.get_log(__name__)


class SentenceTransformerHandler(BaseMLEngine):
    name = "sentence transformer"

    def create(self, target, df=None, args=None, **kwargs):
        """creates embeddings model and persists"""

        args = args["using"]

        valid_args = Parameters(**args)

        model = load_embeddings_model(valid_args.embeddings_model_name)

        self.model_storage.file_set("model", dill.dumps(model))
        self.model_storage.json_set("args", valid_args.dict())

    def predict(self, df, args=None):
        """loads persisted embeddings model and gets embeddings on input text column(s)"""

        args = args["predict_params"]
        columns = args.get("columns")

        if columns:
            if isinstance(args["columns"], str):
                columns = [columns]

        else:
            logger.info("no columns specified, all columns from input will be embedded")

            columns = df.columns

        documents = df_to_documents(df=df, page_content_columns=columns)

        model = dill.loads(self.model_storage.file_get("model"))

        embeddings = []

        for _, document in enumerate(documents):
            _embeddings = model.encode(document.text).tolist()
            embeddings.append(_embeddings)

        embeddings_df = pd.DataFrame(data={"embeddings": embeddings})

        return embeddings_df

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:

        args = self.model_storage.json_get("args")

        if attribute == "args":
            return pd.DataFrame(args.items(), columns=["key", "value"])
        


import os
from typing import Optional, Dict

from anthropic import Anthropic, HUMAN_PROMPT, AI_PROMPT
import pandas as pd

from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.base import BaseMLEngine

from mindsdb.utilities.log import get_log


logger = get_log("integrations.anthropic_handler")

class AnthropicHandler(BaseMLEngine):
    """
    Integration with the Anthropic LLM Python Library
    """
    name = 'anthropic'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_chat_model = 'claude-2'
        self.supported_chat_models = ['claude-1', 'claude-2']
        self.default_max_tokens = 100
        self.generative = True
        self.connection = None

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:


        if 'using' not in args:
            raise Exception("Anthropic engine requires a USING clause! Refer to its documentation for more details.")

        if 'model' not in args['using']:
            args['using']['model'] = self.default_chat_model
        elif args['using']['model'] not in self.supported_chat_models:
            raise Exception(f"Invalid chat model. Please use one of {self.supported_chat_models}")

        if 'max_tokens' not in args['using']:
            args['using']['max_tokens'] = self.default_max_tokens
        
        self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:

        args = self.model_storage.json_get('args')
        api_key = self._get_anthropic_api_key(args)

        self.connection = Anthropic(api_key=api_key,)

        input_column = args['using']['column']

        if input_column not in df.columns:
            raise RuntimeError(f'Column "{input_column}" not found in input data')
        
        result_df = pd.DataFrame() 

        result_df['predictions'] = df[input_column].apply(self.predict_answer)     

        result_df = result_df.rename(columns={'predictions': args['target']})
        
        return result_df


    def _get_anthropic_api_key(self, args, strict=True):
        """ 
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. ANTHROPIC_API_KEY env variable
            4. anthropic.api_key setting in config.json
        """

        # 1
        if 'api_key' in args['using']:
            return args['using']['api_key']
        # 2
        connection_args = self.engine_storage.get_connection_args()
        if 'api_key' in connection_args:
            return connection_args['api_key']
        # 3
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if api_key is not None:
            return api_key
        # 4
        config = Config()
        anthropic_cfg = config.get('anthropic', {})
        if 'api_key' in anthropic_cfg:
            return anthropic_cfg['api_key']

        if strict:
            raise Exception(f'Missing API key "api_key". Either re-create this ML_ENGINE specifying the `api_key` parameter,\
                 or re-create this model and pass the API key with `USING` syntax.')  

    def predict_answer(self,text):
        """ 
        connects with anthropic api to predict the answer for the particular question

        """ 

        args = self.model_storage.json_get('args')

        completion = self.connection.completions.create(
            model=args['using']['model'],
            max_tokens_to_sample=args['using']['max_tokens'],
            prompt=f"{HUMAN_PROMPT} {text} {AI_PROMPT}",
        )

        return completion.completion

