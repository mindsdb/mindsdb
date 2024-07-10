from typing import Optional

import pandas as pd

from mindsdb.integrations.handlers.sentence_transformers_handler.settings import Parameters

from mindsdb.integrations.handlers.rag_handler.settings import load_embeddings_model, df_to_documents


from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class SentenceTransformersHandler(BaseMLEngine):
    name = "sentence transformers"

    def __init__(self, model_storage, engine_storage, **kwargs) -> None:
        super().__init__(model_storage, engine_storage, **kwargs)
        self.generative = True

    def create(self, target, df=None, args=None, **kwargs):
        """creates embeddings model and persists"""

        args = args["using"]

        valid_args = Parameters(**args)
        self.model_storage.json_set("args", valid_args.model_dump())

    def predict(self, df, args=None):
        """loads persisted embeddings model and gets embeddings on input text column(s)"""

        args = self.model_storage.json_get("args")

        if isinstance(df['content'].iloc[0], list) and len(df['content']) == 1:
            # allow user to pass in a list of strings in where clause
            # i.e where content = ['hello', 'world'] or where content = (select content from some_db.some_table)
            input_df = df.copy()
            df = pd.DataFrame(data={"content": input_df['content'].iloc[0]})

        # get text columns if specified
        if isinstance(args['text_columns'], str):
            columns = [args['text_columns']]

        elif isinstance(args['text_columns'], list):
            columns = args['text_columns']

        elif args['text_columns'] is None:
            # assume all columns are text columns
            logger.info("No text columns specified, assuming all columns are text columns")
            columns = df.columns.tolist()

        else:
            raise ValueError(f"Invalid value for text_columns: {args['text_columns']}")

        documents = df_to_documents(df=df, page_content_columns=columns)

        content = [doc.page_content for doc in documents]
        metadata = [doc.metadata for doc in documents]

        model = load_embeddings_model(args['embeddings_model_name'])

        embeddings = model.embed_documents(texts=content)

        embeddings_df = pd.DataFrame(data={"content": content, "embeddings": embeddings, "metadata": metadata})

        return embeddings_df

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:

        args = self.model_storage.json_get("args")

        if attribute == "args":
            return pd.DataFrame(args.items(), columns=["key", "value"])
