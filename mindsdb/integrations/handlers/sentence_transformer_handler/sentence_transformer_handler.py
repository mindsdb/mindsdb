from typing import Optional

import dill
import pandas as pd
from integrations.handlers.sentence_transformer_handler.settings import (
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
