from typing import Optional

import dill
import pandas as pd
from integrations.handlers.sentence_transformer_handler.settings import (
    Embeddings,
    df_to_documents,
    load_embeddings_model,
)

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

logger = log.get_log(__name__)


class SentenceTransformerHandler(BaseMLEngine):
    name = "sentence transformer"

    def create(self, target, df=None, args=None, **kwargs):

        args = args["using"]

        model = load_embeddings_model(args["embeddings_model_name"])

        self.model_storage.file_set("model", dill.dumps(model))
        self.model_storage.json_set("args", args)

    def predict(self, df, args=None):

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
        metadata = []

        for _, document in enumerate(documents):

            _metadata = document.metadata
            # add text to metadata
            _metadata["text"] = document.text

            _embeddings = model.encode(document.text).tolist()

            metadata.append(_metadata)
            embeddings.append(_embeddings)

        embeddings_df = pd.DataFrame(
            data={"embeddings": embeddings, "metadata": metadata}
        )

        return embeddings_df

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:

        args = self.model_storage.json_get("args")

        if attribute == "args":
            return pd.DataFrame(args.items(), columns=["key", "value"])
