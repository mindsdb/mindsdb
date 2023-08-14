import importlib
from typing import Dict, Union

import pandas as pd
from langchain.embeddings.base import Embeddings
from pandas import DataFrame

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities.log import get_log

LOG = get_log("langchain_embedding")

# construct the embedding model name to the class mapping
# we try to import all embedding models from langchain.embeddings
# for each class, we get a more user friendly name for it
# E.g. OpenAIEmbeddings -> OpenAI
# This is used for the user to select the embedding model
EMBEDDING_MODELS = {}

try:
    module = importlib.import_module("langchain.embeddings")
    # iterate __all__ to get all the classes
    for class_name in module.__all__:
        class_ = getattr(module, class_name)
        if not issubclass(class_, Embeddings):
            continue
        # convert the class name to a more user friendly name
        # e.g. OpenAIEmbeddings -> OpenAI
        user_friendly_name = class_name.replace("Embeddings", "")
        EMBEDDING_MODELS[user_friendly_name] = class_name
        EMBEDDING_MODELS[user_friendly_name.lower()] = class_name

except ImportError:
    raise Exception(
        "The langchain is not installed. Please install it with `pip install langchain`."
    )


def get_langchain_class(class_name: str) -> Embeddings:
    """Returns the class object of the handler class.

    Args:
        class_name (str): Name of the class

    Returns:
        langchain.embeddings.BaseEmbedding: The class object
    """
    try:
        module = importlib.import_module("langchain.embeddings")
        class_ = getattr(module, class_name)
    except ImportError:
        raise Exception(
            "The langchain is not installed. Please install it with `pip install langchain`."
        )
    except AttributeError:
        raise Exception(
            f"Could not find the class {class_name} in langchain.embeddings. Please check the class name."
        )
    return class_


def construct_model_from_args(args: Dict) -> Embeddings:
    """
    Deserializes the model from the model storage
    """
    target = args.pop("target", None)
    class_name = args.pop("class", LangchainEmbeddingHandler.DEFAULT_EMBEDDING_CLASS)
    if class_name in EMBEDDING_MODELS:
        LOG.info(
            f"Mapping the user friendly name {class_name} to the class name: {EMBEDDING_MODELS[class_name]}"
        )
        class_name = EMBEDDING_MODELS[class_name]
    MODEL_CLASS = get_langchain_class(class_name)
    serialized_dict = args
    model = MODEL_CLASS(**serialized_dict)
    if target is not None:
        args["target"] = target
    args["class"] = class_name
    return model


class LangchainEmbeddingHandler(BaseMLEngine):
    """
    Bridge class to connect langchain.embeddings module to mindsDB
    """

    DEFAULT_EMBEDDING_CLASS = "OpenAIEmbeddings"

    def __init__(self, model_storage, engine_storage, **kwargs) -> None:
        super().__init__(model_storage, engine_storage, **kwargs)

    def create(
        self,
        target: str,
        df: Union[DataFrame, None] = None,
        args: Union[Dict, None] = None,
    ) -> None:
        # get the class name from the args
        user_args = args.get("using", {})
        # this may raise an exception if
        # the arguments are not sufficient to create such as class
        # due to e.g., lack of API key
        # But the validation logic is handled by langchain and pydantic
        construct_model_from_args(user_args)

        # save the model to the model storage
        user_args["target"] = target
        self.model_storage.json_set("args", user_args)

    def predict(self, df: DataFrame, args) -> DataFrame:
        # reconstruct the model from the model storage
        user_args = self.model_storage.json_get("args")
        model = construct_model_from_args(user_args)

        # get the target from the model storage
        target = user_args.get("target")
        # check if the target is in the df
        if target is None or target not in df.columns:
            raise Exception(
                f"Target column {target} not found in the input dataframe. Available columns are {df.columns}"
            )

        # run the actual embedding vector generation
        texts = df[target].tolist()
        embeddings = model.embed_documents(texts)

        # create a new dataframe with the embeddings
        df_embeddings = df.assign(embeddings=embeddings)

        return df_embeddings

    def finetune(
        self, df: Union[DataFrame, None] = None, args: Union[Dict, None] = None
    ) -> None:
        # re-save the model to the model storage
        return self.create(df=df, args=args)

    def describe(self, attribute: Union[str, None] = None) -> DataFrame:
        args = self.model_storage.json_get("args")

        if attribute == "args":
            return pd.DataFrame(args.items(), columns=["key", "value"])
        elif attribute == "metadata":
            return pd.DataFrame(
                [
                    ("model_class", self.model_storage.json_get("model_class")),
                ],
                columns=["key", "value"],
            )

        else:
            tables = ("args", "metadata")
            return pd.DataFrame(tables, columns=["tables"])
