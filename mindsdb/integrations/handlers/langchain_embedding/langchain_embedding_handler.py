import importlib
from typing import Dict, Optional, Union

import pandas as pd
from langchain.embeddings.base import Embeddings
from pandas import DataFrame

from mindsdb.integrations.libs.base import BaseMLEngine


def get_langchain_class(class_name: str) -> Embeddings:
    """Returns the class object of the handler class.

    Args:
        class_name (str): Name of the class

    Returns:
        langchain.embeddings.BaseEmbedding: The class object
    """
    try:
        module = importlib.import_module("langchain.embeddings")
    except ImportError:
        raise Exception(
            "The langchain is not installed. Please install it with `pip install langchain`."
        )
    try:
        class_ = getattr(module, class_name)
    except AttributeError:
        raise Exception(
            f"Could not find the class {class_name} in langchain.embeddings. Please check the class name."
        )
    return class_


def serialize_model_to_storage(embedding_model: Embeddings, model_storage):
    """
    Serializes the model to the model storage
    """

    # save the model to the model storage
    serialized_dict = embedding_model.dict()
    serialized_dict["_model_class"] = embedding_model.__class__.__name__
    model_storage.json_set("_model", serialized_dict)


def deserialize_model_from_storage(model_storage) -> Embeddings:
    """
    Deserializes the model from the model storage
    """
    model_info = model_storage.json_get("_model")
    class_name = model_info.pop("_model_class")
    MODEL_CLASS = get_langchain_class(class_name)
    serialized_dict = model_info
    model = MODEL_CLASS(**serialized_dict)
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
        class_ = user_args.pop("class", self.DEFAULT_EMBEDDING_CLASS)

        # get the class object
        MODEL_CLASS = get_langchain_class(class_)
        # this may raise an exception if
        # the arguments are not sufficient to create such as class
        # due to e.g., lack of API key
        # But the validation logic is handled by langchain and pydantic
        model = MODEL_CLASS(**user_args)

        # save the model to the model storage
        serialize_model_to_storage(model, self.model_storage)
        user_args["target"] = target
        self.model_storage.json_set("args", user_args)

    def predict(self, df: DataFrame, args) -> DataFrame:
        # reconstruct the model from the model storage
        model = deserialize_model_from_storage(self.model_storage)

        # get the target from the model storage
        target = self.model_storage.json_get("args").get("target")
        # check if the target is in the df
        if target not in df.columns:
            raise Exception(f"Target column {target} not found in the input dataframe.")

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
