from typing import Optional
from Optional import Dict
import pandas as pd

from pytorch_tabular.config import DataConfig, OptimizerConfig, TrainerConfig
from pytorch_tabular.models import CategoryEmbeddingModelConfig
from pytorch_tabular.models.common.heads import LinearHeadConfig
from pytorch_tabular.tabular_model import TabularModel

from mindsdb.integrations.libs.base import BaseMLEngine

class Pytorch_TabularHandler(BaseMLEngine):
    """
    Implements pytorch tabular to train deep neural networks on tabular dataset
    """
    name = 'pytorch_tabular'

    @staticmethod
    def create_validation(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        task_supported = ['regression','classification','backbone']
        activation_supported = ['RelU','TanH','LeakyRelU']
        initialization_supported = ['kaiming','xavier','random']
        args = args["using"]
        if "task" not in args["using"]:
            if args["task"] not in task_supported:
                raise Exception(f"Please specify task parameter supported : {task_supported}")
        if args["activation"] not in activation_supported:
            raise Exception(f"The supported activation functions are {activation_supported}")
        if args["initialization"] not in initialization_supported:
            raise Exception(f"Initialization scheme choices are : {initialization_supported}")
        if "target" not in args:
            raise Exception("Please provide the target column")

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        args = args["using"]


    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        raise TypeError("Not implemented yet")

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
