from typing import Optional
from Optional import Dict
import pandas as pd
import ast

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
        if "task" not in args:
            if args["task"] not in task_supported:
                raise Exception(f"Please specify task parameter supported : {task_supported}")
        if "activation" not in args or args["activation"] not in activation_supported:
            raise Exception(f"The supported activation functions are {activation_supported}")
        if "initialization" not in args or args["initialization"] not in initialization_supported:
            raise Exception(f"Initialization scheme choices are : {initialization_supported}")
        if "target" not in args:
            raise Exception("Please provide the target column")

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        args = args["using"]
        categorical_columns = None
        dropout = 0.0
        continuous_columns = None
        if 'categorical_cols' in args:
            categorical_columns = ast.literal_eval(args["categorical"])
        if 'continuous_cols' in args:
            continuous_columns = ast.literal_eval(args['continous_cols'])
        if 'drop_out' in args:
            dropout = int(args['drop_out'])
        data_config = DataConfig(
            target = target,
            continuous_cols = continuous_columns,
            categorical_cols = categorical_columns,
            continuous_feature_transform=None,
            normalize_continuous_features=True,
        )
        head_config = LinearHeadConfig(
            layers='', dropout=dropout, initialization=args['initialization']
            # No additional layer in head, just a mapping layer to output_dim
        ).__dict__




    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        raise TypeError("Not implemented yet")

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
