from typing import Optional
from Optional import Dict
import pandas as pd
import ast
import torch
import os

from pytorch_tabular.config import DataConfig, OptimizerConfig, TrainerConfig
from pytorch_tabular.models import CategoryEmbeddingModelConfig
from pytorch_tabular.models.common.heads import LinearHeadConfig
from pytorch_tabular.tabular_model import TabularModel

from mindsdb.integrations.libs.base import BaseMLEngine

class Pytorch_TabularHandler(BaseMLEngine):
    """
    Implements pytorch tabular to train deep neural networks.
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
        train_data = df
        args = args["using"]
        categorical_columns = None
        dropout = 0.0
        epochs = 3
        continuous_columns = None
        batch_size = 32
        if not train_data:
            raise Exception("Please provide data for the model to train on")
        if 'categorical_cols' in args:
            categorical_columns = ast.literal_eval(args["categorical"])
        if 'continuous_cols' in args:
            continuous_columns = ast.literal_eval(args['continous_cols'])
        if 'drop_out' in args:
            dropout = int(args['drop_out'])
        if 'epochs' in args:
            epochs = int(args['epochs'])
        if 'batch_size' in args:
            batch_size = int(args['batch_size'])
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
        model_config = CategoryEmbeddingModelConfig(
            task = args["task"],
            head_config = head_config
        )
        trainer_config = TrainerConfig(auto_lr_find=True,
                                       fast_dev_run=False,
                                       max_epochs=epochs,
                                       batch_size=batch_size)

        optimizer_config = OptimizerConfig()

        # Create the tabular model
        tabular_model = TabularModel(
            data_config=data_config,
            model_config=model_config,
            optimizer_config=optimizer_config,
            trainer_config=trainer_config,
        )
        tabular_model.fit(train=train_data)

        # Save the trained model
        torch.save(tabular_model,"pytorch_tabular.pt")


    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        file_path = 'pytorch_tabular_handler.py'
        if os.path.isfile(file_path):
            tabular_model = torch.load(file_path)
        else:
            raise Exception("Trained Model not loaded successfully, Please check if a trained model exists")

        predictions = tabular_model.predict(df)
        return predictions
    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
