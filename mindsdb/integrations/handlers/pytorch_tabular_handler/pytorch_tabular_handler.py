from typing import Optional, Dict
import re

import dill
import pandas as pd
import ast

import io
import torch
from pytorch_tabular.config import DataConfig, OptimizerConfig, TrainerConfig
from pytorch_tabular.models import CategoryEmbeddingModelConfig
from pytorch_tabular.models.common.heads import LinearHeadConfig
from pytorch_tabular.tabular_model import TabularModel

from mindsdb.integrations.libs.base import BaseMLEngine

class Pytorch_Tabular_Handler(BaseMLEngine):
    """
    Implements pytorch tabular to train deep neural networks.
    """
    name = 'pytorch_tabular'

    @staticmethod
    def create_validation(self,args: Optional[dict] = None) -> None:
        task_supported = ['regression','classification','backbone']
        initialization_supported = ['kaiming','xavier','random']
        args = args["using"]
        #pattern for layers regex
        pattern = r'^\d+-\d+-\d+$'
        if "task" in args:
            if args["task"] not in task_supported:
                raise Exception(f"Please specify task parameter supported : {task_supported}")
        if "initialization" not in args or args["initialization"] not in initialization_supported:
            raise Exception(f"Initialization scheme choices are : {initialization_supported}")
        if "layers" in args:
            if not re.match(pattern,args["layers"]):
                raise Exception(f"Please specify layers in format : '128-64-32'")

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        train_data = df
        # Set default values if user does not specify
        categorical_columns = None
        dropout = 0.0
        epochs = 3
        continuous_columns = None
        batch_size = 32
        layers = '128-64-32'
        args = args["using"]
        if 'categorical_cols' in args:
            categorical_columns = ast.literal_eval(args["categorical_cols"])
        if 'continuous_cols' in args:
            continuous_columns = ast.literal_eval(args['continuous_cols'])
        if 'drop_out' in args:
            dropout = int(args['drop_out'])
        if 'epochs' in args:
            epochs = int(args['epochs'])
        if 'batch_size' in args:
            batch_size = int(args['batch_size'])
        if 'layers' in args:
            layers = args['layers']
        data_config = DataConfig(
            target = [target],
            continuous_cols = continuous_columns,
            categorical_cols = categorical_columns,
            continuous_feature_transform=None,
            normalize_continuous_features=True,
        )
        head_config = LinearHeadConfig(
            layers=layers, dropout=dropout, initialization=args['initialization']
            # No additional layer in head, just a mapping layer to output_dim
        ).__dict__
        model_config = CategoryEmbeddingModelConfig(
            task = args['task'],
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
        self.model_storage.json_set('args', args)
        self.model_storage.file_set('model.pt', dill.dumps(tabular_model))

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        tabular_model = torch.load(self.model_storage.file_get('model'))
        predictions = tabular_model.predict(df)
        return predictions

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        des_dict = {'epochs': args['epochs'], 'initialization': args['initialization'], 'task': args['task'],
                    'categorical_columns': args['categorical_cols'], 'continuous_columns': args['continuous_cols']}
        df_describe = pd.DataFrame.from_dict(des_dict)
        return df_describe