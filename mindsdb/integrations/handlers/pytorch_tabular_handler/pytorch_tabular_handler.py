from typing import Optional, Dict
import re

import dill
import pandas as pd
import ast

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
    def create_validation(self, args: Optional[dict] = None) -> None:
        task_supported = ['regression', 'classification', 'backbone']
        initialization_supported = ['kaiming', 'xavier', 'random']
        args = args["using"]
        # pattern for layers regex
        pattern = r'^\d+-\d+-\d+$'
        if "task" in args and args["task"] not in task_supported:
            raise Exception(f"Please specify task parameter supported : {task_supported}")
        if "initialization" in args and args["initialization"] not in initialization_supported:
            raise Exception(f"Initialization scheme choices are : {initialization_supported}")
        if "layers" in args:
            if not re.match(pattern, args["layers"]):
                raise Exception("Please specify layers in format : '128-64-32'")

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        train_data = df
        args = args["using"]
        categorical_columns = args.get('categorical_cols', None)
        continuous_columns = args.get('continuous_cols', None)

        # If string, convert to list
        # TODO: Automate this using mindsdb.type_infer
        if categorical_columns is not None and isinstance(categorical_columns, str):
            categorical_columns = ast.literal_eval(categorical_columns)
        if continuous_columns is not None and isinstance(continuous_columns, str):
            continuous_columns = ast.literal_eval(continuous_columns)

        print(categorical_columns)
        print(continuous_columns)
        dropout = float(args.get('drop_out', 0.0))
        epochs = int(args.get('epochs', 3))
        batch_size = args.get('batch_size', 32)
        layers = args.get('layers', '128-64-32')
        initialization = args.get('initialization', 'kaiming')

        # Create the data config
        data_config = DataConfig(
            target=[target],
            continuous_cols=continuous_columns,
            categorical_cols=categorical_columns,
            continuous_feature_transform=None,
            normalize_continuous_features=True,
        )
        head_config = LinearHeadConfig(
            layers=layers, dropout=dropout, initialization=initialization
            # No additional layer in head, just a mapping layer to output_dim
        ).__dict__
        model_config = CategoryEmbeddingModelConfig(
            task=args['task'],
            head_config=head_config
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
        self.model_storage.file_set('model', dill.dumps(tabular_model))

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        tabular_model = dill.load(self.model_storage.file_get('model'))
        predictions = tabular_model.predict(df)
        return predictions

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        des_dict = {key: args.get(key, default_value) for key, default_value in
                    [('epochs', 3), ('initialization', 'kaiming'), ('task', 'regression'), ('categorical_cols', []),
                     ('continuous_cols', [])]}
        df_describe = pd.DataFrame.from_dict(des_dict)
        return df_describe
