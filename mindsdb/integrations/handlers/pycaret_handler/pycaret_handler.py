from typing import Optional, Dict
import os

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from pycaret.classification import ClassificationExperiment
from pycaret.regression import RegressionExperiment
from pycaret.time_series import TSForecastingExperiment
from pycaret.clustering import ClusteringExperiment
from pycaret.anomaly import AnomalyExperiment


class PyCaretHandler(BaseMLEngine):
    name = 'pycaret'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # figure out the location where the model will be stored

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """Create and train model on given data"""
        # parse args
        if 'using' not in args:
            raise Exception("PyCaret engine requires a USING clause! Refer to its documentation for more details.")
        using = args['using']
        if df is None:
            raise Exception("PyCaret engine requires a some data to initialize!")
        # create experiment
        s = self._get_experiment(using['model_type'])
        s.setup(df, **self._get_experiment_setup_kwargs(using, args['target']))
        # train model
        model = self._train_model(using['model_type'], using['model_name'], s)
        # save model and args
        model_file_path = os.path.join(self.model_storage.fileStorage.folder_path, 'model')
        s.save_model(model, model_file_path)
        self.model_storage.json_set('saved_args', {
            **args['using'],
            'model_path': model_file_path
        })

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> pd.DataFrame:
        """Predict on the given data"""
        # load model
        saved_args = self.model_storage.json_get('saved_args')
        s = self._get_experiment(saved_args['model_type'])
        model = s.load_model(saved_args['model_path'])
        # predict and return
        return self._predict_model(s, model, df, saved_args)

    def _get_experiment(self, model_type):
        """Returns one of the types of experiments in PyCaret"""
        if model_type == "classification":
            return ClassificationExperiment()
        elif model_type == "regression":
            return RegressionExperiment()
        elif model_type == "time_series":
            return TSForecastingExperiment()
        elif model_type == "clustering":
            return ClusteringExperiment()
        elif model_type == "anomaly":
            return AnomalyExperiment()
        else:
            raise Exception(f"Unrecognized model type '{model_type}'")

    def _get_experiment_setup_kwargs(self, args: Dict, target: str):
        """Returns the arguments that need to passed in setup function for the experiment"""
        # TODO: support more kwargs
        model_type = args['model_type']
        kwargs = {
            'session_id': args['session_id'],
            'system_log': False
        }
        if model_type == 'classification':
            return {**kwargs, 'target': target}
        elif model_type == 'regression':
            return {**kwargs, 'target': target}
        elif model_type == 'time_series':
            return {**kwargs}
        elif model_type == 'clustering':
            return {**kwargs}
        elif model_type == 'anomaly':
            return {**kwargs}
        else:
            raise Exception(f"Unrecognized model type '{model_type}'")

    def _predict_model(self, s, model, df, saved_args):
        """Apply predictor arguments and get predictions"""
        model_type = saved_args["model_type"]
        kwargs = {}
        if model_type == 'classification':
            kwargs = self._select_keys_if_exist(saved_args, [
                "probability_threshold",
                "encoded_labels",
                "raw_score",
            ], "predict_")
            kwargs["data"] = df
        elif model_type == 'regression':
            kwargs["data"] = df
        elif model_type == 'time_series':
            kwargs = self._select_keys_if_exist(saved_args, [
                "fh",
                "X",
                "return_pred_int",
                "alpha",
                "coverage",
            ], "predict_")
        elif model_type == 'clustering':
            kwargs["data"] = df
        elif model_type == 'anomaly':
            kwargs["data"] = df
        else:
            raise Exception(f"Unrecognized model type '{model_type}'")
        return s.predict_model(model, **kwargs)

    def _train_model(self, model_type: str, model_name: str, experiment):
        """Train the model and return the best (if applicable)"""
        if (
            model_type == 'classification'
            or model_type == 'regression'
            or model_type == 'time_series'
        ) and model_name == 'best':
            # TODO: compare models take various params
            return experiment.compare_models()
        if model_name == 'best':
            raise Exception("Specific model name must be provided for clustering or anomaly tasks")
        # TODO: do we need assign_model for clustering and anomaly
        return experiment.create_model(model_name)

    def _select_keys_if_exist(self, d, keys, prefix):
        """Copies selected keys having a specified prefix to a new dict without the prefix"""
        result = {}
        for k in keys:
            if prefix + k in d:
                result[k] = d[prefix + k]
        return result
