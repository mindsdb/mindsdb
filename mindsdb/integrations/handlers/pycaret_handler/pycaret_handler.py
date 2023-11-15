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
        model = self._train_model(s, using)
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
        model_type = args['model_type']
        # copy setup kwargs
        kwargs = self._select_keys(args, "setup_")
        # return kwargs
        if model_type == 'classification' or model_type == 'regression' or model_type == 'time_series':
            return {**kwargs, 'target': target}
        elif model_type == 'clustering' or model_type == 'anomaly':
            return {**kwargs}
        raise Exception(f"Unrecognized model type '{model_type}'")

    def _predict_model(self, s, model, df, args):
        """Apply predictor arguments and get predictions"""
        model_type = args["model_type"]
        kwargs = self._select_keys(args, "predict_")
        if (
            model_type == 'classification'
            or model_type == 'regression'
            or model_type == 'clustering'
            or model_type == 'anomaly'
        ):
            kwargs["data"] = df
        elif model_type == 'time_series':
            # do nothing
            pass
        else:
            raise Exception(f"Unrecognized model type '{model_type}'")
        return s.predict_model(model, **kwargs)

    def _train_model(self, experiment, args):
        """Train the model and return the best (if applicable)"""
        model_type = args['model_type']
        model_name = args['model_name']
        kwargs = self._select_keys(args, "create_")
        if (
            model_type == 'classification'
            or model_type == 'regression'
            or model_type == 'time_series'
        ) and model_name == 'best':
            return experiment.compare_models(**kwargs)
        if model_name == 'best':
            raise Exception("Specific model name must be provided for clustering or anomaly tasks")
        return experiment.create_model(model_name, **kwargs)

    def _select_keys(self, d, prefix):
        """Selects keys with given prefix and returns a new dict"""
        result = {}
        for k in d:
            if k.startswith(prefix):
                result[k[len(prefix):]] = d[k]
        return result
