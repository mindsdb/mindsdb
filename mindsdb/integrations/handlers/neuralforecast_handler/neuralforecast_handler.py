import os
import tempfile
import string
import random
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.time_series_utils import (
    transform_to_nixtla_df,
    get_results_from_nixtla_df,
    infer_frequency,
)
from neuralforecast import NeuralForecast
from neuralforecast.models import NHITS
from neuralforecast.auto import AutoNHITS
from ray.tune.search.hyperopt import HyperOptSearch

DEFAULT_FREQUENCY = "D"
DEFAULT_MODEL_NAME = "NHITS"
DEFAULT_TRIALS = 20
MIN_TRIALS_FOR_AUTOTUNE = 3


def choose_model(num_trials, horizon, window, exog_vars=[], threshold=MIN_TRIALS_FOR_AUTOTUNE):
    """Chooses model based on the number of trials allowed by the user.

    A lower args for training time reduces the number of trial models we can
    search through. Below a certain threshold, we switch to the default
    NHITS implementation instead of using AutoML to search for the best config.
    """
    if num_trials >= threshold:
        model = AutoNHITS(horizon, gpus=0, num_samples=num_trials, search_alg=HyperOptSearch())
    else:  # faster implementation without auto parameter tuning
        model = NHITS(horizon, window, hist_exog_list=exog_vars)
    return model


class NeuralForecastHandler(BaseMLEngine):
    """Integration with the Nixtla NeuralForecast library for
    time series forecasting with neural networks.
    """

    name = "neuralforecast"

    def create(self, target, df, args={}):
        """Create the NeuralForecast Handler.

        Requires specifying the target column to predict and time series arguments for
        prediction horizon, time column (order by) and grouping column(s).

        Saves model params to desk, which are called later in the predict() method.
        """
        time_settings = args["timeseries_settings"]
        using_args = args["using"]
        assert time_settings["is_timeseries"], "Specify time series settings in your query"
        ###### store model args and time series settings in the model folder
        model_args = {}
        model_args["target"] = target
        model_args["horizon"] = time_settings["horizon"]
        model_args["order_by"] = time_settings["order_by"]
        model_args["group_by"] = time_settings["group_by"]
        model_args["frequency"] = (
            using_args["frequency"] if "frequency" in using_args else infer_frequency(df, time_settings["order_by"])
        )
        model_args["model_name"] = DEFAULT_MODEL_NAME
        num_trials = int(DEFAULT_TRIALS * using_args["train_time"]) if "train_time" in using_args else DEFAULT_TRIALS
        exog_vars = using_args["exogenous_vars"] if "exogenous_vars" in using_args else []
        model_args["model_folder"] = tempfile.mkdtemp()

        # Train model
        model = choose_model(num_trials, time_settings["horizon"], time_settings["window"])
        nixtla_df = transform_to_nixtla_df(df, model_args, exog_vars)
        nf = NeuralForecast(models=[model], freq=model_args["frequency"])
        nf.fit(nixtla_df)
        nf.save(model_args["model_folder"], overwrite=True)

        ###### persist changes to handler folder
        self.model_storage.json_set("model_args", model_args)

    def predict(self, df, args={}):
        """Makes forecasts with the NeuralForecast Handler.

        NeuralForecast is setup to predict for all groups, so it won't handle
        a dataframe that's been filtered to one group very well. Instead, we make
        the prediction for all groups then take care of the filtering after the
        forecasting. Prediction is nearly instant.
        """
        # Load model arguments
        model_args = self.model_storage.json_get("model_args")

        prediction_df = transform_to_nixtla_df(df, model_args)
        groups_to_keep = prediction_df["unique_id"].unique()

        nf = NeuralForecast.load(model_args["model_folder"])
        forecast_df = nf.predict(prediction_df)
        forecast_df = forecast_df[forecast_df.index.isin(groups_to_keep)]
        return get_results_from_nixtla_df(forecast_df, model_args)
