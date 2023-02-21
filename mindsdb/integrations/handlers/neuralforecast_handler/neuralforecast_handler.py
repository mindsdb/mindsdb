import os
import string
import random
import numpy as np
import pandas as pd
import dill
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.handlers.statsforecast_handler.statsforecast_handler import infer_frequency, transform_to_nixtla_df, get_results_from_nixtla_df
from neuralforecast import NeuralForecast
from neuralforecast.models import NHITS

DEFAULT_FREQUENCY = "D"
DEFAULT_MODEL = NHITS
DEFAULT_MODEL_NAME = "NHITS"
DEFAULT_TRAIN_PERCENT = 90
DEFAULT_MAX_EPOCHS = 100


class NeuralForecastHandler(BaseMLEngine):
    """Integration with the Nixtla NeuralForecast library for
    time series forecasting with classical methods.
    """

    name = "neuralforecast"

    def create(self, target, df, args={}):
        """Create the NeuralForecast Handler.

        Requires specifying the target column to predict and time series arguments for
        prediction horizon, time column (order by) and grouping column(s).

        Saves args, models params, and the formatted training df to disk. The training df
        is used later in the predict() method.
        """
        with open("args.txt", "w+") as f:
            f.write(str(args))
        time_settings = args["timeseries_settings"]
        using_args = args["using"]
        assert time_settings["is_timeseries"], "Specify time series settings in your query"
        ###### store model args and time series settings in the model folder
        model_args = {}
        model_args["target"] = target
        model_args["horizon"] = time_settings["horizon"]
        model_args["order_by"] = time_settings["order_by"]
        model_args["group_by"] = time_settings["group_by"]
        model_args["frequency"] =  using_args["frequency"] if "frequency" in using_args else infer_frequency(df, time_settings["order_by"])
        model_args["model_name"] =  DEFAULT_MODEL_NAME

        # You have to save neuralforecast into a different folder each time.
        # Make sure you call random before creating the model, as that fixes the random seed
        random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=24))
        model_args["model_folder"] = os.path.join("neuralforecast", random_string)

        nixtla_df = transform_to_nixtla_df(df, model_args)

        train_set_end_date = np.percentile(nixtla_df["ds"], DEFAULT_TRAIN_PERCENT)
        train_df = nixtla_df[nixtla_df["ds"] <= train_set_end_date]

        model = DEFAULT_MODEL(model_args["horizon"], time_settings["window"], max_epochs=DEFAULT_MAX_EPOCHS)
        nf = NeuralForecast(models=[model], freq=model_args["frequency"])
        nf.fit(train_df)
        nf.save(model_args["model_folder"], overwrite=True)

        ###### persist changes to handler folder
        self.model_storage.json_set("model_args", model_args)
        self.model_storage.file_set("training_df", dill.dumps(nixtla_df))

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
