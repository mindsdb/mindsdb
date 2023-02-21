import os
import string
import random
import numpy as np
import pandas as pd
import dill
from mindsdb.integrations.libs.base import BaseMLEngine
from neuralforecast import NeuralForecast
from neuralforecast.models import NHITS

DEFAULT_FREQUENCY = "D"
DEFAULT_MODEL = NHITS
DEFAULT_MODEL_NAME = "NHITS"
DEFAULT_TRAIN_PERCENT = 90
DEFAULT_MAX_EPOCHS = 100


def infer_frequency(df, time_column, default=DEFAULT_FREQUENCY):
    try:  # infer frequency from time column
        inferred_freq = pd.infer_freq(df[time_column])
    except TypeError:
        inferred_freq = default
    return inferred_freq if inferred_freq is not None else default


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

        # You have to save neuralforecast into a different folder each time.
        # Make sure you call random before creating the model, as that fixes the random seed
        random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=24))
        model_args["model_folder"] = os.path.join("neuralforecast", random_string)

        sf_df = self._transform_to_statsforecast_df(df, model_args)

        train_set_end_date = np.percentile(sf_df["ds"], DEFAULT_TRAIN_PERCENT)
        train_df = sf_df[sf_df["ds"] <= train_set_end_date]
        test_df = sf_df[sf_df["ds"] > train_set_end_date]

        input_size = 2 * model_args["horizon"]
        model = DEFAULT_MODEL(model_args["horizon"], input_size, max_epochs=DEFAULT_MAX_EPOCHS)
        nf = NeuralForecast(models=[model], freq=model_args["frequency"])
        nf.fit(train_df)
        nf.save(model_args["model_folder"], overwrite=True)

        ###### persist changes to handler folder
        self.model_storage.json_set("model_args", model_args)
        self.model_storage.file_set("training_df", dill.dumps(sf_df))

    def predict(self, df, args={}):
        """Makes forecasts with the NeuralForecast Handler.

        NeuralForecast is setup to predict for all groups, so it won't handle
        a dataframe that's been filtered to one group very well. Instead, we make
        the prediction for all groups then take care of the filtering after the
        forecasting. Prediction is nearly instant.
        """
        # Load model arguments
        model_args = self.model_storage.json_get("model_args")

        prediction_df = self._transform_to_statsforecast_df(df, model_args)
        groups_to_keep = prediction_df["unique_id"].unique()

        nf = NeuralForecast.load(model_args["model_folder"])
        forecast_df = nf.predict(prediction_df)
        forecast_df = forecast_df[forecast_df.index.isin(groups_to_keep)]
        return self._get_results_from_statsforecast_df(forecast_df, model_args)

    def _transform_to_statsforecast_df(self, df, settings_dict):
        """Transform dataframes into the specific format required by NeuralForecast.

        NeuralForecast requires dataframes to have the following columns:
            unique_id -> the grouping column. If multiple groups are specified then
            we join them into one name using a | char.
            ds -> the date series
            y -> the target variable for prediction

        You can optionally include exogenous regressors after these three columns, but
        they must be numeric.
        """
        statsforecast_df = df.copy()
        # Transform group columns into single unique_id column
        if len(settings_dict["group_by"]) > 1:
            for col in settings_dict["group_by"]:
                statsforecast_df[col] = statsforecast_df[col].astype(str)
            statsforecast_df["unique_id"] = statsforecast_df[settings_dict["group_by"]].agg("|".join, axis=1)
            group_col = "ignore this"
        else:
            group_col = settings_dict["group_by"][0]

        # Rename columns to statsforecast names
        statsforecast_df = statsforecast_df.rename(
            {settings_dict["target"]: "y", settings_dict["order_by"]: "ds", group_col: "unique_id"}, axis=1
        )

        return statsforecast_df[["unique_id", "ds", "y"]]

    def _get_results_from_statsforecast_df(self, statsforecast_df, model_args):
        """Transform dataframes generated by StatsForecast back to their original format.

        This will return the dataframe to the original format supplied by the MindsDB query.
        """
        renaming_dict = {"ds": model_args["order_by"], DEFAULT_MODEL_NAME: model_args["target"]}
        return_df = statsforecast_df.reset_index().rename(renaming_dict, axis=1)
        if len(model_args["group_by"]) > 1:
            for i, group in enumerate(model_args["group_by"]):
                return_df[group] = return_df["unique_id"].apply(lambda x: x.split("|")[i])
        else:
            group_by_col = model_args["group_by"][0]
            return_df[group_by_col] = return_df["unique_id"]
        return return_df.drop(["unique_id"], axis=1)
