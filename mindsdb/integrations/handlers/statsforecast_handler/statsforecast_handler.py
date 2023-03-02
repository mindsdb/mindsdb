import dill
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.time_series_utils import (
    transform_to_nixtla_df,
    get_results_from_nixtla_df,
    infer_frequency,
)
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA, AutoCES, AutoETS, AutoTheta

DEFAULT_MODEL_NAME = "AutoARIMA"
model_dict = {
    "AutoARIMA": AutoARIMA,
    "AutoCES": AutoCES,
    "AutoETS": AutoETS,
    "AutoTheta": AutoTheta,
}


def choose_model(model_name, frequency):
    """Chooses which model to use in StatsForecast.

    We set a sensible default for seasonality based on the
    frequency parameter. For example: we assume monthly data
    has a season length of 12 (months in a year).

    If the inferred frequency isn't found, we default to 1 i.e.
    no seasonality.
    """
    model = model_dict[model_name]
    season_dict = {  # https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
        "H": 24,
        "M": 12,
        "Q": 4,
        "SM": 24,
        "BM": 12,
        "BMS": 12,
        "BQ": 4,
        "BH": 24,
        }
    new_freq = frequency.split("-")[0] if "-" in frequency else frequency  # shortens longer frequencies like Q-DEC
    season_length = season_dict[new_freq] if new_freq in season_dict else 1
    return model(season_length=season_length)


class StatsForecastHandler(BaseMLEngine):
    """Integration with the Nixtla StatsForecast library for
    time series forecasting with classical methods.
    """

    name = "statsforecast"

    def create(self, target, df, args={}):
        """Create the StatsForecast Handler.

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
        model_args["frequency"] = (
            using_args["frequency"] if "frequency" in using_args else infer_frequency(df, time_settings["order_by"])
        )

        model_args["model_name"] = DEFAULT_MODEL_NAME if "model_name" not in using_args else using_args["model_name"]
        training_df = transform_to_nixtla_df(df, model_args)
        model = choose_model(model_args["model_name"], model_args["frequency"])
        sf = StatsForecast(models=[model], freq=model_args["frequency"], df=training_df)
        fitted_models = sf.fit().fitted_

        ###### persist changes to handler folder
        self.model_storage.json_set("model_args", model_args)
        self.model_storage.file_set("training_df", dill.dumps(training_df))
        self.model_storage.file_set("fitted_models", dill.dumps(fitted_models))

    def predict(self, df, args={}):
        """Makes forecasts with the StatsForecast Handler.

        StatsForecast is setup to predict for all groups, so it won't handle
        a dataframe that's been filtered to one group very well. Instead, we make
        the prediction for all groups then take care of the filtering after the
        forecasting. Prediction is nearly instant.
        """
        # Load model arguments
        model_args = self.model_storage.json_get("model_args")
        training_df = dill.loads(self.model_storage.file_get("training_df"))
        fitted_models = dill.loads(self.model_storage.file_get("fitted_models"))

        prediction_df = transform_to_nixtla_df(df, model_args)
        groups_to_keep = prediction_df["unique_id"].unique()

        sf = StatsForecast(models=[], freq=model_args["frequency"], df=training_df)
        sf.fitted_ = fitted_models
        forecast_df = sf.predict(model_args["horizon"])
        forecast_df = forecast_df[forecast_df.index.isin(groups_to_keep)]
        return get_results_from_nixtla_df(forecast_df, model_args)
