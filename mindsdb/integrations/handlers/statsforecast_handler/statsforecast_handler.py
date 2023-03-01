import pandas as pd
import dill
from mindsdb.integrations.libs.base import BaseMLEngine
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA, AutoCES, AutoETS, AutoTheta

DEFAULT_FREQUENCY = "D"
DEFAULT_MODEL_NAME = "AutoARIMA"
model_dict = {
    "AutoARIMA": AutoARIMA,
    "AutoCES": AutoCES,
    "AutoETS": AutoETS,
    "AutoTheta": AutoTheta,
}


def infer_frequency(df, time_column, default=DEFAULT_FREQUENCY):
    """Infers frequency from the time column of the dataframe.

    If we can't infer frequency, will default to daily to prevent
    pipeline failure.
    """
    try:  # infer frequency from time column
        date_series = pd.to_datetime(df[time_column]).unique()
        date_series.sort()
        inferred_freq = pd.infer_freq(date_series)
    except TypeError:
        inferred_freq = default
    return inferred_freq if inferred_freq is not None else default


def choose_model(model_name, frequency):
    """Chooses which model to use in StatsForecast.

    We set a sensible default for seasonality based on the
    frequency parameter. For example: we assume monthly data
    has a season length of 12 (months in a year).

    If the inferred frequency isn't found, we default to 1 i.e.
    no seasonality.
    """
    model = model_dict[model_name]
    season_dict = {
        "H": 24,
        "M": 12,
        "Q": 4,
        "A": 1
        }
    new_freq = frequency[:1]  # shortens longer frequencies like Q-DEC
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
        model_args["frequency"] =  infer_frequency(df, time_settings["order_by"])
        model_args["frequency"] = (
            using_args["frequency"] if "frequency" in using_args else infer_frequency(df, time_settings["order_by"])
        )

        model_args["model_name"] = DEFAULT_MODEL_NAME if "model_name" not in using_args else using_args["model_name"]
        training_df = self._transform_to_statsforecast_df(df, model_args)
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

        prediction_df = self._transform_to_statsforecast_df(df, model_args)
        groups_to_keep = prediction_df["unique_id"].unique()

        sf = StatsForecast(models=[], freq=model_args["frequency"], df=training_df)
        sf.fitted_ = fitted_models
        forecast_df = sf.predict(model_args["horizon"])
        forecast_df = forecast_df[forecast_df.index.isin(groups_to_keep)]
        return self._get_results_from_statsforecast_df(forecast_df, model_args)

    def _transform_to_statsforecast_df(self, df, settings_dict):
        """Transform dataframes into the specific format required by StatsForecast.

        StatsForecast requires dataframes to have the following columns:
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
        return_df = statsforecast_df.reset_index()
        return_df.columns = ["unique_id", "ds", model_args["target"]]
        if len(model_args["group_by"]) > 1:
            for i, group in enumerate(model_args["group_by"]):
                return_df[group] = return_df["unique_id"].apply(lambda x: x.split("|")[i])
        else:
            group_by_col = model_args["group_by"][0]
            return_df[group_by_col] = return_df["unique_id"]
        return return_df.drop(["unique_id"], axis=1).rename({"ds": model_args["order_by"]}, axis=1)
