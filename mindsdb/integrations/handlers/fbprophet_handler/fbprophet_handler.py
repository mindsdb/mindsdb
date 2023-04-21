import dill
import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.time_series_utils import (
    get_results_from_nixtla_df,
    infer_frequency,
    transform_to_nixtla_df,
)


class FBProphetHandler(BaseMLEngine):
    """Integration with the FBProphet library for
    time series forecasting,
    """

    name = "fbprophet"

    def _fit_for_group(self, df, group, model_args):
        """
        Fit a fbprophet model for a group
        """

        # subset the data for the group
        group_df = df[df["unique_id"] == group]
        # drop the unique_id column
        group_df = group_df.drop("unique_id", axis=1)

        # fit the model
        model = Prophet()
        model.fit(group_df)

        return model

    def create(self, target, df, args={}):
        """Create the FBProphet Handler.

        Requires specifying the target column to predict and time series arguments for
        prediction horizon, time column (order by) and grouping column(s).

        Saves args, models params, and the formatted training df to disk. The training df
        is used later in the predict() method.
        """
        time_settings = args["timeseries_settings"]
        using_args = args["using"]
        assert time_settings[
            "is_timeseries"
        ], "Specify time series settings in your query"
        ###### store model args and time series settings in the model folder
        model_args = {}
        model_args["target"] = target
        model_args["horizon"] = time_settings["horizon"]
        model_args["order_by"] = time_settings["order_by"]
        model_args["group_by"] = time_settings["group_by"]
        model_args["frequency"] = (
            using_args["frequency"]
            if "frequency" in using_args
            else infer_frequency(df, time_settings["order_by"])
        )
        # trainig_df contains 3 columns -- unique_id, ds, y
        training_df = transform_to_nixtla_df(df, model_args)

        # results_df = get_insample_cv_results(model_args, training_df)
        # model_args["accuracies"] = get_model_accuracy_dict(results_df, r2_score)
        # model = choose_model(model_args, results_df)
        # sf = StatsForecast([model], freq=model_args["frequency"], df=training_df)
        # fitted_models = sf.fit().fitted_

        # fitting the model
        # since fbprophet can only handle one group at a time, we need to fit a model for each group
        # and store the fitted models in a dictionary
        fitted_models = {}
        for group in training_df["unique_id"].unique():
            fitted_models[group] = self._fit_for_group(training_df, group, model_args)
            # TODO: to support cross validation. This is very expensive for now.

        ###### persist changes to handler folder
        self.model_storage.json_set("model_args", model_args)

        # persist the model
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
        fitted_models = dill.loads(self.model_storage.file_get("fitted_models"))

        # predict_df -- unique_id, ds
        prediction_df = transform_to_nixtla_df(df, model_args)
        # make future dataframe
        groups_to_keep = prediction_df["unique_id"].unique()

        # for each group, make a prediction
        results = {}
        for group in groups_to_keep:
            # make future dataframe
            model = fitted_models[group]
            future_df = model.make_future_dataframe(
                periods=model_args["horizon"],
                freq=model_args["frequency"],
                include_history=False,
            )
            # make a prediction
            forecast = model.predict(future_df)
            # add the unique_id column back
            forecast["unique_id"] = group
            columns_to_keep = ["unique_id", "ds", "yhat"]
            forecast = forecast[columns_to_keep]
            # add the prediction to the results dictionary
            results[group] = forecast

        # combine the predictions into a single dataframe
        results_df = pd.concat(results.values(), axis=0).set_index("unique_id")

        result = get_results_from_nixtla_df(results_df, model_args)
        return result

    def describe(self, attribute=None):
        # TODO
        pass
