import numpy as np
import dill
from mindsdb.integrations.libs.base import BaseMLEngine
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA

class StatsForecastHandler(BaseMLEngine):
    """
    Integration with the Nixtla StatsForecast library for
    time series forecasting with classical methods.
    """
    name = "statsforecast"


    def create(self, target, df, args, frequency="D"):
        time_settings = args["timeseries_settings"]
        print(time_settings)
        print(df.head())
        assert time_settings["is_timeseries"], "Specify time series settings in your query"
        training_df = df.copy()
        
        if len(time_settings["group_by"]) > 1:
            for col in time_settings["group_by"]:
                training_df[col] = training_df[col].astype(str)
            training_df["unique_id"] = training_df[time_settings["group_by"]].agg('|'.join, axis=1)
            group_col = "ignore this"
        else:
            group_col = time_settings["group_by"][0]
        
        # set up df
        training_df = training_df.rename(
            {target: "y", time_settings["order_by"]: "ds", group_col: "unique_id"},
            axis=1
            )

        training_df = training_df[["unique_id", "ds", "y"]]
        # Train model
        sf = StatsForecast(models=[AutoARIMA()], freq=frequency)
        sf.fit(training_df)

        ###### store model args and time series settings in the model folder
        model_args = sf.fitted_[0][0].model_
        model_args["target"] = target
        model_args["frequency"] = frequency
        model_args["horizon"] = time_settings["horizon"]
        model_args["order_by"] = time_settings["order_by"]
        model_args["group_by"] = time_settings["group_by"]

        ###### persist changes to handler folder
        self.model_storage.file_set('model', dill.dumps(model_args))
    
    def predict(self, df, args):
        # Load fitted model
        model_args = dill.loads(self.model_storage.file_get('model'))
        fitted_model = AutoARIMA()
        fitted_model.model_ = model_args

        prediction_df = df.rename(
            {model_args["target"]: "y", model_args["order_by"]: "ds", model_args["group_by"][0]: "unique_id"},
            axis=1
            )
        # StatsForecast won't handle extra columns - it assumes they're external regressors
        prediction_df = prediction_df[["unique_id", "ds", "y"]]
        sf = StatsForecast(models=[AutoARIMA()], freq="D", df=prediction_df)
        sf.fitted_ = np.array([[fitted_model]])
        forecast_df = sf.forecast(model_args["horizon"])
        return forecast_df.rename({"AutoARIMA": model_args["target"]}, axis=1).reset_index(drop=True)