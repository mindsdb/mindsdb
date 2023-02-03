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


    def create(self, target, df, args, frequency="Q"):
        time_settings = args["timeseries_settings"]
        assert time_settings["is_timeseries"], "Specify time series settings in your query"
        # Train model
        training_df = self._transform_to_sf_df(df, target, time_settings)
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

        prediction_df = self._transform_to_sf_df(df, model_args["target"], model_args)
        # StatsForecast won't handle extra columns - it assumes they're external regressors
        sf = StatsForecast(models=[AutoARIMA()], freq=model_args["frequency"], df=prediction_df)
        sf.fitted_ = np.array([[fitted_model]])
        forecast_df = sf.forecast(model_args["horizon"])
        return forecast_df.rename({"AutoARIMA": model_args["target"]}, axis=1).reset_index(drop=True)
    
    def _transform_to_sf_df(self, df, target, settings_dict):
        statsforecast_df = df.copy()
        
        if len(settings_dict["group_by"]) > 1:
            for col in settings_dict["group_by"]:
                statsforecast_df[col] = statsforecast_df[col].astype(str)
            statsforecast_df["unique_id"] = statsforecast_df[settings_dict["group_by"]].agg('|'.join, axis=1)
            group_col = "ignore this"
        else:
            group_col = settings_dict["group_by"][0]
        
        # set up df
        statsforecast_df = statsforecast_df.rename(
            {target: "y", settings_dict["order_by"]: "ds", group_col: "unique_id"},
            axis=1
            )

        return statsforecast_df[["unique_id", "ds", "y"]]
