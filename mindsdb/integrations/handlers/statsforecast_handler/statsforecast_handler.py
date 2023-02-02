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
        sf = StatsForecast(models=[AutoARIMA()], freq=frequency)
        sf.fit(df)
        ###### store and persist in model folder
        model_args = sf.fitted_[0][0].model_
        model_args["frequency"] = frequency

        ###### persist changes to handler folder
        self.model_storage.file_set('model', dill.dumps(model_args))
    
    def predict(self, df, args):
        model_args = dill.loads(self.model_storage.file_get('model'))
        fitted_model = AutoARIMA()
        fitted_model.model_ = model_args
        sf = StatsForecast(models=[], freq=model_args["frequency"], df=df)
        sf.fitted_ = np.array([[fitted_model]])
        forecast_df = sf.predict(h=12)
        return forecast_df.reset_index(drop=True)