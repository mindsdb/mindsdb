from mindsdb.integrations.libs.base import BaseMLEngine

class StatsForecastHandler(BaseMLEngine):
    """
    Integration with the Nixtla StatsForecast library for
    time series forecasting with classical methods.
    """
    name = "statsforecast"


    def create(self):
        ###### store and persist in model folder
        self.model_storage.json_set('args', args)

        ###### persist changes to handler folder
        self.engine_storage.folder_sync(model_name)