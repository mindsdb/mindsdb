from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import MachineLearningHandler
from mindsdb.utilities import log

import darts
from darts import TimeSeries
from darts.models import ARIMA
from darts.dataprocessing.transformers import Scaler

class DartsHandler(MachineLearningHandler):

    name = 'darts'

    def __init__(self, name: str, connection_data: dict, **kwargs):
        super().__init__(name)
        self.connection_data = connection_data
        self.model = None

    def train(self, data):
        try:
            # Assuming data is in the format: [timestamp, value]
            time_series = TimeSeries.from_dataframe(data, time_col=0, value_col=1)

            # Preprocess data (you can add more preprocessing steps)
            scaler = Scaler()
            time_series = scaler.fit_transform(time_series)

            # Initialize and fit an ARIMA model (you can use other Darts models)
            self.model = ARIMA()

            self.model.fit(time_series)

            log.logger.info("Darts model trained successfully")

        except Exception as e:
            log.logger.error(f"Error training Darts model: {str(e)}")

    def forecast(self, num_forecast_points):
        try:
            if self.model is not None:
                forecast = self.model.predict(num_forecast_points)
                # Extract forecasted values
                forecast_values = forecast.values()

                # Assuming forecast is a list of forecasted values
                return forecast_values
            else:
                log.logger.error("Darts model has not been trained")
                return []

        except Exception as e:
            log.logger.error(f"Error forecasting with Darts model: {str(e)}")

    def evaluate(self, data):
        # Implement evaluation logic here if needed
        pass
