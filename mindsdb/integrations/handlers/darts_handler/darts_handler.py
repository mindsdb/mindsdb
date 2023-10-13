import pandas as pd
from typing import Optional, Dict
from darts import TimeSeries
from darts.models import ARIMA
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.response import HandlerResponse

class DartsModel(BaseMLEngine):
    
    name = "dartsmodel"

    def _init_(self, model_storage, engine_storage) -> None:
        super()._init_(model_storage, engine_storage)

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Train an ARIMA model and save it for later usage.

        Args:
            target (str): Name of the target column.
            df (pd.DataFrame): Input DataFrame containing the time series data.
            args (Dict): Additional arguments (if needed).
        """
        # Ensure df contains a datetime index
        df.set_index('timestamp', inplace=True)

        # Convert DataFrame to Darts TimeSeries
        time_series = TimeSeries.from_dataframe(df, time_col='timestamp', value_cols=[target])

        # Create and train ARIMA model
        arima_model = ARIMA(order=(1, 1, 1))  # Can adjust the order as needed
        arima_model.fit(time_series)

        # Save the trained model to model_storage
        self.model_storage.save_model('arima_model', arima_model)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> HandlerResponse:
        """
        Make predictions using the trained ARIMA model.

        Args:
            df (pd.DataFrame): Input DataFrame containing the time series data for prediction.
            args (Dict): Additional arguments (if needed).

        Returns:
            HandlerResponse: Predicted values in a DataFrame.
        """
        # Ensure df contains a datetime index
        df.set_index('timestamp', inplace=True)

        # Load the trained ARIMA model from model_storage
        arima_model = self.model_storage.load_model('arima_model')

        # Convert DataFrame to Darts TimeSeries
        time_series = TimeSeries.from_dataframe(df, time_col='timestamp')

        # Make predictions
        predictions = arima_model.predict(len(df))

        # Convert predictions to a DataFrame
        prediction_df = predictions.pd_dataframe()

        # Rename the prediction column to match the target column
        prediction_df = prediction_df.rename(columns={'0': 'target'})

        return HandlerResponse(data=prediction_df)

    # You can optionally implement update, describe, and create_engine methods based on your requirements

    def close(self):
        pass
