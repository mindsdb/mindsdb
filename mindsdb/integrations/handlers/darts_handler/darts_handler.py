import pandas as pd
from typing import Optional, Dict
from darts import TimeSeries
from darts.models import AutoARIMA
from mindsdb.integrations.libs.base import BaseMLEngine

class DartsModel(BaseMLEngine):

    name = "dartsmodel"

    def __init__(self, model_storage, engine_storage) -> None:
        super().__init__(model_storage, engine_storage)

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Train an AutoARIMA model and save it for later usage.

        Args:
            target (str): Name of the target column.
            df (pd.DataFrame): Input DataFrame containing the time series data.
            args (Dict): Additional arguments (if needed).
        """

        # Find the timestamp column in the DataFrame
        timestamp_column = None
        for col in df.columns:
            if pd.api.types.is_datetime_dtype(df[col]):
                timestamp_column = col
                break

        if timestamp_column is None:
            raise Exception('No datetime column found in the DataFrame.')

        # Set the timestamp column as the index
        df.set_index(timestamp_column, inplace=True)

        # Convert DataFrame to Darts TimeSeries
        time_series = TimeSeries.from_dataframe(df, time_col=timestamp_column, value_cols=[target])

        auto_arima_model = AutoARIMA()  # AutoARIMA automatically finds the best ARIMA model
        auto_arima_model.fit(time_series)

        # Save the trained model to model_storage
        self.model_storage.save_model('auto_arima_model', auto_arima_model)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Make predictions using the trained AutoARIMA model.

        Args:
            df (pd.DataFrame): Input DataFrame containing the time series data for prediction.
            args (Dict): Additional arguments (if needed).

        Returns:
            pd.DataFrame: Predicted values in a DataFrame.
        """

        # Find the timestamp column in the DataFrame
        timestamp_column = None
        for col in df.columns:
            if pd.api.types.is_datetime_dtype(df[col]):
                timestamp_column = col
                break

        if timestamp_column is None:
            raise Exception('No datetime column found in the DataFrame.')

        # Set the timestamp column as the index
        df.set_index(timestamp_column, inplace=True)

        # Load the trained AutoARIMA model from model_storage
        auto_arima_model = self.model_storage.load_model('auto_arima_model')

        # Convert DataFrame to Darts TimeSeries for future extensions
        time_series = TimeSeries.from_dataframe(df, time_col=timestamp_column)

        # Make predictions
        predictions = auto_arima_model.predict(len(df))

        # Convert predictions to a DataFrame
        prediction_df = predictions.pd_dataframe()

        # Rename the prediction column to match the target column
        prediction_df = prediction_df.rename(columns={predictions.name: 'target'})

        return prediction_df
