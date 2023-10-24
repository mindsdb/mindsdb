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
            args (Dict): Additional arguments (if needed), including 'timeseries_settings'.
        """

        time_settings = args["timeseries_settings"]
        using_args = args["using"]
        assert time_settings["is_timeseries"], "Specify time series settings in your query"

        # Get the 'order_by' column directly from the parsed information
        order_by = time_settings["order_by"]

        # Convert the DataFrame to a Darts TimeSeries using 'order_by' for ordering
        training_series = TimeSeries.from_dataframe(df, time_col=None, value_cols=[target], date_col=order_by)

        # Create an AutoARIMA model
        auto_arima_model = AutoARIMA()  # AutoARIMA automatically finds the best ARIMA model
        auto_arima_model.fit(training_series)

        # Save the trained model to model_storage
        self.model_storage.save_model('auto_arima_model', auto_arima_model)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Make predictions using the trained AutoARIMA model.

        Args:
            df (pd.DataFrame): Input DataFrame containing the time series data for prediction.
            args (Dict): Additional arguments (if needed), including 'timeseries_settings'.

        Returns:
            pd.DataFrame: Predicted values in a DataFrame.
        """

        time_settings = args["timeseries_settings"]
        using_args = args["using"]
        assert time_settings["is_timeseries"], "Specify time series settings in your query"

        # Get the 'order_by' column directly from the parsed information
        order_by = time_settings["order_by"]

        # Convert the DataFrame to a Darts TimeSeries using 'order_by' for ordering
        prediction_series = TimeSeries.from_dataframe(df, time_col=None, value_cols=[], date_col=order_by)

        # Load the trained AutoARIMA model from model_storage
        auto_arima_model = self.model_storage.load_model('auto_arima_model')

        # Make predictions
        predictions = auto_arima_model.predict(len(df))

        # Convert predictions to a DataFrame
        prediction_df = predictions.pd_dataframe()

        # Rename the prediction column to match the target column
        prediction_df = prediction_df.rename(columns={predictions.name: 'target'})

        return prediction_df
