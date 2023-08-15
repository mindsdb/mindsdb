from typing import Optional, Dict

import pandas as pd
from nixtlats import TimeGPT

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.handler_utils import get_api_key

# TODO: add E2E tests.

class TimeGPTHandler(BaseMLEngine):
    """
    Integration with the Nixtla TimeGPT models for
    zero-shot time series forecasting.
    """

    name = "nixtla"

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Create the Nixtla Handler.
        Requires specifying the target column and usual time series arguments. Saves model config for later usage.
        """
        time_settings = args["timeseries_settings"]
        using_args = args["using"]
        assert time_settings["is_timeseries"], "Specify time series settings in your query"
        model_args = {}
        model_args['token'] = get_api_key('TIMEGPT_TOKEN', using_args, self.engine_storage, strict=True)
        model_args["target"] = target
        model_args["horizon"] = time_settings["horizon"]
        model_args["order_by"] = time_settings["order_by"]
        model_args["group_by"] = time_settings["group_by"]
        model_args["level"] = using_args.get("level", 90)
        model_args["frequency"] = (
            using_args["frequency"] if "frequency" in using_args else None
        )
        self.model_storage.json_set("model_args", model_args)  # persist changes to handler folder

    def predict(self, df, args={}):
        """ Makes forecasts with the TimeGPT API. """
        model_args = self.model_storage.json_get("model_args")
        prediction_df = self._transform_to_nixtla_df(df, model_args)
        timegpt = TimeGPT(token=model_args['token'])

        forecast_df = timegpt.forecast(
            prediction_df,
            h=model_args["horizon"],
            freq='T',  # TimeGPT automatically infers the correct frequency
            level=[model_args["level"]],
        )
        results_df = forecast_df[['unique_id', 'ds', 'TimeGPT']]
        results_df = self._get_results_from_nixtla_df(results_df, model_args)

        # add confidence
        level = model_args['level']
        results_df['confidence'] = level/100
        results_df['lower'] = forecast_df[f'TimeGPT-lo-{level}']
        results_df['upper'] = forecast_df[f'TimeGPT-hi-{level}']

        return results_df

    def describe(self, attribute=None):
        model_args = self.model_storage.json_get("model_args")

        if attribute == "model":
            return pd.DataFrame({k: [model_args[k]] for k in ["frequency"]})

        elif attribute == "features":
            return pd.DataFrame(
                {"ds": [model_args["order_by"]], "y": model_args["target"], "unique_id": [model_args["group_by"]]}
            )

        elif attribute == 'info':
            outputs = model_args["target"]
            inputs = [model_args["target"], model_args["order_by"], model_args["group_by"]]
            return pd.DataFrame({"outputs": outputs, "inputs": [inputs]})

        else:
            tables = ['info', 'features', 'model']
            return pd.DataFrame(tables, columns=['tables'])

    # TODO: consolidate this method with the ones in time_series_utils.py
    @staticmethod
    def _get_results_from_nixtla_df(nixtla_df, model_args):
        return_df = nixtla_df.reset_index(drop=True)
        return_df.columns = ["unique_id", "ds", model_args["target"]]
        if len(model_args["group_by"]) > 1:
            for i, group in enumerate(model_args["group_by"]):
                return_df[group] = return_df["unique_id"].apply(lambda x: x.split("/")[i])
        else:
            group_by_col = model_args["group_by"][0]
            return_df[group_by_col] = return_df["unique_id"]
        return_df['ds'] = pd.Series(return_df.ds, dtype=object)
        return return_df.drop(["unique_id"], axis=1).rename({"ds": model_args["order_by"]}, axis=1)

    # TODO: consolidate this method with the ones in time_series_utils.py
    @staticmethod
    def _convert_to_iso(df, date_column):
        # whether values in date_column are numeric (Unix timestamp) or string (date)
        if pd.api.types.is_numeric_dtype(df[date_column]):
            unit = ''
            # ascending unit order
            for u in ['ns', 'us', 'ms', 's']:
                mindate = pd.to_datetime(df[date_column].min(), unit=u, origin='unix')
                maxdate = pd.to_datetime(df[date_column].max(), unit=u, origin='unix')
                if mindate > pd.to_datetime('1970-01-01T00:00:00') and maxdate < pd.to_datetime('2050-12-31T23:59:59'):
                    unit = u
            df[date_column] = pd.to_datetime(df[date_column], unit=unit, origin='unix')
        elif pd.api.types.is_string_dtype(df[date_column]):
            df[date_column] = pd.to_datetime(df[date_column])
        df[date_column] = df[date_column].dt.strftime('%Y-%m-%dT%H:%M:%S')  # convert to ISO 8601 format
        return df

    # TODO: consolidate this method with the ones in time_series_utils.py
    def _transform_to_nixtla_df(self, df, settings_dict, exog_vars=[]):
        nixtla_df = df.copy()
        # Transform group columns into single unique_id column
        if len(settings_dict["group_by"]) > 1:
            for col in settings_dict["group_by"]:
                nixtla_df[col] = nixtla_df[col].astype(str)
            nixtla_df["unique_id"] = nixtla_df[settings_dict["group_by"]].agg("/".join, axis=1)
            group_col = "ignore this"
        else:
            group_col = settings_dict["group_by"][0]

        # Rename columns to statsforecast names
        nixtla_df = nixtla_df.rename(
            {settings_dict["target"]: "y", settings_dict["order_by"]: "ds", group_col: "unique_id"}, axis=1
        )

        columns_to_keep = ["unique_id", "ds", "y"] + exog_vars
        nixtla_df = self._convert_to_iso(nixtla_df, "ds")
        nixtla_df = nixtla_df[columns_to_keep].sort_values(by=['unique_id', 'ds'], ascending=True)  # expects ascending
        nixtla_df['y'] = nixtla_df['y'].astype(float)
        return nixtla_df.reset_index(drop=True)
