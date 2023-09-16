from typing import Optional, Dict

import pandas as pd
from nixtlats import TimeGPT

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.integrations.utilities.time_series_utils import get_results_from_nixtla_df

# TODO: add E2E tests.

class TimeGPTHandler(BaseMLEngine):
    """
    Integration with the Nixtla TimeGPT models for
    zero-shot time series forecasting.
    """

    name = "timegpt"

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Create the TimeGPT Handler.
        Requires specifying the target column and usual time series arguments. Saves model config for later usage.
        """
        time_settings = args["timeseries_settings"]
        using_args = args["using"]

        assert time_settings["is_timeseries"], "Specify time series settings in your query"
        model_args = {
            'token': get_api_key('TIMEGPT_TOKEN', using_args, self.engine_storage, strict=True),
            "target": target,
            "horizon": time_settings["horizon"],
            "order_by": time_settings["order_by"],
            "group_by": time_settings.get("group_by", []),
            "freq": using_args.get("frequency", None),
            "finetune_steps": using_args.get("finetune_steps", 0),
            "validate_token": using_args.get("validate_token", False),
            "date_features": using_args.get("date_features", False),
            "date_features_to_one_hot": using_args.get("date_features_to_one_hot", True),
            "clean_ex_first": using_args.get("clean_ex_first", True),
            "level": using_args.get("level", [90])
        }
        assert isinstance(model_args["level"], list), "`level` must be a list of integers"
        assert all([isinstance(l, int) for l in model_args["level"]]), "`level` must be a list of integers"

        self.model_storage.json_set("model_args", model_args)  # persist changes to handler folder

    def predict(self, df, args={}):
        """ Makes forecasts with the TimeGPT API. """
        model_args = self.model_storage.json_get("model_args")
        prediction_df = self._transform_to_nixtla_df(df, model_args)
        timegpt = TimeGPT(token=model_args['token'])

        forecast_df = timegpt.forecast(
            prediction_df,

            # TODO: supporting param override when joining is blocked by mindsdb_sql#285
            h=args.get("horizon", model_args["horizon"]),
            freq=args.get("freq", model_args["freq"]),  # automatically infers correct frequency if not provided by user
            level=model_args["level"],
            finetune_steps=args.get('finetune_steps', model_args['finetune_steps']),
            validate_token=args.get('validate_token', model_args['validate_token']),
            date_features=args.get('date_features', model_args['date_features']),
            date_features_to_one_hot=args.get('date_features_to_one_hot', model_args['date_features_to_one_hot']),
            clean_ex_first=args.get('clean_ex_first', model_args['clean_ex_first']),

            # TODO: enable these post-refactor
            # X_df=None,  # exogenous variables
            # add_history=False,  # insample
        )
        results_df = forecast_df[['unique_id', 'ds', 'TimeGPT']]
        results_df = get_results_from_nixtla_df(results_df, model_args)
        results_df = results_df.rename({'TimeGPT': model_args['target']}, axis=1)

        # add prediction intervals
        levels = sorted(model_args['level'], reverse=True)
        for i, level in enumerate(levels):
            if i == 0:
                # NOTE: this should be simplified once we refactor the expected time series output within MindsDB
                results_df['confidence'] = level/100  # we report the highest level as the overall confidence
                results_df['lower'] = forecast_df[f'TimeGPT-lo-{level}']
                results_df['upper'] = forecast_df[f'TimeGPT-hi-{level}']
            else:
                results_df[f'lower_{level}'] = forecast_df[f'TimeGPT-lo-{level}']
                results_df[f'upper_{level}'] = forecast_df[f'TimeGPT-hi-{level}']

        return results_df

    def describe(self, attribute=None):
        model_args = self.model_storage.json_get("model_args")

        if attribute == "model":
            df = pd.DataFrame({"frequency": [model_args["freq"] if model_args["freq"] else "automatic"]})
            return df

        elif attribute == "features":
            df = pd.DataFrame({
                "order by": [model_args["order_by"]],
                "target": model_args["target"]
            })
            if model_args["group_by"]:
                df["group by"] = [model_args["group_by"]]
            return df

        elif attribute == 'info':
            outputs = model_args["target"]
            inputs = [model_args["target"], model_args["order_by"]]
            if model_args["group_by"]:
                inputs.append(model_args["group_by"])
            return pd.DataFrame({"output": outputs, "input": [inputs]})

        else:
            tables = ['info', 'features', 'model']
            return pd.DataFrame(tables, columns=['tables'])

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
        else: 
            df[date_column] = pd.to_datetime(df[date_column])
        df[date_column] = df[date_column].dt.strftime('%Y-%m-%dT%H:%M:%S')  # convert to ISO 8601 format
        return df

    # TODO: consolidate this method with the ones in time_series_utils.py
    def _transform_to_nixtla_df(self, df, settings_dict, exog_vars=[]):
        nixtla_df = df.copy()
        # Transform group columns into single unique_id column
        gby = settings_dict["group_by"]
        if len(gby) > 1:
            for col in gby:
                nixtla_df[col] = nixtla_df[col].astype(str)
            nixtla_df["unique_id"] = nixtla_df[gby].agg("/".join, axis=1)
            group_col = "ignore this"
        elif len(gby) == 1:
            group_col = settings_dict["group_by"][0]
        else:
            group_col = '__unique_id'
            nixtla_df[group_col] = '1'

        # Rename columns to statsforecast names
        nixtla_df = nixtla_df.rename(
            {settings_dict["target"]: "y", settings_dict["order_by"]: "ds", group_col: "unique_id"}, axis=1
        )

        columns_to_keep = ["unique_id", "ds", "y"] + exog_vars
        nixtla_df = self._convert_to_iso(nixtla_df, "ds")
        nixtla_df = nixtla_df[columns_to_keep].sort_values(by=['unique_id', 'ds'], ascending=True)  # expects ascending
        nixtla_df['y'] = nixtla_df['y'].astype(float)
        return nixtla_df.reset_index(drop=True)
