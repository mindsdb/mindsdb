from merlion.models.anomaly.forecast_based.prophet import ProphetDetectorConfig, ProphetDetector
from merlion.models.anomaly.isolation_forest import IsolationForestConfig, IsolationForest
from merlion.models.anomaly.windstats import WindStatsConfig, WindStats
from merlion.models.automl.autoprophet import AutoProphetConfig, AutoProphet
from merlion.models.automl.autosarima import AutoSarimaConfig, AutoSarima
from merlion.models.forecast.smoother import MSESConfig, MSES
from merlion.post_process.threshold import AggregateAlarms
from merlion.transform.moving_average import DifferenceTransform
from scipy.stats import norm

from merlion.models.defaults import DefaultDetectorConfig, DefaultDetector, DefaultForecaster, DefaultForecasterConfig
from merlion.utils import TimeSeries

from enum import Enum
import pandas as pd


class MerlionArguments(Enum):
    target_seq_index = "target_seq_index"
    max_forecast_steps = "max_forecast_steps"
    max_backstep = "max_backstep"
    wind_sz = "wind_sz"
    alm_threshold = "alm_threshold"
    maxiter = "maxiter"


class BaseMerlionForecastAdapter:
    TARGET_SEQ_INDEX = 0
    DEFAULT_MAX_FORECAST_STEPS = 100
    DEFAULT_MAX_BACKSTEP = 60
    DEFAULT_MAXITER = 5

    def __init__(self, **kwargs):
        self.max_forecast_steps = kwargs.get(MerlionArguments.max_forecast_steps.value, self.DEFAULT_MAX_FORECAST_STEPS)
        self.max_backstep = kwargs.get(MerlionArguments.max_backstep.value, self.DEFAULT_MAX_BACKSTEP)
        self.maxiter = kwargs.get(MerlionArguments.maxiter.value, self.DEFAULT_MAXITER)
        self.model = None

    def to_bytes(self) -> bytes:
        return self.model.to_bytes()

    def initialize_model(self, bytes):
        self.model = self.model.from_bytes(bytes)

    def to_train_dataframe(self, df: pd.DataFrame, target: str) -> pd.DataFrame:
        columns = list(df.columns.values)
        columns.remove(target)
        columns.insert(self.TARGET_SEQ_INDEX, target)
        return df[columns]

    def train(self, df: pd.DataFrame, target: str):
        df = self.to_train_dataframe(df=df, target=target)
        train_data = TimeSeries.from_pd(df)
        self.model.train(train_data=train_data)

    def predict(self, df: pd.DataFrame, target: str) -> pd.DataFrame:
        forecast_step = self.max_forecast_steps
        df = df[df.index <= self.model.last_train_time + self.model.timedelta * forecast_step]
        if len(list(df.columns.values)) == 0:
            df.loc[:, target] = 0
        predict_data = TimeSeries.from_pd(df)
        predict_pred, predict_err = self.model.forecast(time_stamps=predict_data.time_stamps)
        return self.__prepare_forecast_return(target=target, pred_ts=predict_pred, err_ts=predict_err)

    def __prepare_forecast_return(self, target: str, pred_ts: TimeSeries, err_ts: TimeSeries) -> pd.DataFrame:
        pred_df: pd.DataFrame = pred_ts.to_pd()
        if err_ts is None:
            err_df = None
        else:
            err_df = err_ts.to_pd()

        if err_df is None or target in list(err_df.columns.values):  # error and predict sometimes are same
            std = pred_df[target].std()
            pred_df[f"{target}__upper"] = pred_df[f"{target}"] + std * norm.ppf(0.975)
            pred_df[f"{target}__lower"] = pred_df[f"{target}"] + std * norm.ppf(0.025)
        else:
            pred_df = pred_df.join(err_df, how="left")
            pred_df[f"{target}__upper"] = pred_df[f"{target}"] + norm.ppf(0.975) * err_df[f"{target}_err"]
            pred_df[f"{target}__lower"] = pred_df[f"{target}"] + norm.ppf(0.025) * err_df[f"{target}_err"]
            pred_df.drop(columns=[f"{target}_err"], inplace=True)
        return pred_df


class DefaultForecasterAdapter(BaseMerlionForecastAdapter):
    # DefaultForecaster
    # reference: https://opensource.salesforce.com/Merlion/latest/merlion.models.html#
    #               merlion.models.defaults.DefaultForecaster
    def __init__(self, **kwargs):
        super(DefaultForecasterAdapter, self).__init__(**kwargs)
        self.model = DefaultForecaster(DefaultForecasterConfig(max_forecast_steps=self.max_forecast_steps,
                                                               target_seq_index=self.TARGET_SEQ_INDEX))


class SarimaForecasterAdapter(BaseMerlionForecastAdapter):
    # AutoSarima
    # reference: https://opensource.salesforce.com/Merlion/latest/merlion.models.automl.html#
    #               module-merlion.models.automl.autosarima
    def __init__(self, **kwargs):
        super(SarimaForecasterAdapter, self).__init__(**kwargs)
        config = AutoSarimaConfig(auto_pqPQ=True, auto_d=True, auto_D=True, auto_seasonality=True,
                                  approximation=True, maxiter=5)
        self.model = AutoSarima(config)


class ProphetForecasterAdapter(BaseMerlionForecastAdapter):
    # AutoProphet
    # reference: https://opensource.salesforce.com/Merlion/latest/merlion.models.automl.html#
    #               module-merlion.models.automl.autoprophet
    def __init__(self, **kwargs):
        super(ProphetForecasterAdapter, self).__init__(**kwargs)
        self.model = AutoProphet(AutoProphetConfig(max_forecast_steps=self.max_forecast_steps))


class MSESForecasterAdapter(BaseMerlionForecastAdapter):
    # MSES
    # reference: https://opensource.salesforce.com/Merlion/latest/merlion.models.forecast.html#
    #               merlion.models.forecast.smoother.MSES
    def __init__(self, **kwargs):
        super(MSESForecasterAdapter, self).__init__(**kwargs)
        self.model = MSES(MSESConfig(max_forecast_steps=self.max_forecast_steps,
                                     target_seq_index=self.TARGET_SEQ_INDEX,
                                     max_backstep=self.max_backstep))


class BaseMerlineDetectorAdapter(BaseMerlionForecastAdapter):
    DEFAULT_WIND_SZ = 60
    DEFAULT_ALM_THRESHOLD = 4

    def __init__(self, **kwargs):
        super(BaseMerlineDetectorAdapter, self).__init__(**kwargs)
        self.wind_sz = kwargs.get(MerlionArguments.wind_sz.value, self.DEFAULT_WIND_SZ)
        self.alm_threshold = kwargs.get(MerlionArguments.alm_threshold.value, self.DEFAULT_ALM_THRESHOLD)

    def train(self, df: pd.DataFrame, target: str):
        df = self.to_train_dataframe(df=df, target=target)
        train_data = TimeSeries.from_pd(df[target])
        self.model.train(train_data=train_data)

    def predict(self, df: pd.DataFrame, target: str) -> pd.DataFrame:
        predict_data = TimeSeries.from_pd(df[target])
        predict_label = self.model.get_anomaly_label(time_series=predict_data)
        pred_df = predict_label.to_pd()
        pred_df[f"{target}__anomaly_score"] = pred_df["anom_score"]
        return pred_df


class DefaultDetectorAdapter(BaseMerlineDetectorAdapter):
    # DefaultDetector
    # reference: https://opensource.salesforce.com/Merlion/latest/merlion.models.html#
    #               merlion.models.defaults.DefaultDetector
    def __init__(self, **kwargs):
        super(DefaultDetectorAdapter, self).__init__(**kwargs)
        self.model = DefaultDetector(DefaultDetectorConfig())


class IsolationForestDetectorAdapter(BaseMerlineDetectorAdapter):
    # IsolationForest
    # reference: https://opensource.salesforce.com/Merlion/latest/merlion.models.anomaly.html#
    #               merlion.models.anomaly.isolation_forest.IsolationForest
    def __init__(self, **kwargs):
        super(IsolationForestDetectorAdapter, self).__init__(**kwargs)
        self.model = IsolationForest(IsolationForestConfig())


class WindStatsDetectorAdapter(BaseMerlineDetectorAdapter):
    # WindStats
    # reference: https://opensource.salesforce.com/Merlion/latest/merlion.models.anomaly.html#
    #               merlion.models.anomaly.windstats.WindStats
    def __init__(self, **kwargs):
        super(WindStatsDetectorAdapter, self).__init__(**kwargs)
        config = WindStatsConfig(wind_sz=self.wind_sz, threshold=AggregateAlarms(alm_threshold=self.alm_threshold))
        self.model = WindStats(config)


class ProphetDetectorAdapter(BaseMerlineDetectorAdapter):
    # ProphetDetector
    # reference: https://opensource.salesforce.com/Merlion/latest/merlion.models.anomaly.forecast_based.html#
    #               merlion.models.anomaly.forecast_based.prophet.ProphetDetector
    def __init__(self, **kwargs):
        super(ProphetDetectorAdapter, self).__init__(**kwargs)
        config = ProphetDetectorConfig(transform=DifferenceTransform())
        self.model = ProphetDetector(config)
