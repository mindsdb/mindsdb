from enum import Enum
import json
from typing import Optional, Dict

import numpy as np
import pandas as pd
import copy

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log
from .adapters import BaseMerlionForecastAdapter, DefaultForecasterAdapter, MerlionArguments, DefaultDetectorAdapter, \
    SarimaForecasterAdapter, ProphetForecasterAdapter, MSESForecasterAdapter, IsolationForestDetectorAdapter, \
    WindStatsDetectorAdapter, ProphetDetectorAdapter


class DetectorModelType(Enum):
    default = DefaultDetectorAdapter
    isolation = IsolationForestDetectorAdapter
    windstats = WindStatsDetectorAdapter
    prophet = ProphetDetectorAdapter


class ForecastModelType(Enum):
    default = DefaultForecasterAdapter
    sarima = SarimaForecasterAdapter
    prophet = ProphetForecasterAdapter
    mses = MSESForecasterAdapter


class TaskType(Enum):
    detector = DetectorModelType
    forecast = ForecastModelType


def is_invalid_type(name: str, type_class: Enum) -> bool:
    if name is None:
        return True
    return not name in type_class._member_names_


def enum_to_str(type_class: Enum) -> str:
    all = []
    for element in type_class:
        all.append(element.name)
    return "|".join(all)


def to_ts_dataframe(df: pd.DataFrame, time_col=None) -> (pd.DataFrame, str):
    columns = list(df.columns.values)
    # if time column has been specified, check the specified time column
    if time_col is not None:
        if time_col not in columns:
            raise Exception("invalid column name: " + time_col)
        if df[time_col].dtype != np.datetime64:
            try:
                idx = pd.to_datetime(df[time_col])
            except Exception as e:
                raise Exception("can not convert column to datetime: " + time_col + " " + str(e))
    # if time column has not been specified, try to find one
    else:
        datetime_cols = list(df.select_dtypes(include=["datetime"]).columns.values)
        if len(datetime_cols) > 0:
            time_col = datetime_cols[0]
            idx = pd.to_datetime(df[time_col])
        else:
            raise Exception("can not find datetime column for time series")
    # build return dataframe
    rt_df = copy.deepcopy(df)
    rt_df.drop(columns=[time_col], inplace=True)
    rt_df.index = idx
    return rt_df, time_col


class MerlionHandler(BaseMLEngine):
    name = 'merlion'

    ARG_USING_TASK = "task"
    ARG_USING_MODEL_TYPE = "model_type"
    # keys only be used to persist args to args.json
    ARG_TIME_COLUMN = "time_column"
    ARG_BASE_WINDOW = "base_window"
    ARG_PREDICT_HORIZON = "predict_horizon"
    ARG_TARGET = "target"
    ARG_COLUMN_SEQUENCE = "column_sequence"

    KWARGS_DF = "df"

    DEFAULT_MODEL_TYPE = "default"
    DEFAULT_MAX_PREDICT_STEP = 100
    DEFAULT_PREDICT_BASE_WINDOW = 10

    PERSIST_MODEL_FILE_NAME = "merlion_model"
    PERSIST_ARGS_KEY_IN_JSON_STORAGE = "args"

    def create(self, target, args=None, **kwargs):
        df: pd.DataFrame = kwargs.get(self.KWARGS_DF, None)
        # prepare arguments
        using_args = args.get("using", dict())
        task = using_args.get(self.ARG_USING_TASK, TaskType.forecast.name)
        model_type = using_args.get(self.ARG_USING_MODEL_TYPE, self.DEFAULT_MODEL_TYPE)
        timeseries_settings = args.get("timeseries_settings", dict())
        time_column = timeseries_settings.get("order_by", None)
        horizon = timeseries_settings.get("horizon", self.DEFAULT_MAX_PREDICT_STEP)
        window = timeseries_settings.get("window", self.DEFAULT_PREDICT_BASE_WINDOW)
        # update args for default value maybe has been used, only time column will be set afterwards
        serialize_args = dict()
        serialize_args[self.ARG_TARGET] = target
        serialize_args[self.ARG_USING_TASK] = task
        serialize_args[self.ARG_USING_MODEL_TYPE] = model_type
        serialize_args[self.ARG_PREDICT_HORIZON] = horizon
        serialize_args[self.ARG_BASE_WINDOW] = window

        # check df
        if df is None:
            raise Exception("missing required key in args: " + self.KWARGS_DF)
        else:
            column_sequence = sorted(list(df.columns.values))
            df = df[column_sequence]
            serialize_args[self.ARG_COLUMN_SEQUENCE] = column_sequence

        # check task, model_type and get the adapter_class
        adapter_class = self.__args_to_adapter_class(task=task, model_type=model_type)
        task_enum = TaskType[task]

        # check and cast to ts dataframe
        ts_df, time_column = to_ts_dataframe(df=df, time_col=time_column)
        serialize_args[self.ARG_TIME_COLUMN] = time_column

        # train model
        model_args = {}
        if task_enum == TaskType.forecast:
            model_args[MerlionArguments.max_forecast_steps.value] = horizon
        adapter: BaseMerlionForecastAdapter = adapter_class(**model_args)
        log.logger.info("Training model, args: " + json.dumps(serialize_args))
        adapter.train(df=ts_df, target=target)
        log.logger.info("Training model completed.")

        # persist save model
        model_bytes = adapter.to_bytes()
        self.model_storage.file_set(self.PERSIST_MODEL_FILE_NAME, model_bytes)
        self.model_storage.json_set(self.PERSIST_ARGS_KEY_IN_JSON_STORAGE, serialize_args)
        log.logger.info("Model and args saved.")

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        rt_df = df.copy(deep=True)
        # read model and args from storage
        model_bytes = self.model_storage.file_get(self.PERSIST_MODEL_FILE_NAME)
        deserialize_args = self.model_storage.json_get(self.PERSIST_ARGS_KEY_IN_JSON_STORAGE)

        # resolve args
        task = deserialize_args[self.ARG_USING_TASK]
        model_type = deserialize_args[self.ARG_USING_MODEL_TYPE]
        time_column = deserialize_args[self.ARG_TIME_COLUMN]
        target = deserialize_args[self.ARG_TARGET]
        horizon = deserialize_args[self.ARG_PREDICT_HORIZON]
        feature_column_sequence = list(deserialize_args[self.ARG_COLUMN_SEQUENCE])
        task_enum = TaskType[task]

        # check df and prepare data
        if task_enum == TaskType.forecast:
            feature_column_sequence.remove(target)
        missing_required_columns = set(feature_column_sequence) - set(rt_df.columns.values)
        if len(missing_required_columns) > 0:
            raise Exception("Missing required columns: " + ",".join(missing_required_columns))
        feature_df = rt_df[feature_column_sequence]
        ts_feature_df, _ = to_ts_dataframe(df=feature_df, time_col=time_column)

        # init model adapter
        adapter_class: BaseMerlionForecastAdapter = self.__args_to_adapter_class(task=task, model_type=model_type)
        model_args = {}
        if task_enum == TaskType.forecast:
            model_args[MerlionArguments.max_forecast_steps.value] = horizon
        adapter = adapter_class(**model_args)
        adapter.initialize_model(bytes=model_bytes)

        # predict
        pred_df = adapter.predict(df=ts_feature_df, target=target)

        # build result
        pred_df = ts_feature_df[[]].join(pred_df, how="left")

        # arrange data
        pred_df.index = rt_df.index
        if task_enum == TaskType.forecast:
            pred_df = pred_df[~pred_df[target].isna()]
            rt_df.drop(columns=[target], inplace=True)
        elif task_enum == TaskType.detector:
            pred_df[f"{target}__anomaly_score"].fillna(0, inplace=True)
        rt_df = rt_df.join(pred_df, how="right")
        return rt_df

    def __args_to_adapter_class(self, task: str, model_type: str):
        # check task_type
        try:
            task_enum = TaskType[task]
        except Exception as e:
            raise Exception("wrong using.task: " + task + ", valid options: " + enum_to_str(TaskType))
        # check and get model class
        try:
            adapter_class = task_enum.value[model_type].value
        except Exception as e:
            raise Exception("Wrong using.model_type: " + model_type + ", valid options: " +
                            enum_to_str(task_enum.value) + ", " + str(e))
        return adapter_class
