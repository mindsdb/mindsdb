from enum import Enum
import json
import numpy as np
import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
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


def to_ts_dataframe(df: pd.DataFrame, time_col=None) -> str:
    columns = list(df.columns.values)
    if time_col is not None :
        if time_col not in columns:
            raise Exception("invalid column name: " + time_col)
        if df[time_col].dtype != np.datetime64:
            try:
                df.index = pd.to_datetime(df[time_col])
            except Exception as e:
                raise Exception("can not convert column to datetime: " + time_col + " " + str(e))
    else:
        datetime_cols = list(df.select_dtypes(include=["datetime"]).columns.values)
        if len(datetime_cols) > 0:
            time_col = datetime_cols[0]
            df.index = pd.to_datetime(df[time_col])
        if time_col is None:
            raise Exception("can not find datetime column for time series")
    df.drop(columns=[time_col], inplace=True)
    return time_col


# remove ` from column, the result is (mapping, back_mapping)
# this function is a little strange, but I think it's important to keep
# the raw column names, maybe ` is a trick while parsing sql? maybe ...
def get_column_name_format_mapping(df: pd.DataFrame) -> (dict, dict):
    column_names = list(df.columns.values)
    mapping = {}
    back_mapping = {}
    for column_name in column_names:
        if column_name.find("`") == -1:
            continue
        formatted_name = column_name.replace("`", "")
        mapping[column_name] = formatted_name
        back_mapping[formatted_name] = column_name
    return mapping, back_mapping


class MerlionHandler(BaseMLEngine):
    name = 'merlion'

    ARG_USING_TASK = "task"
    ARG_USING_MODEL_TYPE = "model_type"
    ARG_USING_TIME_COLUMN = "time_column"
    ARG_HORIZON = "horizon"
    ARG_WINDOW = "window"
    ARG_TARGET = "target" # only be used to persist args to args.json
    ARG_COLUMN_SEQUENCE = "column_sequence"  # only be used to persist args to args.json

    KWARGS_DF = "df"

    DEFAULT_MODEL_TYPE = "default"
    DEFAULT_MAX_PREDICT_STEP = 100
    DEFAULT_PREDICT_BASE_WINDOW = 10

    PERSISIT_MODEL_FILE_NAME = "merlion_model"
    PERSISIT_ARGS_KEY_IN_JSON_STORAGE = "args"

    def create(self, target, args=None, **kwargs):
        df: pd.DataFrame = kwargs.get(self.KWARGS_DF, None)
        column_name_mapping, column_name_back_mapping = get_column_name_format_mapping(df)
        df.rename(columns=column_name_mapping, inplace=True)

        # prepare arguments
        task = args.get(self.ARG_USING_TASK, TaskType.forecast.name)
        model_type = args.get(self.ARG_USING_MODEL_TYPE, self.DEFAULT_MODEL_TYPE)
        time_column = args.get(self.ARG_USING_TIME_COLUMN, None)
        horizon = args.get(self.ARG_HORIZON, self.DEFAULT_MAX_PREDICT_STEP)
        window = args.get(self.ARG_WINDOW, self.DEFAULT_PREDICT_BASE_WINDOW)
        # update args for default value maybe has been used, only time column will be set afterwards
        args[self.ARG_TARGET] = target
        args[self.ARG_USING_TASK] = task
        args[self.ARG_USING_MODEL_TYPE] = model_type
        args[self.ARG_HORIZON] = horizon
        args[self.ARG_WINDOW] = window

        # check df
        if df is None:
            raise Exception("missing required key in args: " + self.KWARGS_DF)
        else:
            column_sequence = list(df.columns.values)
            sorted(column_sequence)
            df = df[column_sequence]
            args[self.ARG_COLUMN_SEQUENCE] = column_sequence

        # check task, model_type and get the adapter_class
        adapter_class = self.__args_to_adapter_class(task=task, model_type=model_type)
        task_enum = TaskType[task]

        # check and cast to ts dataframe
        time_column = to_ts_dataframe(df=df, time_col=time_column)
        args[self.ARG_USING_TIME_COLUMN] = time_column

        # train model
        model_args = {}
        if task_enum == TaskType.forecast:
            model_args[MerlionArguments.max_forecast_steps.value] = horizon
        adapter: BaseMerlionForecastAdapter = adapter_class(**model_args)
        print("Training model, args: " + json.dumps(args))
        adapter.train(df=df, target=target)
        print("Training model completed.")

        # persist save model
        model_bytes = adapter.to_bytes()
        self.model_storage.file_set(self.PERSISIT_MODEL_FILE_NAME, model_bytes)
        self.model_storage.json_set(self.PERSISIT_ARGS_KEY_IN_JSON_STORAGE, args)
        print("Model and args saved.")

        # back mapping column name
        df.rename(columns=column_name_back_mapping, inplace=True)

    def predict(self, df):
        rt_df = df.copy(deep=True)
        column_name_mapping, column_name_back_mapping = get_column_name_format_mapping(rt_df)
        rt_df.rename(columns=column_name_mapping, inplace=True)
        # read model and args from storage
        model_bytes = self.model_storage.file_get(self.PERSISIT_MODEL_FILE_NAME)
        args = self.model_storage.json_get(self.PERSISIT_ARGS_KEY_IN_JSON_STORAGE)

        # resolve args
        task = args[self.ARG_USING_TASK]
        model_type = args[self.ARG_USING_MODEL_TYPE]
        time_column = args[self.ARG_USING_TIME_COLUMN]
        target = args[self.ARG_TARGET]
        horizon = args[self.ARG_HORIZON]
        # window = args[self.ARG_WINDOW]
        feature_column_sequence = list(args[self.ARG_COLUMN_SEQUENCE])
        task_enum = TaskType[task]

        # check df and prepare data
        if task_enum == TaskType.forecast:
            feature_column_sequence.remove(target)
        missing_required_columns = set(feature_column_sequence) - set(rt_df.columns.values)
        if len(missing_required_columns) > 0:
            raise Exception("Missing required columns: " + ",".join(missing_required_columns))
        feature_df = rt_df[feature_column_sequence]
        to_ts_dataframe(df=feature_df, time_col=time_column)

        # init model adapter
        adapter_class: BaseMerlionForecastAdapter = self.__args_to_adapter_class(task=task, model_type=model_type)
        model_args = {}
        if task_enum == TaskType.forecast:
            model_args[MerlionArguments.max_forecast_steps.value] = horizon
        adapter = adapter_class(**model_args)
        adapter.from_bytes(bytes=model_bytes)

        # predict
        pred_df = adapter.predict(df=feature_df, target=target)

        # build result
        pred_df = feature_df[[]].join(pred_df, how="left")

        # arrange data
        pred_df.index = rt_df.index
        if task_enum == TaskType.forecast:
            pred_df = pred_df[~pred_df[target].isna()]
            rt_df.drop(columns=[target], inplace=True)
        elif task_enum == TaskType.detector:
            pred_df[f"{target}__anomaly_score"].fillna(0, inplace=True)
        rt_df = rt_df.join(pred_df, how="right")
        # back mapping column name
        rt_df.rename(columns=column_name_back_mapping, inplace=True)
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

    @staticmethod
    def get_custom_column_appendix_dict() -> dict:
        return {"": "float", "__upper": "float", "__lower": "float", "__anomaly_score": "float"}