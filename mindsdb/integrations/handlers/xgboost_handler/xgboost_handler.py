import category_encoders as ce
from enum import Enum
import json
import numpy as np
import optuna
import pandas as pd
import pickle
from sklearn.metrics import accuracy_score, mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
from xgboost import XGBClassifier, XGBRegressor

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log
import xgboost as xgb


class BoosterType(Enum):
    gbtree = "gbtree"
    gblinear = "gblinear"


class ObjectiveType(Enum):
    reg_linear = "reg:linear"
    reg_squarederror = "reg:squarederror"
    multi_softmax = "multi:softmax"
    multi_softprob = "multi:softprob"


class TaskType(Enum):
    auto = "auto"
    regressor = "regressor"
    classifier = "classifier"
    a = "a"
    c = "c"
    r = "r"
    reg = "reg"
    cla = "cla"


def enum_to_str(type_class: Enum) -> str:
    all = []
    for element in type_class:
        all.append(element.name)
    return "|".join(all)


def to_ts_dataframe(df: pd.DataFrame, time_col=None) -> str:
    columns = list(df.columns.values)
    if time_col is not None:
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


def check_target_column_type(df: pd.DataFrame, target: str, task_type: TaskType):
    MAX_LABEL_NUMBER = 100
    UNIQUE_CNT_TO_BE_REG = 5
    target_type = df[target].dtype
    label_cnt = len(df[target].unique())
    if label_cnt == 1:
        raise Exception("Label error: the count of unique value in target column can not be less than 2.")
    if task_type == TaskType.regressor and (target_type == np.dtype("float") or target_type == np.dtype("int") or
                                            target_type is np.dtype("float") or target_type is np.dtype("int")):
        return TaskType.regressor, 0
    if task_type == TaskType.classifier and (target_type == np.dtype("object") or target_type == np.dtype("bool") or
                                             target_type is np.dtype("object") or target_type is np.dtype("bool")):
        return TaskType.classifier, label_cnt

    if task_type == TaskType.classifier and label_cnt <= MAX_LABEL_NUMBER:
        return TaskType.classifier, label_cnt
    if task_type == TaskType.auto:
        if target_type == np.dtype("float") or target_type == np.dtype("int") \
                or target_type is np.dtype("float") or target_type is np.dtype("int"):
            if label_cnt <= UNIQUE_CNT_TO_BE_REG:
                return TaskType.classifier, label_cnt
            else:
                return TaskType.regressor, 0
        else:
            return TaskType.classifier, label_cnt


class FeatureProcessor:
    def process(self, df: pd.DataFrame):
        pass


class ConFillNa(FeatureProcessor):
    def __init__(self, df: pd.DataFrame, column):
        self.column = column
        self.val = df[~df[column].isna()][column].mean()

    def process(self, df: pd.DataFrame):
        df_tmp = df[[self.column]].copy()
        df_tmp[self.column].fillna(self.val, inplace=True)
        return df_tmp[self.column].values.reshape([-1, 1])


class ConNormalize(FeatureProcessor):
    def __init__(self, df: pd.DataFrame, column):
        self.column = column
        self.mea = df[column].mean()
        self.std = df[column].std()
        self.fillna_with = df[~df[column].isna()][column].mean()

    def process(self, df: pd.DataFrame):
        df_tmp = df[[self.column]].copy()
        df_tmp[self.column].fillna(self.fillna_with, inplace=True)
        if self.std > 0:
            rt = (np.array(df_tmp[self.column].values) - self.mea) / self.std
        else:
            rt = np.array(df_tmp[self.column].values)
        return rt.reshape([-1, 1])


class CatOnehot(FeatureProcessor):
    def __init__(self, df, column):
        self.column = column
        self.encoder: OneHotEncoder = OneHotEncoder(handle_unknown="ignore")
        self.encoder.fit(df[column].values.reshape([-1, 1]))

    def process(self, df: pd.DataFrame):
        return self.encoder.transform(df[self.column].values.reshape([-1, 1])).toarray()


class CatTarget(FeatureProcessor):
    def __init__(self, df, column, target):
        self.column = column
        self.target = target
        self.encoder: ce.TargetEncoder = ce.TargetEncoder(cols=[column])
        self.encoder.fit(X=df[column], y=df[self.target])

    def process(self, df: pd.DataFrame):
        return self.encoder.transform(df[self.column]).values


class FeatureTransformer:
    def __init__(self):
        self.column_sequence = []
        self.column_processor = {}
        self.max_null_percent = 0.95
        self.max_unique_obj_percent = 0.95
        self.max_onehot_num = 30
        self.label_encoder = None

    def fit(self, df: pd.DataFrame, target):
        columns = list(df.columns.values)
        columns.remove(target)
        df_len = float(len(df))
        columns = sorted(columns)
        null_threshold = self.max_null_percent * df_len
        unique_obj_threshold = self.max_unique_obj_percent * df_len
        for column in columns:
            column_name_alias = str(column).strip().lower()
            column_type = df[column].dtype
            val_len = len(df[column].unique())
            na_len = float(len(df[df[column].isna()]))
            if na_len > null_threshold:
                log.logger.info(f"Drop {column}, too much null (>={self.max_null_percent * 100}%)")
                continue
            if val_len == 1:
                log.logger.info(f"Drop {column}, column has the constant value")
                continue
            if column_type == np.dtype("int") or column_type is np.dtype("int"):
                if na_len > 0 and (column_name_alias == "id" or column_name_alias.endswith("id")) and val_len == df_len:
                    log.logger.info(f"Drop id column: {column}")
                    continue
                if na_len > 0 and val_len == df_len and df[column].max() - df[column].min() + 1 == df_len:
                    log.logger.info(f"Drop id column: {column}")
                    continue
                self.column_processor[column] = [ConFillNa(df=df, column=column)]
            elif column_type == np.dtype("float") or column_type is np.dtype("float"):
                self.column_processor[column] = [ConFillNa(df=df, column=column)]
            elif column_type == np.dtype("bool") or column_type is np.dtype("bool"):
                self.column_processor[column] = [CatTarget(df=df, column=column, target=target),
                                                 CatOnehot(df=df, column=column)]
            elif column_type == np.dtype("object") or column_type is np.dtype("object"):
                if val_len >= unique_obj_threshold:
                    log.logger.info(f"Drop column with too many unique values: {column}")
                    continue
                self.column_processor[column] = [CatTarget(df=df, column=column, target=target)]
                if val_len <= self.max_onehot_num:
                    self.column_processor[column].append(CatOnehot(df=df, column=column))
            self.column_sequence.append(column)
        return self.column_sequence

    def transform(self, df: pd.DataFrame, label=None, to_d_metrix=True):
        provided_columns_set = set(df.columns.values)
        required_columns_set = set(self.column_sequence)
        array_np_features = None
        missing_columns = required_columns_set - provided_columns_set
        if len(missing_columns) > 0:
            raise Exception("Missing required columns: " + ",".join(list(missing_columns)))
        for column in self.column_sequence:
            processors = self.column_processor[column]
            for processor in processors:
                processor: FeatureProcessor = processor
                generated_np_feature = processor.process(df)
                if generated_np_feature is None:
                    continue
                if array_np_features is None:
                    array_np_features = generated_np_feature
                else:
                    array_np_features = np.concatenate([array_np_features, generated_np_feature], axis=1)

        if not to_d_metrix:
            if label is None:
                return array_np_features
            else:
                return array_np_features, label

        if label is None:
            d_matrix = xgb.DMatrix(array_np_features)
        else:
            d_matrix = xgb.DMatrix(array_np_features, label=label)
        return d_matrix


class XgboostHandler(BaseMLEngine):
    name = 'xgboost'

    ARG_USING_TASK = "task"
    ARG_USING_AUTOML = "automl"
    ARG_USING_TRAIL = "trail"

    # only be used to persist args to args.json
    ARG_CLASS_NUM = "class_num"
    ARG_COLUMN_SEQUENCE = "column_sequence"
    ARG_TARGET = "target"

    KWARGS_DF = "df"

    PERSISIT_MODEL_FILE_NAME = "xgboost_model"
    PERSISIT_FEATURE_TRANSFORMER = "feature_transformer"
    PERSISIT_ARGS_KEY_IN_JSON_STORAGE = "args"

    DEFAULT_EARLYSTOP_ROUND = 50
    DEFAULT_TRAIL = 20
    DEFAULT_USE_AUTOML = False

    def create(self, target, args=None, **kwargs):
        df: pd.DataFrame = kwargs.get(self.KWARGS_DF, None)
        # drop row with na-target
        df = df[~df[target].isna()]
        # prepare arguments
        task_str = args.get(self.ARG_USING_TASK, TaskType.auto.name)
        use_auto_ml = args.get(self.ARG_USING_AUTOML, self.DEFAULT_USE_AUTOML)
        opt_trail = args.get(self.ARG_USING_TRAIL, self.DEFAULT_TRAIL)
        if not isinstance(use_auto_ml, bool):
            use_auto_ml = self.DEFAULT_USE_AUTOML
        if not isinstance(opt_trail, int):
            opt_trail = self.DEFAULT_TRAIL

        # check arguments
        try:
            task_type = None
            task_type = TaskType(task_str)
        except Exception as e:
            if task_type is None:
                raise Exception("task error: " + task_str + ", options:" +
                                "|".join([str(t) for t in TaskType.__members__]))
        if task_type in (TaskType.a, TaskType.auto):
            task_type = TaskType.auto
        elif task_type in (TaskType.r, TaskType.reg, TaskType.regressor):
            task_type = TaskType.regressor
        elif task_type in (TaskType.c, TaskType.cla, TaskType.classifier):
            task_type = TaskType.classifier
        else:
            raise Exception("unsupported task type: " + str(task_type))
        # check df
        if df is None:
            raise Exception("missing required key in args: " + self.KWARGS_DF)
        # validate and check the task type
        task_type, class_num = check_target_column_type(df=df, target=target, task_type=task_type)
        if task_type not in (TaskType.classifier, TaskType.regressor):
            raise Exception("Unsupported task type: " + task_type.name)
        # feature analysis
        feature_transformer = FeatureTransformer()
        feature_transformer.fit(df=df, target=target)
        candidate_feature_columns = list(df.columns.values)
        candidate_feature_columns.remove(target)
        feature_df = df[candidate_feature_columns]
        # prepare model and config
        if task_type == TaskType.regressor:
            target_col = df[target]
            config, get_tune_para_func, metric_str = \
                XgboostHandler.get_regressor_default_config()
            model = XGBRegressor(**config)
        else:
            feature_transformer.label_encoder = LabelEncoder()
            target_col = feature_transformer.label_encoder.fit_transform(df[target])
            config, get_tune_para_func, metric_str = \
                XgboostHandler.get_classifier_default_config(num_class=class_num)
            model = XGBClassifier(**config)
        # prepare training samples
        train_x = feature_transformer.transform(feature_df, to_d_metrix=False)
        train_y = target_col
        # search parameters
        if use_auto_ml:
            tmp_train_x, val_x, tmp_train_y, val_y = train_test_split(train_x, train_y, test_size=0.3,
                                                                      random_state=47)

            def objective(trial: optuna.Trial):
                tune_args = get_tune_para_func(trial)
                tmp_args = dict(config)
                tmp_args.update(tune_args)
                tmp_args.update({"eval_metric": metric_str, "early_stopping_rounds": 30})
                if task_type == TaskType.regressor:
                    model = XGBRegressor(**tmp_args)
                    model.fit(tmp_train_x, tmp_train_y, eval_set=[(tmp_train_x, tmp_train_y)], verbose=50)
                    y_pred = model.predict(val_x)
                    return - mean_squared_error(val_y, y_pred)
                else:
                    model = XGBClassifier(**tmp_args)
                    model.fit(tmp_train_x, tmp_train_y, eval_set=[(tmp_train_x, tmp_train_y)], verbose=50)
                    y_pred = model.predict(val_x)
                    return accuracy_score(val_y, np.argmax(y_pred, axis=1))

            study = optuna.create_study(direction='maximize')
            study.optimize(objective, n_trials=opt_trail)
            optimized_parameter_log = ",".join([F"{k}:{v}" for k, v in study.best_params.items()])
            log.logger.info("best_params: " + optimized_parameter_log)
            log.logger.info("best_value: " + str(study.best_value))
            config.update(study.best_params)
            config.update({"eval_metric": metric_str, "early_stopping_rounds": self.DEFAULT_EARLYSTOP_ROUND})
            if task_type == TaskType.regressor:
                model = XGBRegressor(**config)
            else:
                model = XGBClassifier(**config)
        # train model
        log.logger.info(F"Model training.")
        try:
            model.fit(train_x, train_y, eval_set=[(train_x, train_y)], verbose=50)
        except Exception as e:
            log.logger.warn(F"Model training failed: " + self.name)
            raise e
        log.logger.info(F"Model trained.")
        # save args, model, feature transformer
        args[self.ARG_TARGET] = target
        args[self.ARG_USING_TASK] = task_type.value
        args[self.ARG_CLASS_NUM] = class_num
        args[self.ARG_COLUMN_SEQUENCE] = candidate_feature_columns
        log.logger.info(F"Model config: " + json.dumps(args))
        model_bytes = model.get_booster().save_raw()
        feature_transformer_bytes = pickle.dumps(feature_transformer)
        self.model_storage.json_set(self.PERSISIT_ARGS_KEY_IN_JSON_STORAGE, args)
        self.model_storage.file_set(self.PERSISIT_MODEL_FILE_NAME, model_bytes)
        self.model_storage.file_set(self.PERSISIT_FEATURE_TRANSFORMER, feature_transformer_bytes)
        log.logger.info(F"Model saved.")

    def predict(self, df, args):
        # read from storage
        args = self.model_storage.json_get(self.PERSISIT_ARGS_KEY_IN_JSON_STORAGE)
        feature_transofrmer_bytes = self.model_storage.file_get(self.PERSISIT_FEATURE_TRANSFORMER)
        boost_model_bytes = self.model_storage.file_get(self.PERSISIT_MODEL_FILE_NAME)
        # initialize args, model, transformer
        target = args[self.ARG_TARGET]
        task_type = TaskType(args[self.ARG_USING_TASK])
        class_num = args[self.ARG_CLASS_NUM]
        feature_transformer: FeatureTransformer = pickle.loads(feature_transofrmer_bytes)
        boost_model: xgb.Booster = xgb.Booster(model_file=bytearray(boost_model_bytes))
        # predict
        p_feature_data = feature_transformer.transform(df=df)
        pred_data = boost_model.predict(p_feature_data)
        if task_type == TaskType.classifier:
            label_arr = feature_transformer.label_encoder.inverse_transform([i for i in range(class_num)])
            pred_label = [str(label_arr[i]) for i in np.argmax(pred_data, axis=1)]
            pred_probs_arr = []
            for probs in pred_data:
                prob_items = []
                for i, prob in enumerate(probs):
                    prob_items.append("\"" + str(label_arr[i]) + "\"=" + str(round(prob, 5)))
                pred_probs_arr.append("{" + ",".join(prob_items) + "}")
            rt_df = pd.DataFrame(index=df.index,
                                 data={target: pred_label, F"{target}__prob": pred_probs_arr})
        else:
            rt_df = pd.DataFrame(index=df.index, data={target: pred_data})
        columns_with_target = list(df.columns.values)
        if target in set(columns_with_target):
            columns_with_target.remove(target)
        rt_df = df[columns_with_target].join(rt_df)
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
    def get_classifier_default_config(num_class: int) \
            -> (dict, object, str):
        default_args = {"max_depth": 5,
                        "learning_rate": 0.1,
                        "n_estimators": 100,
                        "objective": ObjectiveType.multi_softprob.value,
                        "num_class": num_class,
                        "booster": BoosterType.gbtree.value,
                        "gamma": 0,
                        "min_child_weight": 1,
                        "max_delta_step": 0,
                        "subsample": 1,
                        "colsample_bytree": 1,
                        "reg_alpha": 0,
                        "reg_lambda": 1,
                        "n_jobs": 4,
                        "seed": 2973}

        def get_tune_parameters(trial: optuna.Trial):
            return {"max_depth": trial.suggest_int("max_depth", 2, 20, 2),
                    "n_estimators": trial.suggest_int("n_estimators", 10, 150, 10),
                    "gamma": trial.suggest_float(name="gamma", low=0, high=3, step=0.5),
                    "min_child_weight": trial.suggest_int("min_child_weight", 1, 5, 1),
                    "reg_alpha": trial.suggest_float(name="reg_alpha", low=0, high=6, step=1),
                    "reg_lambda": trial.suggest_float(name="reg_lambda", low=0, high=6, step=1)}

        return default_args, get_tune_parameters, "mlogloss"

    @staticmethod
    def get_regressor_default_config() \
            -> (dict, object, str):
        default_args = {"max_depth": 5,
                        "learning_rate": 0.1,
                        "n_estimators": 100,
                        "objective": ObjectiveType.reg_squarederror.value,
                        "booster": BoosterType.gblinear.value,
                        "n_jobs": 4,
                        "gamma": 0,
                        "min_child_weight": 1,
                        "max_delta_step": 0,
                        "subsample": 1,
                        "colsample_bytree": 1,
                        "colsample_bylevel": 1,
                        "reg_alpha": 0,
                        "reg_lambda": 1,
                        "scale_pos_weight": 1,
                        "base_score": 0.5,
                        "random_state": 0,
                        "seed": 599,
                        "importance_type": 'gain'}

        def get_tune_parameters(trial: optuna.Trial):
            return {"max_depth": trial.suggest_int("max_depth", 2, 20, 2),
                    "n_estimators": trial.suggest_int("n_estimators", 10, 150, 10),
                    "gamma": trial.suggest_float(name="gamma", low=0, high=3, step=0.5),
                    "min_child_weight": trial.suggest_int("min_child_weight", 1, 5, 1),
                    "reg_alpha": trial.suggest_float(name="reg_alpha", low=0, high=6, step=1),
                    "reg_lambda": trial.suggest_float(name="reg_lambda", low=0, high=6, step=1)}

        return default_args, get_tune_parameters, "rmse"
