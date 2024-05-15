from mindsdb.integrations.libs.base import BaseMLEngine
import pandas as pd
from mindsdb.integrations.handlers.anomaly_detection_handler.utils import (
    train_unsupervised,
    train_supervised,
    train_semisupervised,
)
from joblib import dump, load
from pyod.models.ecod import ECOD  # unsupervised default
from pyod.models.xgbod import XGBOD  # semi-supervised default
from catboost import CatBoostClassifier  # supervised default
from pyod.models.lof import LOF
from pyod.models.knn import KNN
from pyod.models.pca import PCA
from xgboost import XGBClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.preprocessing import StandardScaler
import numpy as np
import os


MODELS = {
    "supervised": {
        "catboost": CatBoostClassifier(logging_level="Silent"),
        "xgb": XGBClassifier(),
        "nb": GaussianNB(),
    },
    "semi-supervised": {
        "xgbod": XGBOD(estimator_list=[ECOD()]),
    },
    "unsupervised": {
        "ecod": ECOD(),
        "knn": KNN(),
        "pca": PCA(),
        "lof": LOF(),
    },
}


def choose_model_type(training_df, model_type=None, target=None, supervised_threshold=3000):
    """Choose the model type based on the presence of labels and size of the dataset"""
    if model_type is None:
        if target is None:
            model_type = "unsupervised"
        else:
            model_type = "supervised" if len(training_df) > supervised_threshold else "semi-supervised"
    assert model_type in [
        "supervised",
        "semi-supervised",
        "unsupervised",
    ], "model type must be 'supervised', 'semi-supervised', or 'unsupervised'"
    return model_type


def choose_model(df, model_name=None, model_type=None, target=None, supervised_threshold=3000):
    """Choose the best model based on the size of the dataset and the model type"""
    training_df = preprocess_data(df)
    model_type = choose_model_type(training_df, model_type, target, supervised_threshold)
    if model_name is not None:
        assert model_name in MODELS[model_type], f"model name must be one of {list(MODELS[model_type].keys())}"
        model = MODELS[model_type][model_name]
    else:
        model = None
    if model_type == "unsupervised":
        return train_unsupervised(training_df, model=model)
    X_train = training_df.drop(target, axis=1)
    y_train = training_df[target].astype(int)

    if model_type == "supervised":
        return train_supervised(X_train, y_train, model=model)
    elif model_type == "semi-supervised":
        return train_semisupervised(X_train, y_train)  # Only one semi-supervised model available


def anomaly_type_to_model_name(anomaly_type):
    """Choose the best model name based on the anomaly type"""
    assert anomaly_type in [
        "local",
        "global",
        "clustered",
        "dependency",
    ], "anomaly type must be 'local', 'global', 'clustered', or 'dependency'"
    anomaly_type_dict = {
        "local": "lof",
        "global": "knn",
        "clustered": "pca",
        "dependency": "knn",
    }
    return anomaly_type_dict[anomaly_type]


def preprocess_data(df):
    """Preprocess the data by one-hot encoding categorical columns and scaling numeric columns"""
    # one-hot encode categorical columns
    categorical_columns = list(df.select_dtypes(include=["object"]).columns.values)
    df[categorical_columns] = df[categorical_columns].astype("category")
    df[categorical_columns] = df[categorical_columns].apply(lambda x: x.cat.codes)
    df = pd.get_dummies(df, columns=categorical_columns)
    numeric_columns = list(df.select_dtypes(include=["number"]).columns.values)

    scaler = StandardScaler()
    df[numeric_columns] = scaler.fit_transform(df[numeric_columns])
    return df


def get_model_names(using_args):
    """Get the model names from the using_args. Model names is a list of model names to train.
    If the model is not an ensemble, it only contains one model"""
    model_names = anomaly_type_to_model_name(using_args["anomaly_type"]) if "anomaly_type" in using_args else None
    model_names = using_args["model_name"] if "model_name" in using_args else model_names
    model_names = using_args["ensemble_models"] if "ensemble_models" in using_args else model_names
    model_names = [model_names] if model_names is None else model_names
    model_names = [model_names] if type(model_names) is str else model_names
    return model_names


class AnomalyDetectionHandler(BaseMLEngine):
    """Integration with the PyOD and CatBoost libraries for
    anomaly detection. Both supervised and unsupervised.
    """

    name = "anomaly_detection"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True  # makes unsupervised learning work

    def create(self, target, df, args=None):
        """Train a model and save it to the model storage"""
        args = {} if args is None else args
        using_args = args["using"]
        model_type = using_args["type"] if "type" in using_args else None

        model_names = get_model_names(using_args)

        model_save_paths = []
        model_targets = []
        model_class_names = []
        base_path = self.model_storage.folder_get("context")
        for model_name in model_names:
            model = choose_model(df, model_name=model_name, model_type=model_type, target=target)
            this_model_target = "outlier" if target is None else target  # output column name for unsupervised learning
            save_path = "model.joblib" if model_name is None else model_name + ".joblib"
            dump(model, os.path.join(base_path, save_path))
            model_save_paths.append(save_path)
            model_targets.append(this_model_target)
            model_class_names.append(model.__class__.__name__)
        model_args = {"model_path": model_save_paths, "target": model_targets, "model_name": model_class_names}
        self.model_storage.json_set("model_args", model_args)
        self.model_storage.folder_sync("context")

    def predict(self, df, args=None):
        """Load a model from the model storage and use it to make predictions"""
        args = {} if args is None else args
        model_args = self.model_storage.json_get("model_args")
        results_list = []
        if "__mindsdb_row_id" in df.columns:
            df = df.drop("__mindsdb_row_id", axis=1)
        if model_args["target"][0] in df.columns:
            df = df.drop(model_args["target"], axis=1)
        base_path = self.model_storage.folder_get("context")
        for model_path in model_args["model_path"]:
            model = load(os.path.join(base_path, model_path))
            predict_df = preprocess_data(df).astype(float)
            results = model.predict(predict_df)
            results_list.append(results)
        final_results = np.array(results_list).mean(axis=0)
        final_results = np.where(final_results > 0.5, 1, 0)
        return pd.DataFrame({model_args["target"][0]: final_results})

    def describe(self, attribute="model"):
        model_args = self.model_storage.json_get("model_args")
        df = pd.DataFrame({"model_name": [], "target": []})
        if attribute == "model":
            for model_name, target in zip(model_args["model_name"], model_args["target"]):
                df2 = pd.DataFrame({"model_name": model_name, "target": target}, index=[0])
                df = pd.concat([df, df2], ignore_index=True)
            return df
        else:
            raise NotImplementedError(f"attribute {attribute} not implemented")
