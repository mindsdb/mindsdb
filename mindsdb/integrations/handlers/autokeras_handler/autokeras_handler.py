import os
import random
import string
from typing import Optional

import numpy as np
import pandas as pd
import autokeras as ak
from sklearn import preprocessing
from tensorflow.keras.models import load_model

from mindsdb.integrations.libs.base import BaseMLEngine


# Makes this run on systems where this arg isn't specified, like Windows Subsystem for Linux
# Doesn't break things on Mac
os.environ["XLA_FLAGS"] = "--xla_gpu_cuda_data_dir=/usr/lib/cuda"

DEFAULT_TRIALS = 100


def train_model(df, target, max_trials=DEFAULT_TRIALS):
    """Helper function to trains an AutoKeras model with an input df.

    Automatically decides on classification vs. regression depending on
    the type of the target.

    Will auto-encode categorical variables as dummies.

    Returns both trained model and the names of categoric dummy columns, which
    are passed later to the prediction method.
    """
    # Choose regressor of classifier based on target data type
    if np.issubdtype(df[target].dtype, np.number):
        mode = "regression"
        y_train = df[target].to_numpy()
    else:
        mode = "classification"
        lb = preprocessing.LabelBinarizer()
        y_train = lb.fit_transform(df[target])

    training_df = df.drop(target, axis=1)
    trainer = ak.AutoModel(
        inputs=ak.Input(),
        outputs=ak.RegressionHead() if mode == "regression" else ak.ClassificationHead(),
        overwrite=True,
        max_trials=max_trials
    )
    # Save the column names of all numeric columns before transforming any categorical columns into dummies
    numeric_column_names = training_df.select_dtypes(include=[np.number]).columns.values.tolist()
    training_df = pd.get_dummies(training_df)
    categorical_dummy_column_names = [
        col for col in training_df.columns.values.tolist() if col not in numeric_column_names
    ]
    x_train = training_df.to_numpy()
    trainer.fit(x_train, y_train, verbose=2)
    return trainer.export_model(), categorical_dummy_column_names


def get_preds_from_model(df, model, target, column_count, categorical_dummy_column_names):
    """Gets predictions from the stored AutoKeras model."""
    df_to_predict = df.copy()
    for col in ["__mindsdb_row_id", target]:
        if col in df_to_predict.columns.values.tolist():
            df_to_predict = df_to_predict.drop(col, axis=1)

    if len(df_to_predict.columns) != column_count:
        raise Exception("All feature columns must be defined in the WHERE clause when making predictions")
    # Get dummies for any categorical columns and then populate the missing ones with zeros
    df_with_dummies = pd.get_dummies(df_to_predict)
    for col in categorical_dummy_column_names:  # exception handler for empty columns
        if col not in df_with_dummies.columns.values.tolist():
            df_with_dummies[col] = 0

    return model.predict(df_with_dummies, verbose=2)


def format_categorical_preds(predictions, original_y, df_to_predict, target_col):
    """Transforms class predictions back to their original class.

    Categoric predictions come out the AutoKeras model in a binary
    format e.g. (0, 1, 0). This function maps them back to their
    original class e.g. 'Blue', and adds a DF column for the
    model confidence score.
    """
    # Turn prediction back into categorical value
    lb = preprocessing.LabelBinarizer()
    lb.fit(original_y)
    preds = lb.inverse_transform(predictions)

    # Add the confidence score next to the prediction
    df_to_predict[target_col] = pd.Series(preds).astype(original_y.dtype)
    df_to_predict["confidence"] = [max(row) for _, row in enumerate(predictions)]
    return df_to_predict


class AutokerasHandler(BaseMLEngine):
    """
    Integration with the AutoKeras ML library.
    """  # noqa

    name = "autokeras"

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        """Create and tune AutoKeras model using the input df.

        Saves the AutoKeras best model params to model storage.
        """
        args = args["using"]  # ignore the rest of the problem definition
        args["target"] = target
        max_trials = int(args["train_time"] * DEFAULT_TRIALS) if "train_time" in args else DEFAULT_TRIALS
        # Save the training df in order to filter the training data based on the predict df
        args["training_df"] = df.to_json()
        args["training_data_column_count"] = len(df.columns) - 1  # subtract 1 for target

        random_string = "".join(random.choices(string.ascii_uppercase + string.digits, k=24))
        args["folder_path"] = os.path.join("autokeras", f"{random_string}.keras")
        os.makedirs(os.path.dirname(args["folder_path"]), exist_ok=True)

        model, args["data_column_names"] = train_model(df, target, max_trials)
        model.save(args["folder_path"])
        self.model_storage.json_set("predict_args", args)

    def predict(self, df, args=None):
        """Predicts with best saved AutoKeras model."""
        args = self.model_storage.json_get("predict_args")
        training_df = pd.read_json(args["training_df"])
        model = load_model(args["folder_path"], custom_objects=ak.CUSTOM_OBJECTS)

        df_to_predict = df.copy()
        predictions = get_preds_from_model(df_to_predict, model, args["target"], args["training_data_column_count"], args["data_column_names"])

        # If we used the classifier we need to pre-process the predictions before returning them
        original_y = training_df[args["target"]]
        if not np.issubdtype(original_y.dtype, np.number):
            return format_categorical_preds(predictions, original_y, df_to_predict, args["target"])

        df_to_predict[args["target"]] = predictions
        return df_to_predict
