import random
import string
from typing import Optional
import os
import pandas as pd
import numpy as np
import autokeras as ak
from sklearn import preprocessing
from mindsdb.integrations.libs.base import BaseMLEngine
from tensorflow.keras.models import load_model


# Makes this run on Windows Subsystem for Linux
os.environ["XLA_FLAGS"] = "--xla_gpu_cuda_data_dir=/usr/lib/cuda"

trainer_dict = {"regression": ak.StructuredDataRegressor, "classification": ak.StructuredDataClassifier}

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
        y_train = df[target]
    else:
        mode = "classification"
        lb = preprocessing.LabelBinarizer()
        y_train = lb.fit_transform(df[target])

    training_df = df.drop(target, axis=1)
    trainer = trainer_dict[mode](overwrite=True, max_trials=max_trials)

    # Save the column names of all numeric columns before transforming any categorical columns into dummies
    numeric_column_names = training_df.select_dtypes(include=[np.number]).columns.values.tolist()
    training_df = pd.get_dummies(training_df)
    categorical_dummy_column_names = [
        col for col in training_df.columns.values.tolist() if col not in numeric_column_names
    ]
    trainer.fit(training_df, y_train, verbose=2)
    return trainer.export_model(), categorical_dummy_column_names


def get_prediction_df(mindsdb_df, training_df):
    """Gets a prediction df using the df passed by the predict() method and the training df.

    mindsdb_df is passed in from the .predict() method. This drops columns when users call
    the WHERE clause in a SQL query.

    training_df is the full df from training. This contains the dropped columns, which we
    join with the rows from the mindsdb_df.
    """
    # Remove column that didn't exist in the training df so that we can do filtering
    df_to_predict = mindsdb_df.copy()
    if "__mindsdb_row_id" in df_to_predict.columns.values.tolist():
        df_to_predict = df_to_predict.drop("__mindsdb_row_id", axis=1)

    # Filter the training df based on the predict df
    keys = list(df_to_predict.columns.values)
    i1 = training_df.set_index(keys).index
    i2 = df_to_predict.set_index(keys).index
    filtered_df = training_df[i1.isin(i2)]

    if filtered_df.empty:
        # TODO: Rephrase the exception message in a more user-friendly way
        raise Exception(
            "The condition(s) in the WHERE clause filtered out all the data. Please refine these and try again"
        )
    return filtered_df


def get_preds_from_model(df, model, target, categorical_dummy_column_names):
    """Gets predictions from the stored AutoKeras model."""
    cols_to_drop = ["__mindsdb_row_id", target]
    for col in cols_to_drop:
        if col in df.columns.values.tolist():
            df = df.drop(col, axis=1)

    # Get dummies for any categorical columns and then populate the missing ones with zeros
    prediction_df = pd.get_dummies(df)
    for col in categorical_dummy_column_names:  # exception handler for empty columns
        if col not in prediction_df.columns.values.tolist():
            prediction_df[col] = 0

    return model.predict(prediction_df, verbose=2)


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
        args["folder_path"] = os.path.join("autokeras", random_string)

        model, args["data_column_names"] = train_model(df, target, max_trials)
        model.save(args["folder_path"])
        self.model_storage.json_set("predict_args", args)

    def predict(self, df, args=None):
        """Predicts with best saved AutoKeras model."""
        args = self.model_storage.json_get("predict_args")
        training_df = pd.read_json(args["training_df"])
        model = load_model(args["folder_path"], custom_objects=ak.CUSTOM_OBJECTS)

        df_to_predict = get_prediction_df(df, training_df)
        predictions = get_preds_from_model(df_to_predict, model, args["target"], args["data_column_names"])

        # If we used the classifier we need to pre-process the predictions before returning them
        original_y = training_df[args["target"]]
        if not np.issubdtype(original_y.dtype, np.number):
            return format_categorical_preds(predictions, original_y, df_to_predict, args["target"])

        df_to_predict[args["target"]] = predictions
        return df_to_predict
