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

from mindsdb.utilities import log

# Makes this run on Windows Subsystem for Linux
os.environ["XLA_FLAGS"]="--xla_gpu_cuda_data_dir=/usr/lib/cuda"

trainer_dict = {
    "regression": ak.StructuredDataRegressor,
    "classification": ak.StructuredDataClassifier
}

DEFAULT_EPOCHS = 3
DEFAULT_TRIALS = 3

def train_autokeras_model(df, target):
    # Choose regressor of classifier based on target data type
    if np.issubdtype(df[target].dtype, np.number):
        mode = "regression"
        y_train = df[target]
    else:
        mode = "classification"
        lb = preprocessing.LabelBinarizer()
        y_train = lb.fit_transform(df[target])

    training_df = df.drop(target, axis=1)
    trainer = trainer_dict[mode](overwrite=True, max_trials=DEFAULT_TRIALS)

    # Save the column names of all numeric columns before transforming any categorical columns into dummies
    numeric_column_names = training_df.select_dtypes(include=[np.number]).columns.values.tolist()

    training_df = pd.get_dummies(training_df)

    # Extract all dummy column names. These will be used to prepare the predict df before the prediction
    categorical_dummy_column_names = [col for col in training_df.columns.values.tolist() if col not in numeric_column_names]

    trainer.fit(training_df, y_train, epochs=DEFAULT_EPOCHS)
    return trainer.export_model(), categorical_dummy_column_names


def get_preds_from_autokeras_model(df, model, target, categorical_dummy_column_names):
    # Get dummies for any categorical columns and then populate the missing ones with zeros
    prediction_df = pd.get_dummies(df)
    for col in categorical_dummy_column_names:
        if col not in prediction_df.columns.values.tolist():
            prediction_df[col] = 0

    return model.predict(prediction_df)



class AutokerasHandler(BaseMLEngine):
    """
    Integration with the AutoKeras ML library.
    """  # noqa

    name = 'autokeras'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        """
        Create and train model on the input df
        """
        args = args['using']  # ignore the rest of the problem definition
        args["target"] = target
        # Save the training df in order to filter the training data based on the predict df
        args["training_df"] = df.to_json()
        args["training_data_column_count"] = len(df.columns) - 1 # subtract 1 for target

        random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=24))
        args["folder_path"] = os.path.join("autokeras", random_string)

        model, args["data_column_names"] = train_autokeras_model(df, target)
        model.save(args["folder_path"])
        self.model_storage.json_set("predict_args", args)
    
    def predict(self, df, args=None):
        args = self.model_storage.json_get("predict_args")
        training_df = pd.read_json(args["training_df"])

        model = load_model(args["folder_path"], custom_objects=ak.CUSTOM_OBJECTS)

        df_to_predict = df.copy()
        # Remove column that didn't exist in the training df so that we can do filtering
        if "__mindsdb_row_id" in df_to_predict.columns.values.tolist():
            df_to_predict = df_to_predict.drop("__mindsdb_row_id", axis=1)

        # Filter the training df based on the predict df
        keys = list(df_to_predict.columns.values)
        i1 = training_df.set_index(keys).index
        i2 = df_to_predict.set_index(keys).index
        filtered_df = training_df[i1.isin(i2)]

        # Remove columns that didn't exist in the training df
        cols_to_drop = ["__mindsdb_row_id", args["target"]]
        for col in cols_to_drop:
            if col in filtered_df.columns.values.tolist():
                filtered_df = filtered_df.drop(col, axis=1)

        if filtered_df.empty:
            # TODO: Rephrase the exception message in a more user-friendly way
            raise Exception("The condition(s) in the WHERE clause filtered out all the data. Please refine these and try again")

        original_y = training_df[args["target"]]
        predictions = get_preds_from_autokeras_model(filtered_df, model, args["target"], args["data_column_names"])

        # If we used the classifier we need to pre-process the predictions before returning them
        if not np.issubdtype(original_y.dtype, np.number):
            # Turn prediction back into categorical value
            lb = preprocessing.LabelBinarizer()
            lb.fit(original_y)
            preds = lb.inverse_transform(predictions)

            # Add the confidence score next to the prediction
            filtered_df[args["target"]] = pd.Series(preds).astype(original_y.dtype)
            filtered_df["confidence"] = [max(row) for _, row in enumerate(predictions)]
            return filtered_df


        filtered_df[args["target"]] = predictions
        return filtered_df
