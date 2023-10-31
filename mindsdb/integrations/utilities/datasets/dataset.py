from pathlib import Path

import pandas as pd

DATASETS_BASE_PATH = Path(__file__).parent

SUPPORTED_TASK_TYPES = ("question_answering",)


class DatasetNameMissing(Exception):
    pass


class MLTaskTypeMissing(Exception):
    pass


class DatasetNotFound(Exception):
    pass


class UnsupportedMLTaskType(Exception):
    pass


class MissingColumns(Exception):
    pass


def validate_dataset(ml_task_type=None, dataset_name=None):

    if ml_task_type is None:
        raise MLTaskTypeMissing(
            "ML Task type is missing. Please provide a valid 'ml_task_type'."
        )

    if dataset_name is None:
        raise DatasetNameMissing(
            "Dataset name is missing. Please provide a valid 'dataset_name'."
        )

    if ml_task_type not in SUPPORTED_TASK_TYPES:
        raise UnsupportedMLTaskType(
            f"ML Task type '{ml_task_type}' is not supported. Supported types are: {SUPPORTED_TASK_TYPES}"
        )

    dataset_path = DATASETS_BASE_PATH / ml_task_type / f"{dataset_name}.csv"

    if not dataset_path.exists():
        raise DatasetNotFound(
            f"Dataset '{dataset_name}' for ML Task type '{ml_task_type}' not found '{dataset_path}'."
        )

    return dataset_path


def load_dataset(ml_task_type=None, dataset_name=None):

    dataset_path = validate_dataset(ml_task_type, dataset_name)

    return pd.read_csv(dataset_path)


def validate_dataframe(df, mandatory_columns):

    columns_exist = all([col in df.columns for col in mandatory_columns])

    if not columns_exist:
        raise MissingColumns(
            f"Columns {mandatory_columns} are missing from the dataframe."
        )

    return df
