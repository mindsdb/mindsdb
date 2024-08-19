from copy import deepcopy
from typing import List

import pandas as pd

from dataprep_ml.helpers import log


class BaseImputer:
    def __init__(self, target: str, dependencies: List[str] = []):
        """
        Lightwood imputers will fill in missing values in any given column.

        The single method to implement, `impute`, is where the logic for filling in missing values has to be specified.

        Note that if access to other columns is required, this can be specified with the `dependencies` parameter.

        Also note, by default some Lightwood encoders (e.g. categorical) are able to cope with missing data, so having imputers as a general rule is not required but can be a nice to have.

        :param target: Column that the imputer will modify.
        :param dependencies: Any additional columns (other than the target) that will be needed inside `impute()`.
        """  # noqa
        self.target = target
        self.dependencies = dependencies

    def impute(self, data: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("Please implement your custom imputing logic in a module that inherits this class.")


class NumericalImputer(BaseImputer):
    def __init__(self, target: str, dependencies: List[str] = [], value: str = 'zero', typecast: bool = True):
        """
        Imputer for numerical columns. Supports a handful of different approaches to define the imputation value.

        :param value: The value to impute. One of 'mean', 'median', 'zero', 'mode'.
        :param typecast: Used to cast the column into 'float' dtype.
        """  # noqa
        self.value = value
        self.typecast = typecast
        super().__init__(target, dependencies)

    def impute(self, data: pd.DataFrame) -> pd.DataFrame:
        data = deepcopy(data)
        col = self.target

        if data[col].dtype not in (int, float):
            if self.typecast:
                try:
                    data[col] = data[col].astype(float)
                except ValueError:
                    log.warning(f'Numerical imputer failed to cast column {col} to float!')
            else:
                log.warning(f'Numerical imputer used in non-numeric column {col} with dtype {data[col].dtype}!')

        if self.value == 'mean':
            value = data[col].dropna().mean()
        elif self.value == 'median':
            value = data[col].dropna().median()
        elif self.value == 'mode':
            value = data[col].dropna().mode().iloc[0]  # if there's a tie, this chooses the smallest value
        else:
            value = 0.0

        data[col] = data[col].fillna(value=value)

        return data


class CategoricalImputer(BaseImputer):
    def __init__(self, target: str, dependencies: List[str] = [], value: str = 'mode'):
        """
        Imputer for categorical columns.

        :param value: Type of imputation. Currently, only `mode` is supported, and replaces missing data with the observed mode.
        """  # noqa
        self.value = value
        super().__init__(target, dependencies)

    def impute(self, data: pd.DataFrame) -> pd.DataFrame:
        data = deepcopy(data)
        col = self.target

        if self.value == 'mode':
            value = data[col].dropna().mode().iloc[0]
            data[col] = data[col].fillna(value=value)

        return data
