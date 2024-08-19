import re
from copy import deepcopy
from typing import Dict, List, Optional, Tuple, Callable, Union

import numpy as np
import pandas as pd

from type_infer.dtype import dtype
from type_infer.helpers import is_nan_numeric, clean_float

from dataprep_ml.helpers import log
from dataprep_ml.imputers import BaseImputer


def cleaner(
        data: pd.DataFrame,
        dtype_dict: Dict[str, str],
        pct_invalid: float,
        target: str,  # TODO: turn into optional (requires logic changes). pass inside a normal dict.
        timeseries_settings: Dict,  # TODO: move TS logic into separate cleaner and call sequentially from lw
        anomaly_detection: bool,  # TODO: pass inside a dict?
        mode: Optional[str] = 'train',  # TODO: pass inside a dict? add unit tests for missing param
        identifiers: Optional[Dict[str, str]] = None,  # TODO: add unit test for no identifiers provided
        imputers: Dict[str, BaseImputer] = {},  # TODO: pass inside a normal dict
        custom_cleaning_functions: Dict[str, str] = {}
) -> pd.DataFrame:
    """
    The cleaner is a function which takes in the raw data, plus additional information about it's types and about the problem. Based on this it generates a "clean" representation of the data, where each column has an ideal standardized type and all malformed or otherwise missing or invalid elements are turned into ``None``. Optionally, these ``None`` values can be replaced with imputers.

    :param data: The raw data
    :param dtype_dict: Type information for each column
    :param pct_invalid: How much of each column can be invalid
    :param identifiers: A dict containing all identifier typed columns
    :param target: The target columns
    :param mode: Can be "predict" or "train"
    :param imputers: The key corresponds to the single input column that will be imputed by the object. Refer to the imputer documentation for more details.
    :param timeseries_settings: Timeseries related settings, only relevant for timeseries predictors, otherwise can be the default object
    :param anomaly_detection: Are we detecting anomalies with this predictor?

    :returns: The cleaned data
    """  # noqa
    data = _remove_columns(data, identifiers, target, mode, timeseries_settings,
                           anomaly_detection, dtype_dict)

    _timeseries_edge_case_detection(data, timeseries_settings)  # raise assertion errors for edge cases

    data['__mdb_original_index'] = np.arange(len(data))

    for col in _get_columns_to_clean(data, dtype_dict, mode, target):
        # Get and apply a cleaning function for each data type
        # If you want to customize the cleaner, it's likely you can to modify ``get_cleaning_func``
        fn, vec = get_cleaning_func(dtype_dict[col], custom_cleaning_functions)
        if not vec:
            data[col] = data[col].apply(fn)
        if vec:
            data[col] = fn(data[col])

    if timeseries_settings.get('is_timeseries', False):
        data = clean_timeseries(data, timeseries_settings)

    for col, imputer in imputers.items():
        if col in data.columns:
            cols = [col] + [col for col in imputer.dependencies]
            data[col] = imputer.impute(data[cols])

    return data


def _check_if_invalid(new_data: pd.Series, pct_invalid: float, col_name: str):
    """
    Checks how many invalid data points there are. Invalid data points are flagged as "Nones" from the cleaning processs (see data/cleaner.py for default).
    If there are too many invalid data points (specified by `pct_invalid`), then an error message will pop up. This is used as a safeguard for very messy data.

    :param new_data: data to check for invalid values.
    :param pct_invalid: maximum percentage of invalid values. If this threshold is surpassed, an exception is raised.
    :param col_name: name of the column to analyze.

    """  # noqa

    chk_invalid = (
        100 * (len(new_data) - len([x for x in new_data if x is not None])) / len(new_data)
    )

    if chk_invalid > pct_invalid:
        err = f'Too many ({chk_invalid}%) invalid values in column {col_name}nam'
        log.error(err)
        raise Exception(err)


def get_cleaning_func(data_dtype: dtype, custom_cleaning_functions: Dict[str, str]) -> Tuple[Callable, bool]:
    """
    For the provided data type, provide the appropriate cleaning function. Below are the defaults, users can either override this function OR impose a custom block.

    :param data_dtype: The data-type (inferred from a column) as prescribed from ``api.dtype``

    :returns: A 2-tuple.
        0: The appropriate function that will pre-process (clean) data of specified dtype.
        1: Whether the function is "vectorized": applied per item (False), or over entire column at once (True).
    """  # noqa
    vec = False

    if data_dtype in custom_cleaning_functions:
        clean_func = eval(custom_cleaning_functions[data_dtype])

    elif data_dtype in (dtype.date, dtype.datetime):
        clean_func = _standardize_datetime

    elif data_dtype in (dtype.float, dtype.num_tsarray):
        clean_func = _clean_float

    elif data_dtype in (dtype.integer):
        clean_func = _clean_int

    elif data_dtype in (dtype.num_array):
        clean_func = _standardize_num_array

    elif data_dtype in (dtype.cat_array):
        clean_func = _standardize_cat_array

    elif data_dtype in (dtype.tags):
        clean_func = _tags_to_tuples

    elif data_dtype in (dtype.quantity):
        clean_func = _clean_quantity

    elif data_dtype in (
            dtype.short_text,
            dtype.rich_text,
            dtype.categorical,
            dtype.binary,
            dtype.audio,
            dtype.image,
            dtype.video,
            dtype.cat_tsarray
    ):
        clean_func = _clean_text

    else:
        raise ValueError(f"{data_dtype} is not supported. Check lightwood.api.dtype")

    # vectorized function lookup
    vec = clean_func in (_clean_int, _clean_float, _standardize_datetime, _clean_quantity)

    return clean_func, vec


# ------------------------- #
# Temporal Cleaning
# ------------------------- #

def _standardize_datetime(element: pd.Series) -> pd.Series:
    """ Converts pandas.Series into pandas.Timestamp Series.

        :param element: pandas.Series
        :returns pandas.Series, None

        :note
            Returns `None` if the routine fails to extract
            a `pandas.Timestamp` from any entry of `element`.
    """
    result = None
    try:
        result = pd.to_datetime(element,
                                infer_datetime_format=True,
                                format='mixed').apply(lambda x: x.timestamp())
    except ValueError:
        pass

    return result

# ------------------------- #
# Tags/Sequences
# ------------------------- #


# TODO Make it split on something other than commas
def _tags_to_tuples(tags_str: str) -> Tuple[str]:
    """
    Converts comma-separated values into a tuple to preserve a sequence/array.

    Ex:
    >> x = 'apples, oranges, bananas'
    >> _tags_to_tuples(x)
    >> ('apples', 'oranges', 'bananas')
    """
    try:
        return tuple([x.strip() for x in tags_str.split(",")])
    except Exception:
        return tuple()


def _standardize_num_array(element: object) -> Optional[Union[List[float], float]]:
    """
    Given an array of numbers in the form ``[1, 2, 3, 4]``, converts into a numerical sequence.

    :param element: An array-like element in a sequence
    :returns: standardized array OR scalar number IF edge case

    Ex of edge case:
    >> element = [1]
    >> _standardize_num_array(element)
    >> 1
    """
    try:
        element = str(element)
        element = element.rstrip("]").lstrip("[")
        element = element.rstrip(" ").lstrip(" ")
        element = element.replace(", ", " ").replace(",", " ")
        # Handles cases where arrays are numbers
        if " " not in element:
            element = _clean_float(element)
        else:
            element = [float(x) for x in element.split(" ")]
    except Exception:
        pass

    return element


def _standardize_cat_array(element: List) -> Optional[List[str]]:
    """
    Given an array, replace non-string with string casted (or, failing that, None) tokens. None values are kept.

    :param element: An array-like element in a sequence
    :returns: standardized array OR label IF edge case

    Ex of edge case:
    >> element = ['a']
    >> _standardize_cat_array(element)
    >> 'a'
    """
    if len(element) == 1:
        return element[0]
    else:
        new_element = []
        for sub_elt in element:
            try:
                new_sub_elt = str(sub_elt) if sub_elt else None
                new_element.append(new_sub_elt)
            except TypeError:
                new_element.append(None)
        element = new_element

    return element


# ------------------------- #
# Integers/Floats/Quantities
# ------------------------- #

def _clean_float(element: pd.Series) -> pd.Series:
    def _clean(x: object):
        cleaned_float = clean_float(x)
        return cleaned_float if not is_nan_numeric(cleaned_float) else None

    return element.apply(_clean)


def _clean_int(element: pd.Series) -> pd.Series:
    """
    Given a series, converts it into integer numeric format. If element is NaN, or inf, then returns None.
    """
    ints = pd.to_numeric(_clean_float(element), errors='coerce')
    ints = ints.replace([np.inf, -np.inf], np.nan).fillna(np.nan)
    return ints


def _clean_quantity(element: pd.Series) -> pd.Series:
    """
    Given a quantity, clean and convert it into float numeric format. If element is NaN, or inf, then returns None.
    """

    def _no_symbols(elt):
        no_symbols = re.sub("[^0-9.,]", "", str(elt)).replace(",", ".")
        no_symbols = '0' if no_symbols == '' else no_symbols
        return float(no_symbols)

    element = element.apply(_no_symbols)
    return _clean_float(element)


# ------------------------- #
# Text
# ------------------------- #
def _clean_text(element: object) -> Union[str, None]:
    if isinstance(element, str):
        return element
    elif element is None or element != element:
        return None
    else:
        return str(element)


# ------------------------- #
# Other helpers
# ------------------------- #
def _rm_rows_w_empty_targets(df: pd.DataFrame, target: str) -> pd.DataFrame:
    """
    Drop any rows that have targets as unknown. Targets are necessary to train.

    :param df: The input dataframe including the target value
    :param target: the column name that is the output target variable

    :returns: Data with any target smissing
    """
    # Compare length before/after
    len_before = len(df)

    # Use Pandas ```dropna``` to omit any rows with missing values for targets; these cannot be trained
    df = df.dropna(subset=[target])

    # Compare length with after
    len_after = len(df)
    nr_removed = len_before - len_after

    if nr_removed != 0:
        log.warning(
            f"Removed {nr_removed} rows because target was missing. Training on these rows is not possible."
        )  # noqa

    return df


def _timeseries_edge_case_detection(data: pd.DataFrame, timeseries_settings: Dict):
    """
    Detect timeseries edge cases and raise assertion errors:

    1) if window size is greater than dataset length

    :param data: The raw data
    :param timeseries_settings: Timeseries related settings, only relevant for timeseries predictors, otherwise can be
    the default object

    :returns: None

    """

    if timeseries_settings.get('window', None) is not None:
        window = timeseries_settings.get('window', 0)
        if len(data) < window:
            log.info("Window size is greater than input data length: strongly recommend to increase input data length.")

    return


def _remove_columns(data: pd.DataFrame, identifiers: Dict[str, object], target: str,
                    mode: str, timeseries_settings: Dict, anomaly_detection: bool,
                    dtype_dict: Dict[str, dtype]) -> pd.DataFrame:
    """
    Drop columns we don't want to use in order to train or predict

    :param data: The raw data
    :param dtype_dict: Type information for each column
    :param identifiers: A dict containing all identifier typed columns
    :param target: The target columns
    :param mode: Can be "predict" or "train"
    :param timeseries_settings: Timeseries related settings, only relevant for timeseries predictors, otherwise can be 
    the default object
    :param anomaly_detection: Are we detecting anomalies with this predictor?

    :returns: A (new) dataframe without the dropped columns
    """  # noqa

    data = deepcopy(data)
    to_drop = [*[x for x in identifiers.keys() if x != target],
               *[x for x in data.columns if x in dtype_dict and dtype_dict[x] == dtype.invalid]]

    exceptions = ["__mdb_forecast_offset"]
    if timeseries_settings.get('group_by', None) is not None:
        exceptions += timeseries_settings.get('group_by', [])

    to_drop = [x for x in to_drop if x in data.columns and x not in exceptions]

    if to_drop:
        log.info(f'Dropping features: {to_drop}')
        data = data.drop(columns=to_drop)

    if mode == "train":
        data = _rm_rows_w_empty_targets(data, target)
    if mode == "predict":
        if (
                target in data.columns
                and (not timeseries_settings.get('is_timeseries', False) or
                     not timeseries_settings.get('use_previous_target', False))
                and not anomaly_detection
        ):
            data = data.drop(columns=[target])

    # Drop extra columns
    for name in list(data.columns):
        if name not in dtype_dict and name not in exceptions:
            data = data.drop(columns=[name])
    return data


def _get_columns_to_clean(data: pd.DataFrame, dtype_dict: Dict[str, dtype], mode: str, target: str) -> List[str]:
    """
    :param data: The raw data
    :param dtype_dict: Type information for each column
    :param target: The target columns
    :param mode: Can be "predict" or "train"

    :returns: A list of columns that we want to clean
    """  # noqa

    cleanable_columns = []
    for name, _ in dtype_dict.items():
        if mode == "predict":
            if name == target:
                continue
        if name in data.columns:
            cleanable_columns.append(name)
    return cleanable_columns


def _fix_duplicates(df: pd.DataFrame, tss: dict) -> pd.DataFrame:
    """ Removes duplicate timestamps from DataFrame.

        :param df: DataFrame with input data.
        :param order_by: name of column to order DataFrame
                         i.e. the timestamp column.

        Groups of duplicates with numerical data are averaged
        to form a single entry, while for groups of duplicates
        with non-numerical data the duplicates are discarded and
        only the first occurence is kept.
    """
    # build mask with duplicated timestamps
    df = df.reset_index(drop=True)
    dup_ts_mask = df[tss['order_by']].duplicated(keep='first')
    # return if no duplicated indices are found
    if dup_ts_mask.sum() == 0:
        return df

    # build groups of duplicated indices
    dup_idx_groups = []
    curr_group = []
    for j, dup_idx in enumerate(df.index[:-1]):
        # check if current and next items are duplicated
        is_curr_dup = dup_ts_mask.values[j]
        is_next_dup = dup_ts_mask.values[j + 1]
        # current and next item are not duplicates
        if (not is_curr_dup) and (not is_next_dup):
            continue
        # next item is marked as duplicated
        # -> creates a new group and adds item
        if not is_curr_dup and is_next_dup:
            curr_group = []
            curr_group.append(dup_idx)
        # current and next item are marked as duplicated
        # -> adds and item
        if is_curr_dup and is_next_dup:
            curr_group.append(dup_idx)
        # current item is marked as duplicated and next
        # item is not, or reached end of data
        # -> add item and close group
        if (is_curr_dup and (not is_next_dup)) or (j == len(df) - 1):
            curr_group.append(dup_idx)
            g = np.asarray(curr_group)
            dup_idx_groups.append(g)
    # average numerical columns in groups
    # keep the first value for non-numerical columns
    avg_groups = []
    for grp in dup_idx_groups:
        avg_num_row = df.loc[grp].mean(axis=0, numeric_only=True)
        num_data_float = pd.DataFrame(avg_num_row).transpose()
        row = None
        # respect original types
        num_data = pd.DataFrame()
        num_cols = list(num_data_float.columns)
        for nc in num_cols:
            col_dtype = df.dtypes[nc]
            num_data[nc] = num_data_float[nc].values.astype(col_dtype)
        num_data = num_data.reset_index(drop=True)
        # handle case where all columns are numeric
        if num_data.shape[1] == len(df.columns):
            row = num_data
        else:
            non_num_row = df.loc[grp].iloc[0].drop(num_cols)
            non_num_data = pd.DataFrame(non_num_row).transpose()
            non_num_data = non_num_data.reset_index(drop=True)
            row = num_data.join(non_num_data, how='right')
        avg_groups.append(row)
    dedup = pd.concat(avg_groups + [df.loc[~dup_ts_mask], ], axis=0)
    mask = ~dedup[tss['order_by']].duplicated(keep='first')
    dedup = dedup.loc[mask]
    dedup = dedup.sort_values(by=tss['order_by'])

    return dedup


def clean_timeseries(df: pd.DataFrame, tss: dict) -> pd.DataFrame:
    """
        All timeseries-specific cleaning logic goes here. Currently:
            1) Any row with `nan`-valued order-by measurements is dropped.
            2) Rows with duplicated time-stamps are trated the following way:
               - columns that are numerical are averaged
               - for non-numerical columns, only the first duplicate is kept.

        :param df: data.
        :param tss: timeseries settings
        :return: cleaned data.
    """
    # locate and drop rows with invalid (None, Nan, NaT) timestamps
    invalid_rows = df[df[tss['order_by']].isna()].index
    df = df.drop(invalid_rows)

    # save original order of columns
    orig_cols = deepcopy(df.columns.to_list())

    # cast order_by as numerical
    df[tss['order_by']] = pd.to_numeric(df[tss['order_by']], errors='raise')

    # fix duplicates by group
    if tss.get('group_by', False):
        correct_dfs = []
        grps = df.groupby(by=tss['group_by'])
        for _, g in grps:
            correct_dfs += [_fix_duplicates(g, tss)]
        df = pd.concat(correct_dfs)
    else:
        df = _fix_duplicates(df, tss)
    df = df.reset_index(drop=True)
    df = df.reindex(orig_cols, axis=1)

    return df
