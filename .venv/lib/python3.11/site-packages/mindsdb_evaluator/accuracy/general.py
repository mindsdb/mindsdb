import importlib
from typing import List, Dict, Optional, Union

import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder

from mindsdb_evaluator.helpers.general import Module, filter_fn_args
from mindsdb_evaluator.accuracy.forecasting import \
    evaluate_array_accuracy, \
    evaluate_num_array_accuracy, \
    evaluate_cat_array_accuracy, \
    complementary_smape_array_accuracy


SCORE_TYPES = (float, np.float16, np.float32, np.float64,
               int, np.int8, np.int16, np.int32, np.int64)

if hasattr(np, 'float128'):
    SCORE_TYPES += (np.float128,)


def evaluate_accuracy(data: pd.DataFrame,
                      predictions: pd.Series,
                      accuracy_function: str,
                      target: Optional[str] = None,
                      ts_analysis: Optional[dict] = {},
                      n_decimals: Optional[int] = 3,
                      fn_kwargs: Optional[dict] = {}) -> float:
    """
    Dispatcher for accuracy evaluation.

    :param data: original dataframe.
    :param predictions: output of a machine learning model for the input `data`.
    :param target: target column name.
    :param accuracy_function: either a metric from the `accuracy` module or `scikit-learn.metric`.
    :param ts_analysis: `lightwood.data.timeseries_analyzer` output, used to compute time series task accuracy.
    :param n_decimals: used to round accuracies.
    :param fn_kwargs: additional arguments to be passed to the accuracy function.
    
    :return: accuracy score, given input data and model predictions.
    """  # noqa
    if 'array_accuracy' in accuracy_function or accuracy_function in ('bounded_ts_accuracy',):
        if ts_analysis is None or not ts_analysis.get('tss', False) or not ts_analysis['tss'].is_timeseries:
            # normal array, needs to be expanded
            cols = [target]
            y_true = data[cols].apply(lambda x: pd.Series(x[target]), axis=1)
        else:
            horizon = 1 if not isinstance(predictions.iloc[0], list) else len(predictions.iloc[0])
            gby = ts_analysis.get('tss', {}).group_by if ts_analysis.get('tss', {}).group_by else []
            cols = [target] + [f'{target}_timestep_{i}' for i in range(1, horizon)] + gby
            y_true = data[cols]

        y_true = y_true.reset_index(drop=True)
        y_pred = predictions.apply(pd.Series).reset_index(drop=True)  # split array cells into columns

        if accuracy_function == 'evaluate_array_accuracy':
            acc_fn = evaluate_array_accuracy
        elif accuracy_function == 'evaluate_num_array_accuracy':
            acc_fn = evaluate_num_array_accuracy
        elif accuracy_function == 'evaluate_cat_array_accuracy':
            acc_fn = evaluate_cat_array_accuracy
        elif accuracy_function == 'complementary_smape_array_accuracy':
            acc_fn = complementary_smape_array_accuracy
        else:
            raise Exception(f"Could not retrieve accuracy function: {accuracy_function}")

        fn_kwargs['data'] = data[cols]
        fn_kwargs['ts_analysis'] = ts_analysis
        fn_kwargs = filter_fn_args(acc_fn, fn_kwargs)
        score = acc_fn(y_true, y_pred, **fn_kwargs)
    else:
        y_true = data[target].tolist()
        y_pred = list(predictions)
        if hasattr(importlib.import_module('mindsdb_evaluator.accuracy'), accuracy_function):
            accuracy_function = getattr(importlib.import_module('mindsdb_evaluator.accuracy'), accuracy_function)
        else:
            raise Exception(f"Could not retrieve accuracy function: {accuracy_function}")

        try:
            fn_kwargs = filter_fn_args(accuracy_function, fn_kwargs)
            score = accuracy_function(y_true, y_pred, **fn_kwargs)
            assert type(score) in SCORE_TYPES, f"Accuracy function `{accuracy_function.__name__}` returned invalid type {type(score)}"  # noqa
        except ValueError as e:
            if 'mix of label input' in str(e).lower():
                # mixed types, try to convert to string. note: shouldn't happen anymore when labels are passed
                fn_kwargs = filter_fn_args(accuracy_function, fn_kwargs)
                score = accuracy_function([str(y) for y in y_true],
                                          [str(y) for y in y_pred],
                                          **fn_kwargs)
            else:
                raise e

    return round(score, n_decimals)


def evaluate_accuracies(data: pd.DataFrame,
                        predictions: pd.Series,
                        target: str,
                        accuracy_functions: List[Union[str, Module]],
                        ts_analysis: Optional[dict] = {},
                        labels: Optional[list] = [],
                        n_decimals: Optional[int] = 3) -> Dict[str, float]:
    # if a label list is provided, we need to encode the target and predictions accordingly
    if labels:
        le = LabelEncoder()
        le.fit(labels)
        data[target] = le.transform(data[target])
        predictions = le.transform(predictions)

    score_dict = {}
    for accuracy_function in accuracy_functions:
        fn_kwargs = {}
        if not isinstance(accuracy_function, str):
            fn_kwargs = accuracy_function['args']
            accuracy_function = accuracy_function['module']
        score = evaluate_accuracy(data,
                                  predictions,
                                  accuracy_function,
                                  target=target,
                                  ts_analysis=ts_analysis,
                                  n_decimals=n_decimals,
                                  fn_kwargs=fn_kwargs,
                                  )
        score_dict[accuracy_function] = score

    return score_dict
