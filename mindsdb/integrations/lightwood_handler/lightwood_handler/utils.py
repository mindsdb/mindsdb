import gc
import sys
import dill

import pandas as pd

from mindsdb_sql.parser.ast import Identifier, Constant
from lightwood.api.high_level import _module_from_code


def unpack_jsonai_old_args(json_ai_override):
    while '.' in str(list(json_ai_override.keys())):
        for k in list(json_ai_override.keys()):
            if '.' in k:
                nks = k.split('.')
                obj = json_ai_override
                for nk in nks[:-1]:
                    if nk not in obj:
                        obj[nk] = {}
                    obj = obj[nk]
                obj[nks[-1]] = json_ai_override[k]
                del json_ai_override[k]


def _recur_get_conditionals(args: list, values):
    """Gets all the specified data from an arbitrary amount of AND clauses inside the WHERE statement"""  # noqa
    if isinstance(args[0], Identifier) and isinstance(args[1], Constant):
        values[args[0].parts[0]] = [args[1].value]
    else:
        for op in args:
            values = {**values, **_recur_get_conditionals([*op.args], {})}
    return values


def get_aliased_columns(aliased_columns, model_alias, targets, mode=None):
    """ This method assumes mdb_sql will alert if there are two columns with the same alias """
    for col in targets:
        if mode == 'input':
            if str(col.parts[0]) != model_alias and col.alias is not None:
                aliased_columns[aliased_columns.index(col.parts[-1])] = str(col.alias)

        if mode == 'output':
            if str(col.parts[0]) == model_alias and col.alias is not None:
                aliased_columns[aliased_columns.index('prediction')] = str(col.alias)

    return aliased_columns


def load_predictor(predictor_dict, name):
    try:
        module_name = None
        return dill.loads(predictor_dict['predictor'])
    except Exception as e:
        module_name = str(e).lstrip("No module named '").split("'")[0]

        try:
            del sys.modules[module_name]
        except Exception:
            pass

        gc.collect()
        _module_from_code(predictor_dict['code'], module_name)
        return dill.loads(predictor_dict['predictor'])


def default_train_data_gather(handler, query):
    records = handler.query(query)['data_frame']
    df = pd.DataFrame.from_records(records)
    return df
