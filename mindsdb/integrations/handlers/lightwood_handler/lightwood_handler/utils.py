import gc
import sys
import dill

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
