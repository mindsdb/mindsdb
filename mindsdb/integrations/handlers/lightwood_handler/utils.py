import gc
import json
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


def rep_recur(org: dict, ovr: dict):
    for k in ovr:
        if k in org:
            if isinstance(org[k], dict) and isinstance(ovr[k], dict):
                rep_recur(org[k], ovr[k])
            else:
                org[k] = ovr[k]
        else:
            org[k] = ovr[k]


def brack_to_mod(ovr):
    if not isinstance(ovr, dict):
        if isinstance(ovr, list):
            for i in range(len(ovr)):
                ovr[i] = brack_to_mod(ovr[i])
        elif isinstance(ovr, str):
            if '(' in ovr and ')' in ovr:
                mod = ovr.split('(')[0]
                args = {}
                if '()' not in ovr:
                    for str_pair in ovr.split('(')[1].split(')')[0].split(','):
                        k = str_pair.split('=')[0].strip(' ')
                        v = str_pair.split('=')[1].strip(' ')
                        args[k] = v

                ovr = {'module': mod, 'args': args}
            elif '{' in ovr and '}' in ovr:
                try:
                    ovr = json.loads(ovr)
                except Exception:
                    pass
        return ovr
    else:
        for k in ovr.keys():
            ovr[k] = brack_to_mod(ovr[k])

    return ovr
