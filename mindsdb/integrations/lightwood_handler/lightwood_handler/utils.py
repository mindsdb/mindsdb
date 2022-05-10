

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