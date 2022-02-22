def get_column_in_case(columns, name):
    '''
    '''
    candidates = []
    name_lower = name.lower()
    for column in columns:
        if column.lower() == name_lower:
            candidates.append(column)
    if len(candidates) != 1:
        return None
    return candidates[0]
