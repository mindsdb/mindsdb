def get_column_in_case(columns, name):
    '''
    '''
    candidates = []
    name_lower = name.lower()
    for column in columns:
        if column.lower() == name_lower:
            candidates.append(column)
    if len(candidates) != 1:
        raise Exception(f'Cant get appropriate cast column case. Columns: {columns}, column: {name}, candidates: {candidates}')
    return candidates[0]
