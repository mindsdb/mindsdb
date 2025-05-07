def _is_in_quotes(pos: int, quote_positions: list[tuple[int, int]]) -> bool:
    """
    Check if a position is within any quoted string.

    Args:
        pos (int): The position to check.
        quote_positions (list[tuple[int, int]]): A list of tuples, each containing the start and
                                                 end positions of a quoted string.

    Returns:
        bool: True if the position is within any quoted string, False otherwise.
    """
    return any(start < pos < end for start, end in quote_positions)


def clear_sql(sql: str) -> str:
    '''Remove comments (--, /**/, and oracle-stype #) and trailing ';' from sql
    Note: written mostly by LLM

    Args:
        sql (str): The SQL query to clear.

    Returns:
        str: The cleared SQL query.
    '''
    if sql is None:
        raise ValueError('sql query is None')

    # positions of (', ", `)
    quote_positions = []
    for quote_char in ["'", '"', '`']:
        i = 0
        while i < len(sql):
            if sql[i] == quote_char and (i == 0 or sql[i - 1] != '\\'):
                start = i
                i += 1
                while i < len(sql) and (sql[i] != quote_char or sql[i - 1] == '\\'):
                    i += 1
                if i < len(sql):
                    quote_positions.append((start, i))
            i += 1

    # del /* */ comments
    result = []
    i = 0
    while i < len(sql):
        if i + 1 < len(sql) and sql[i:i + 2] == '/*' and not _is_in_quotes(i, quote_positions):
            # skip until */
            i += 2
            while i + 1 < len(sql) and sql[i:i + 2] != '*/':
                i += 1
            if i + 1 < len(sql):
                i += 2  # skip */
            else:
                i += 1
        else:
            result.append(sql[i])
            i += 1

    sql = ''.join(result)

    # del -- and # comments
    result = []
    i = 0
    while i < len(sql):
        if i + 1 < len(sql) and sql[i:i + 2] == '--' and not _is_in_quotes(i, quote_positions):
            while i < len(sql) and sql[i] != '\n':
                i += 1
        elif sql[i] == '#' and not _is_in_quotes(i, quote_positions):
            while i < len(sql) and sql[i] != '\n':
                i += 1
        else:
            result.append(sql[i])
            i += 1

    sql = ''.join(result)

    # del ; at the end
    sql = sql.rstrip()
    if sql and sql[-1] == ';':
        sql = sql[:-1].rstrip()

    return sql.strip(' \n\t')
