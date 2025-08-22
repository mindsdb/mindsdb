from mindsdb_sql_parser.ast import Identifier, Function, Constant, BinaryOperation


def adapt_char_fn(node: Function) -> Function | None:
    """Replace MySQL's multy-arg CHAR call to chain of DuckDB's CHR calls

    Example:
        CHAR(77, 78, 79) => CHR(77) || CHR(78) || CHR(79)

    Args:
        node (Function): Function node to adapt

    Returns:
        Function | None: Adapted function node
    """
    if len(node.args) == 1:
        node.op = "chr"
        return node

    acc = None
    for arg in node.args:
        fn = Function(op="chr", args=[arg])
        if acc is None:
            acc = fn
            continue
        acc = BinaryOperation("||", args=[acc, fn])

    acc.parentheses = True
    acc.alias = node.alias
    return acc


def adapt_locate_fn(node: Function) -> Function | None:
    """Replace MySQL's LOCATE (or INSTR) call to DuckDB's STRPOS call

    Example:
        LOCATE('bar', 'foobarbar') => STRPOS('foobarbar', 'bar')
        INSTR('foobarbar', 'bar') => STRPOS('foobarbar', 'bar')
        LOCATE('bar', 'foobarbar', 3) => ValueError (there is no analogue in DuckDB)

    Args:
        node (Function): Function node to adapt

    Returns:
        Function | None: Adapted function node

    Raises:
        ValueError: If the function has 3 arguments
    """
    if len(node.args) == 3:
        raise ValueError("MySQL LOCATE function with 3 arguments is not supported")
    if node.op == "locate":
        node.args = [node.args[1], node.args[0]]
    elif node.op == "insrt":
        node.args = [node.args[0], node.args[1]]
    node.op = "strpos"


def adapt_unhex_fn(node: Function) -> None:
    """Check MySQL's UNHEX function call arguments to ensure they are strings,
    because DuckDB's UNHEX accepts only string arguments, while MySQL's UNHEX can accept integer arguments.
    NOTE: if return dataframe from duckdb then unhex values are array - this may be an issue

    Args:
        node (Function): Function node to adapt

    Returns:
        None

    Raises:
        ValueError: If the function argument is not a string
    """
    for arg in node.args:
        if not isinstance(arg, (str, bytes)):
            raise ValueError("MySQL UNHEX function argument must be a string")


def adapt_format_fn(node: Function) -> None:
    """Adapt MySQL's FORMAT function to DuckDB's FORMAT function

    Example:
        FORMAT(1234567.89, 0) => FORMAT('{:,.0f}', 1234567.89)
        FORMAT(1234567.89, 2) => FORMAT('{:,.2f}', 1234567.89)
        FORMAT(name, 2) => FORMAT('{:,.2f}', name)
        FORMAT('{:.2f}', 1234567.89) => FORMAT('{:,.2f}', 1234567.89)  # no changes for original style

    Args:
        node (Function): Function node to adapt

    Returns:
        None

    Raises:
        ValueError: If MySQL's function has 3rd 'locale' argument, like FORMAT(12332.2, 2, 'de_DE')
    """
    match node.args[0], node.args[1]:
        case Constant(value=(int() | float())), Constant(value=int()):
            ...
        case Identifier(), Constant(value=int()):
            ...
        case _:
            return node

    if len(node.args) > 2:
        raise ValueError("'locale' argument of 'format' function is not supported")
    decimal_places = node.args[1].value

    if isinstance(node.args[0], Constant):
        node.args[1].value = node.args[0].value
        node.args[0].value = f"{{:,.{decimal_places}f}}"
    else:
        node.args[1] = node.args[0]
        node.args[0] = Constant(f"{{:,.{decimal_places}f}}")


def adapt_sha2_fn(node: Function) -> None:
    """Adapt MySQL's SHA2 function to DuckDB's SHA256 function

    Example:
        SHA2('test', 256) => SHA256('test')

    Args:
        node (Function): Function node to adapt

    Returns:
        None

    Raises:
        ValueError: If the function has more than 1 argument or the argument is not 256
    """
    if len(node.args) > 1 and node.args[1].value != 256:
        raise ValueError("Only sha256 is supported")
    node.op = "sha256"
    node.args = [node.args[0]]


def adapt_length_fn(node: Function) -> None:
    """Adapt MySQL's LENGTH function to DuckDB's STRLEN function
    NOTE: duckdb also have LENGTH, therefore it can not be used

    Example:
        LENGTH('test') => STRLEN('test')

    Args:
        node (Function): Function node to adapt

    Returns:
        None
    """
    node.op = "strlen"


def adapt_regexp_substr_fn(node: Function) -> None:
    """Adapt MySQL's REGEXP_SUBSTR function to DuckDB's REGEXP_EXTRACT function

    Example:
        REGEXP_SUBSTR('foobarbar', 'bar', 1, 1) => REGEXP_EXTRACT('foobarbar', 'bar')

    Args:
        node (Function): Function node to adapt

    Returns:
        None

    Raises:
        ValueError: If the function has more than 2 arguments or 3rd or 4th argument is not 1
    """
    if (
        len(node.args) == 3
        and node.args[2].value != 1
        or len(node.args) == 4
        and (node.args[3].value != 1 or node.args[2].value != 1)
        or len(node.args) > 4
    ):
        raise ValueError("Only 2 arguments are supported for REGEXP_SUBSTR function")
    node.args = node.args[:2]
    node.op = "regexp_extract"


def adapt_substring_index_fn(node: Function) -> BinaryOperation | Function:
    """Adapt MySQL's SUBSTRING_INDEX function to DuckDB's SPLIT_PART function

    Example:
        SUBSTRING_INDEX('a.b.c.d', '.', 1) => SPLIT_PART('a.b.c.d', '.', 1)
        SUBSTRING_INDEX('a.b.c.d', '.', 2) => CONCAT_WS('.', SPLIT_PART('a.b.c.d', '.', 1), SPLIT_PART('a.b.c.d', '.', 2))

    Args:
        node (Function): Function node to adapt

    Returns:
        BinaryOperation | Function: Binary operation node or function node

    Raises:
        ValueError: If the function has more than 3 arguments or the 3rd argument is not 1
    """
    if len(node.args[1].value) > 1:
        raise ValueError("Only one car in separator")

    if node.args[2].value == 1:
        node.op = "split_part"
        return node

    acc = [node.args[1]]
    for i in range(node.args[2].value):
        fn = Function(op="split_part", args=[node.args[0], node.args[1], Constant(i + 1)])
        acc.append(fn)

    acc = Function(op="concat_ws", args=acc)
    acc.alias = node.alias
    return acc


def adapt_curtime_fn(node: Function) -> BinaryOperation:
    """Adapt MySQL's CURTIME function to DuckDB's GET_CURRENT_TIME function.
    To get the same type as MySQL's CURTIME function, we need to cast the result to time type.

    Example:
        CURTIME() => GET_CURRENT_TIME()::time

    Args:
        node (Function): Function node to adapt

    Returns:
        BinaryOperation: Binary operation node
    """
    return BinaryOperation("::", args=[Function(op="get_current_time", args=[]), Identifier("time")], alias=node.alias)


def adapt_timestampdiff_fn(node: Function) -> None:
    """Adapt MySQL's TIMESTAMPDIFF function to DuckDB's DATE_DIFF function
    NOTE: Looks like cast string args to timestamp works in most cases, but there may be some exceptions.

    Example:
        TIMESTAMPDIFF(YEAR, '2000-02-01', '2003-05-01') => DATE_DIFF('year', timestamp '2000-02-01', timestamp '2003-05-01')

    Args:
        node (Function): Function node to adapt

    Returns:
        None
    """
    node.op = "date_diff"
    node.args[0] = Constant(node.args[0].parts[0])
    node.args[1] = BinaryOperation(" ", args=[Identifier("timestamp"), node.args[1]])
    node.args[2] = BinaryOperation(" ", args=[Identifier("timestamp"), node.args[2]])


def adapt_extract_fn(node: Function) -> None:
    """Adapt MySQL's EXTRACT function to DuckDB's EXTRACT function
    TODO: multi-part args, like YEAR_MONTH, is not supported yet
    NOTE: Looks like adding 'timestamp' works in most cases, but there may be some exceptions.

    Example:
        EXTRACT(YEAR FROM '2000-02-01') => EXTRACT('year' from timestamp '2000-02-01')

    Args:
        node (Function): Function node to adapt

    Returns:
        None
    """
    node.args[0] = Constant(node.args[0].parts[0])
    if not isinstance(node.from_arg, Identifier):
        node.from_arg = BinaryOperation(" ", args=[Identifier("timestamp"), node.from_arg])
