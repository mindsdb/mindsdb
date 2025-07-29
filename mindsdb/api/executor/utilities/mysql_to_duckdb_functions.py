from mindsdb_sql_parser.ast import ASTNode, Select, Identifier, Function, Constant, BinaryOperation


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
        node.op = 'chr'
        return node

    acc = None
    for arg in node.args:
        fn = Function(op='chr', args=[arg])
        if acc is None:
            acc = fn
            continue
        acc = BinaryOperation('||', args=[acc, fn])
    
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
        raise ValueError('MySQL LOCATE function with 3 arguments is not supported')
    if node.op == 'locate':
        node.args = [node.args[1], node.args[0]]
    elif node.op == 'insrt':
        node.args = [node.args[0], node.args[1]]
    node.op = 'strpos'

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
            raise ValueError('MySQL UNHEX function argument must be a string')

def adapt_format_fn(node: Function) -> Function | None:
    """Adapt MySQL's FORMAT function to DuckDB's FORMAT function

    Example:
        FORMAT(1234567.89, 0) => FORMAT('{:,.0f}', 1234567.89)
        FORMAT(1234567.89, 2) => FORMAT('{:,.2f}', 1234567.89)
        FORMAT('{:.2f}', 1234567.89) => FORMAT('{:,.2f}', 1234567.89)  # no changes for original style

    Args:
        node (Function): Function node to adapt

    Returns:
        Function | None: Adapted function node

    Raises:
        ValueError: If MySQL's function has 3rd 'locale' argument, like FORMAT(12332.2, 2, 'de_DE')
    """
    if (
        not isinstance(node.args[0], Constant) or not isinstance(node.args[0].value, (int, float))
        or not isinstance(node.args[1], Constant) or not isinstance(node.args[1].value, int)
    ):
        return node
    if len(node.args) > 2:
        raise ValueError("'locale' argument of 'format' function is not supported")
    decimal_places = node.args[1].value
    node.args[1].value = node.args[0].value
    node.args[0].value = f'{{:,.{decimal_places}f}}'

def adapt_sha2_fn(node: Function) -> Function | None:
    """Adapt MySQL's SHA2 function to DuckDB's SHA256 function

    Example:
        SHA2('test', 256) => SHA256('test')

    Args:
        node (Function): Function node to adapt

    Returns:
        Function | None: Adapted function node

    Raises:
        ValueError: If the function has more than 1 argument or the argument is not 256
    """
    if len(node.args) > 1 and node.args[1].value != 256:
        raise ValueError("Only sha256 is supported")
    node.op = "sha256"
    node.args = [node.args[0]]
