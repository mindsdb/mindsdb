import re
from mindsdb_sql_parser.ast import Identifier, Function, Constant, BinaryOperation, Interval, ASTNode, UnaryOperation


# ---- helper -----


def cast(node: ASTNode, typename: str) -> BinaryOperation:
    return BinaryOperation("::", args=[node, Identifier(typename)])


def date_part(node, part):
    """
    Wrap element into DATE_PART function

    Docs:
        https://duckdb.org/docs/stable/sql/functions/date#date_partpart-date
    """
    node.args = apply_nested_functions(node.args)

    if len(node.args) != 1:
        raise ValueError(f"Wrong arguments: {node.args}")

    return Function("DATE_PART", args=[Constant(part), cast(node.args[0], "date")])


# ------------------------------


def char_fn(node: Function) -> Function | None:
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


def locate_fn(node: Function) -> Function | None:
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


def unhex_fn(node: Function) -> None:
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


def format_fn(node: Function) -> None:
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


def sha2_fn(node: Function) -> None:
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


def length_fn(node: Function) -> None:
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


def regexp_substr_fn(node: Function) -> None:
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


def substring_index_fn(node: Function) -> BinaryOperation | Function:
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


def curtime_fn(node: Function) -> BinaryOperation:
    """Adapt MySQL's CURTIME function to DuckDB's GET_CURRENT_TIME function.
    To get the same type as MySQL's CURTIME function, we need to cast the result to time type.

    Example:
        CURTIME() => GET_CURRENT_TIME()::time

    Args:
        node (Function): Function node to adapt

    Returns:
        BinaryOperation: Binary operation node
    """
    return cast(Function(op="get_current_time", args=[]), "time")


def timestampdiff_fn(node: Function) -> None:
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
    node.args[1] = cast(node.args[1], "timestamp")
    node.args[2] = cast(node.args[2], "timestamp")


def extract_fn(node: Function) -> None:
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
    part = node.args[0].parts[0]
    if part.upper() == "YEAR_MONTH":
        node.args = apply_nested_functions([node.from_arg, Constant("%Y%m")])
        node.from_arg = None
        date_format_fn(node)
        return cast(node, "int")
    elif part.upper() == "DAY_MINUTE":
        node.args = apply_nested_functions([node.from_arg, Constant("%e%H%i")])
        node.from_arg = None
        date_format_fn(node)
        return cast(node, "int")
    else:
        node.args[0] = Constant(part)
        if not isinstance(node.from_arg, Identifier):
            node.from_arg = cast(node.from_arg, "timestamp")


def get_format_fn(node: Function) -> Constant:
    """
    Replace function with a constant according to table:
    Important! The parameters can be only constants.

    Example: GET_FORMAT(DATE, 'USA') => '%m.%d.%Y'

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_get-format
    """

    if len(node.args) != 2:
        raise ValueError("MySQL GET_FORMAT supports only 2 arguments")

    arg1, arg2 = node.args

    if not isinstance(arg1, Identifier) and len(arg1.parts) != 1:
        raise ValueError(f"Unknown type: {arg1}")

    if not isinstance(arg2, Constant):
        raise ValueError(f"Unknown format name: {arg2}")

    match arg1.parts[0].upper(), arg2.value.upper():
        case "DATE", "USA":
            value = "%m.%d.%Y"
        case "DATE", "JIS":
            value = "%Y-%m-%d"
        case "DATE", "ISO":
            value = "%Y-%m-%d"
        case "DATE", "EUR":
            value = "%d.%m.%Y"
        case "DATE", "INTERNAL":
            value = "%Y%m%d"

        case "DATETIME", "USA":
            value = "%Y-%m-%d %H.%i.%s"
        case "DATETIME", "JIS":
            value = "%Y-%m-%d %H:%i:%s"
        case "DATETIME", "ISO":
            value = "%Y-%m-%d %H:%i:%s"
        case "DATETIME", "EUR":
            value = "%Y-%m-%d %H.%i.%s"
        case "DATETIME", "INTERNAL":
            value = "%Y%m%d%H%i%s"

        case "TIME", "USA":
            value = "%h:%i:%s %p"
        case "TIME", "JIS":
            value = "%H:%i:%s"
        case "TIME", "ISO":
            value = "%H:%i:%s"
        case "TIME", "EUR":
            value = "%H.%i.%s"
        case "TIME", "INTERNAL":
            value = "%H%i%s"

        case _:
            value = ""

    return Constant(value)


def date_format_fn(node: Function):
    """
    Adapt to strftime function and convert keys in format string.

      DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y')
        =>
      strftime('2009-10-04 22:23:00'::datetime, '%A %B %Y')

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_date-format
        https://duckdb.org/docs/stable/sql/functions/timestamp.html#strftimetimestamp-format
        https://duckdb.org/docs/stable/sql/functions/dateformat.html#format-specifiers
    """
    specifiers_map = {
        "%c": "%-m",  # Month, numeric (0..12) -> Month as decimal
        "%D": "%-d",  # Day with English suffix -> Day as decimal (no suffix in DuckDB)
        "%e": "%-d",  # Day of month (0..31) -> Day as decimal
        "%h": "%I",  # Hour (01..12)
        "%i": "%M",  # Minutes
        "%j": "%j",  # Day of year
        "%k": "%-H",  # Hour (0..23) -> Hour as decimal
        "%l": "%-I",  # Hour (1..12) -> Hour as decimal
        "%M": "%B",  # Month name -> Full month name
        "%r": "%I:%M:%S %p",  # Time, 12-hour
        "%s": "%S",  # Seconds
        "%T": "%X",  # Time, 24-hour
        "%u": "%V",  # Week, mode 1, Monday is first day, can be wrong in the edges of year
        "%v": "%V",  # Week, mode 3, Monday is first day
        "%V": "%U",  # Week, mode 2, Sunday is first day, can be wrong in the edges of year
        "%W": "%A",  # Weekday name -> Full weekday name
        "%X": "%G",  # Year for week
        "%x": "%G",  # Year for week
    }
    node.op = "strftime"

    node.args = apply_nested_functions(node.args)

    if len(node.args) != 2 or not isinstance(node.args[1], Constant):
        raise ValueError(f"Wrong arguments: {node.args}")

    def repl_f(match):
        specifier = match.group()
        return specifiers_map.get(specifier, specifier)

    # adapt format string
    node.args[1].value = re.sub(r"%[a-zA-Z]", repl_f, node.args[1].value)

    # add type casting
    node.args[0] = cast(node.args[0], "timestamp")


def from_unixtime_fn(node):
    """
    Adapt to make_timestamp function
        FROM_UNIXTIME(1447430881) => make_timestamp((1447430881::int8 *1000000))

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_from-unixtime
        https://duckdb.org/docs/stable/sql/functions/timestamp#make_timestampmicroseconds
    """

    if len(node.args) != 1:
        raise ValueError(f"Wrong arguments: {node.args}")

    node.op = "make_timestamp"

    node.args[0] = BinaryOperation("*", args=[cast(node.args[0], "int8"), Constant(1_000_000)])


def from_days_fn(node):
    """
    Adapt to converting days to interval and adding to first day of the 0 year:
        FROM_DAYS(735669)  => '0000-01-01'::date + (735669 * INTERVAL '1 day')

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_from-days
    """
    node.args = apply_nested_functions(node.args)

    if len(node.args) != 1:
        raise ValueError(f"Wrong arguments: {node.args}")

    return BinaryOperation(
        op="+",
        args=[
            BinaryOperation("::", args=[Constant("0000-01-01"), Identifier("date")]),
            BinaryOperation("*", args=[node.args[0], Interval("1 day")]),
        ],
    )


def dayofyear_fn(node):
    """
    Addapt to DATE_PART:
        DAYOFYEAR('2007-02-03') => DATE_PART('doy', '2007-02-03'::date)

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_dayofyear
    """

    return date_part(node, "doy")


def dayofweek_fn(node):
    """
    Addapt to DATE_PART:
        DAYOFWEEK('2007-02-03'); => DATE_PART('dow', '2007-02-03'::date) + 1;

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_dayofweek
    """
    return BinaryOperation("+", args=[date_part(node, "dow"), Constant(1)])


def dayofmonth_fn(node):
    """
    Addapt to DATE_PART:
        DAYOFMONTH('2007-02-03') => DATE_PART('day', '2007-02-03'::date)

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_dayofmonth
    """

    return date_part(node, "day")


def dayname_fn(node):
    """
    Use the same function with type casting
        DAYNAME('2007-02-03') => DAYNAME('2007-02-03'::date)

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_dayname
    """
    if len(node.args) != 1:
        raise ValueError(f"Wrong arguments: {node.args}")

    node.args[0] = cast(node.args[0], "date")


def curdate_fn(node):
    """
    Replace the name of the function
        CURDATE() => CURRENT_DATE()

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_curdate
        https://duckdb.org/docs/stable/sql/functions/date.html#current_date
    """
    node.op = "CURRENT_DATE"


def datediff_fn(node):
    """
    Change argument's order and cast to date:
        DATEDIFF('2007-12-31 23:59:59','2007-11-30') => datediff('day',DATE '2007-11-30', DATE '2007-12-31 23:59:59')

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_datediff
        https://duckdb.org/docs/stable/sql/functions/date#date_diffpart-startdate-enddate

    """
    if len(node.args) != 2:
        raise ValueError(f"Wrong arguments: {node.args}")

    arg1, arg2 = node.args
    node.args = [Constant("day"), cast(arg2, "date"), cast(arg1, "date")]


def adddate_fn(node):
    """
    Replace the name of the function and add type casting
    Important! The second parameter can be only interval (not count of days).
        SELECT ADDDATE('2008-01-02', INTERVAL 31 DAY) => SELECT DATE_ADD('2008-01-02'::date, INTERVAL 31 DAY)

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_adddate
        https://duckdb.org/docs/stable/sql/functions/date.html#date_adddate-interval
    """
    if len(node.args) != 2:
        raise ValueError(f"Wrong arguments: {node.args}")

    node.op = "DATE_ADD"
    node.args[0] = cast(node.args[0], "timestamp")


def date_sub_fn(node):
    """
    Use DATE_ADD with negative interval
        SELECT DATE_SUB('1998-01-02', INTERVAL 31 DAY) => select DATE_ADD('1998-01-02'::date, -INTERVAL 31 DAY)

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_date-add
        https://duckdb.org/docs/stable/sql/functions/date.html#date_adddate-interval
    """
    if len(node.args) != 2:
        raise ValueError(f"Wrong arguments: {node.args}")

    node.op = "DATE_ADD"
    node.args[0] = cast(node.args[0], "timestamp")
    node.args[1] = UnaryOperation("-", args=[node.args[1]])


def addtime_fn(node):
    """
    Convert second parameter into interval.
    Important!
        - The second parameter can be only a constant.
        - The first parameter can be only date/datetime (not just time)

      ADDTIME('2007-12-31', '1 1:1:1.2')
        =>
      DATE_ADD('2007-12-31'::timestamp, INTERVAL '1 day 1 hour 1 minute 1.2 second')

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_addtime
        https://duckdb.org/docs/stable/sql/functions/date.html#date_adddate-interval
    """
    node.args = apply_nested_functions(node.args)

    if len(node.args) != 2:
        raise ValueError(f"Wrong arguments: {node.args}")

    interval = node.args[1]
    if not isinstance(interval, Constant) or not isinstance(interval.value, str):
        raise ValueError(f"The second argument have to be string: {node.args[1]}")

    pattern = r"^(?:(\d+)\s+)?(?:(\d+):)?(?:(\d+):)?(\d+)(?:\.(\d+))?$"

    match = re.match(pattern, interval.value)
    if not match:
        raise ValueError(f"Invalid MySQL time format: {interval.value}")

    # Extract components
    days, hours, minutes, seconds, fractional = match.groups()
    # Build interval string
    parts = []
    if days and int(days) > 0:
        parts.append(f"{days} day")

    if hours and int(hours) > 0:
        parts.append(f"{int(hours)} hour")

    if minutes and int(minutes) > 0:
        parts.append(f"{int(minutes)} minute")

    seconds = int(seconds) if seconds else 0
    fractional = float(f"0.{fractional}") if fractional else 0.0
    total_seconds = seconds + fractional
    if total_seconds > 0:
        seconds_str = str(total_seconds).rstrip("0").rstrip(".")
        parts.append(f"{seconds_str} second")

    # If all components are zero, return 0 seconds
    if not parts:
        interval_str = "0 second"
    else:
        interval_str = " ".join(parts)

    return Function(
        "DATE_ADD",
        args=[
            cast(node.args[0], "timestamp"),
            Interval(interval_str),
        ],
    )


def convert_tz_fn(node):
    """
    Concatenate timezone to first argument and cast it as timestamptz. Then use `timezone` function
    Important! Duckdb doesn't recognize timezones in digital formats: +10:00

      CONVERT_TZ('2004-01-01 12:00:00','GMT','MET')
        =>
      timezone('MET', ('2004-01-01 12:00:00' || ' ' || 'GMT')::timestamptz);

    Docs:
        https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_convert-tz
        https://duckdb.org/docs/stable/sql/functions/timestamptz.html#timezonetext-timestamp
    """
    node.args = apply_nested_functions(node.args)

    if len(node.args) != 3:
        raise ValueError(f"Wrong arguments: {node.args}")

    date, tzfrom, tzto = node.args

    # concatenate tz name: date || ' ' || tzfrom
    tzdate = BinaryOperation("||", args=[BinaryOperation("||", args=[date, Constant(" ")]), tzfrom], parentheses=True)

    return Function(
        "timezone",
        args=[
            tzto,
            cast(tzdate, "timestamptz"),
        ],
    )


def apply_nested_functions(args):
    args2 = []
    for arg in args:
        if isinstance(arg, Function):
            fnc = mysql_to_duckdb_fnc(arg)
            if args2 is not None:
                arg = fnc(arg)
        args2.append(arg)
    return args2


def mysql_to_duckdb_fnc(node):
    fnc_name = node.op.lower()

    mysql_to_duck_fn_map = {
        "char": char_fn,
        "locate": locate_fn,
        "insrt": locate_fn,
        "unhex": unhex_fn,
        "format": format_fn,
        "sha2": sha2_fn,
        "length": length_fn,
        "regexp_substr": regexp_substr_fn,
        "substring_index": substring_index_fn,
        "curtime": curtime_fn,
        "timestampdiff": timestampdiff_fn,
        "extract": extract_fn,
        "get_format": get_format_fn,
        "date_format": date_format_fn,
        "from_unixtime": from_unixtime_fn,
        "from_days": from_days_fn,
        "dayofyear": dayofyear_fn,
        "dayofweek": dayofweek_fn,
        "day": dayofmonth_fn,
        "dayofmonth": dayofmonth_fn,
        "dayname": dayname_fn,
        "curdate": curdate_fn,
        "datediff": datediff_fn,
        "adddate": adddate_fn,
        "date_sub": date_sub_fn,
        "date_add": adddate_fn,
        "addtime": addtime_fn,
        "convert_tz": convert_tz_fn,
    }
    if fnc_name in mysql_to_duck_fn_map:
        return mysql_to_duck_fn_map[fnc_name]
