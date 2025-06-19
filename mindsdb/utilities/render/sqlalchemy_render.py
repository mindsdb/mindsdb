import re
import datetime as dt

import sqlalchemy as sa
from sqlalchemy.sql import operators
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import aliased
from sqlalchemy.dialects import mysql, postgresql, sqlite, mssql, oracle
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql import functions as sa_fnc

from mindsdb_sql_parser import ast


RESERVED_WORDS = {"collation"}

sa_type_names = [
    key
    for key, val in sa.types.__dict__.items()
    if hasattr(val, "__module__") and val.__module__ in ("sqlalchemy.sql.sqltypes", "sqlalchemy.sql.type_api")
]

types_map = {}
for type_name in sa_type_names:
    types_map[type_name.upper()] = getattr(sa.types, type_name)
types_map["BOOL"] = types_map["BOOLEAN"]
types_map["DEC"] = types_map["DECIMAL"]


class RenderError(Exception): ...


# https://github.com/sqlalchemy/sqlalchemy/discussions/9483?sort=old#discussioncomment-5312979
class INTERVAL(ColumnElement):
    def __init__(self, info):
        self.info = info
        self.type = sa.Interval()


@compiles(INTERVAL)
def _compile_interval(element, compiler, **kw):
    items = element.info.split(" ", maxsplit=1)
    if compiler.dialect.name == "oracle" and len(items) == 2:
        # replace to singular names (remove leading S if exists)
        if items[1].upper().endswith("S"):
            items[1] = items[1][:-1]

    if compiler.dialect.driver in ["snowflake"]:
        # quote all
        args = " ".join(map(str, items))
        args = f"'{args}'"
    else:
        # quote first element
        items[0] = f"'{items[0]}'"
        args = " ".join(items)
    return "INTERVAL " + args


class AttributedStr(str):
    """
    Custom str-like object to pass it to `_requires_quotes` method with `is_quoted` flag
    """

    def __new__(cls, string, is_quoted: bool):
        obj = str.__new__(cls, string)
        obj.is_quoted = is_quoted
        return obj

    def replace(self, *args):
        obj = super().replace(*args)
        return AttributedStr(obj, self.is_quoted)


def get_is_quoted(identifier: ast.Identifier):
    quoted = getattr(identifier, "is_quoted", [])
    # len can be different
    quoted = quoted + [None] * (len(identifier.parts) - len(quoted))
    return quoted


class SqlalchemyRender:
    def __init__(self, dialect_name):
        dialects = {
            "mysql": mysql,
            "postgresql": postgresql,
            "postgres": postgresql,
            "sqlite": sqlite,
            "mssql": mssql,
            "oracle": oracle,
            "Snowflake": oracle,
        }

        if isinstance(dialect_name, str):
            dialect = dialects[dialect_name].dialect
        else:
            dialect = dialect_name

        # override dialect's preparer
        if hasattr(dialect, "preparer"):

            class Preparer(dialect.preparer):
                def _requires_quotes(self, value: str) -> bool:
                    # check force-quote flag
                    if isinstance(value, AttributedStr):
                        if value.is_quoted:
                            return True

                    lc_value = value.lower()
                    return (
                        lc_value in self.reserved_words
                        or value[0] in self.illegal_initial_characters
                        or not self.legal_characters.match(str(value))
                        #  Override sqlalchemy behavior: don't require to quote mixed- or upper-case
                        # or (lc_value != value)
                    )

            dialect.preparer = Preparer

        # remove double percent signs
        # https://docs.sqlalchemy.org/en/14/faq/sqlexpressions.html#why-are-percent-signs-being-doubled-up-when-stringifying-sql-statements
        self.dialect = dialect(paramstyle="named")
        self.dialect.div_is_floordiv = False

        self.selects_stack = []

        if dialect_name == "mssql":
            # update version to MS_2008_VERSION for supports_multivalues_insert
            self.dialect.server_version_info = (10,)
            self.dialect._setup_version_attributes()
        elif dialect_name == "mysql":
            # update version for support float cast
            self.dialect.server_version_info = (8, 0, 17)

    def to_column(self, identifier: ast.Identifier) -> sa.Column:
        # because sqlalchemy doesn't allow columns consist from parts therefore we do it manually

        parts2 = []

        quoted = get_is_quoted(identifier)
        for i, is_quoted in zip(identifier.parts, quoted):
            if isinstance(i, ast.Star):
                part = "*"
            elif is_quoted or i.lower() in RESERVED_WORDS:
                # quote anyway
                part = self.dialect.identifier_preparer.quote_identifier(i)
            else:
                # quote if required
                part = self.dialect.identifier_preparer.quote(i)

            parts2.append(part)
        text = ".".join(parts2)
        if identifier.is_outer and self.dialect.name == "oracle":
            text += "(+)"
        return sa.column(text, is_literal=True)

    def get_alias(self, alias):
        if alias is None or len(alias.parts) == 0:
            return None
        if len(alias.parts) > 1:
            raise NotImplementedError(f"Multiple alias {alias.parts}")

        if self.selects_stack:
            self.selects_stack[-1]["aliases"].append(alias)

        is_quoted = get_is_quoted(alias)[0]
        return AttributedStr(alias.parts[0], is_quoted)

    def make_unique_alias(self, name):
        if self.selects_stack:
            aliases = self.selects_stack[-1]["aliases"]
            for i in range(10):
                name2 = f"{name}_{i}"
                if name2 not in aliases:
                    aliases.append(name2)
                    return name2

    def to_expression(self, t):
        # simple type
        if isinstance(t, str) or isinstance(t, int) or isinstance(t, float) or t is None:
            t = ast.Constant(t)

        if isinstance(t, ast.Star):
            col = sa.text("*")
        elif isinstance(t, ast.Last):
            col = self.to_column(ast.Identifier(parts=["last"]))
        elif isinstance(t, ast.Constant):
            col = sa.literal(t.value)
            if t.alias:
                alias = self.get_alias(t.alias)
            else:
                if t.value is None:
                    alias = "NULL"
                else:
                    alias = str(t.value)
            col = col.label(alias)
        elif isinstance(t, ast.Identifier):
            # sql functions
            col = None
            if len(t.parts) == 1:
                if isinstance(t.parts[0], str):
                    name = t.parts[0].upper()
                    if name == "CURRENT_DATE":
                        col = sa_fnc.current_date()
                    elif name == "CURRENT_TIME":
                        col = sa_fnc.current_time()
                    elif name == "CURRENT_TIMESTAMP":
                        col = sa_fnc.current_timestamp()
                    elif name == "CURRENT_USER":
                        col = sa_fnc.current_user()
            if col is None:
                col = self.to_column(t)
            if t.alias:
                col = col.label(self.get_alias(t.alias))
        elif isinstance(t, ast.Select):
            sub_stmt = self.prepare_select(t)
            col = sub_stmt.scalar_subquery()
            if t.alias:
                alias = self.get_alias(t.alias)
                col = col.label(alias)
        elif isinstance(t, ast.Function):
            col = self.to_function(t)
            if t.alias:
                alias = self.get_alias(t.alias)
                col = col.label(alias)
            else:
                alias = self.make_unique_alias(str(t.op))
                if alias:
                    col = col.label(alias)

        elif isinstance(t, ast.BinaryOperation):
            ops = {
                "+": operators.add,
                "-": operators.sub,
                "*": operators.mul,
                "/": operators.truediv,
                "%": operators.mod,
                "=": operators.eq,
                "!=": operators.ne,
                "<>": operators.ne,
                ">": operators.gt,
                "<": operators.lt,
                ">=": operators.ge,
                "<=": operators.le,
                "is": operators.is_,
                "is not": operators.is_not,
                "like": operators.like_op,
                "not like": operators.not_like_op,
                "in": operators.in_op,
                "not in": operators.not_in_op,
                "||": operators.concat_op,
            }
            functions = {
                "and": sa.and_,
                "or": sa.or_,
            }

            arg0 = self.to_expression(t.args[0])
            arg1 = self.to_expression(t.args[1])

            op = t.op.lower()
            if op in ("in", "not in"):
                if t.args[1].parentheses:
                    arg1 = [arg1]
                if isinstance(arg1, sa.sql.selectable.ColumnClause):
                    raise NotImplementedError(f"Required list argument for: {op}")

            sa_op = ops.get(op)

            if sa_op is not None:
                if isinstance(arg0, sa.TextClause):
                    # text doesn't have operate method, reverse operator
                    col = arg1.reverse_operate(sa_op, arg0)
                elif isinstance(arg1, sa.TextClause):
                    # both args are text, return text
                    col = sa.text(f"{arg0.compile(dialect=self.dialect)} {op} {arg1.compile(dialect=self.dialect)}")
                else:
                    col = arg0.operate(sa_op, arg1)

            elif t.op.lower() in functions:
                func = functions[t.op.lower()]
                col = func(arg0, arg1)
            else:
                col = arg0.op(t.op)(arg1)

            if t.alias:
                alias = self.get_alias(t.alias)
                col = col.label(alias)

        elif isinstance(t, ast.UnaryOperation):
            # not or munus
            opmap = {
                "NOT": "__invert__",
                "-": "__neg__",
            }
            arg = self.to_expression(t.args[0])

            method = opmap[t.op.upper()]
            col = getattr(arg, method)()
            if t.alias:
                alias = self.get_alias(t.alias)
                col = col.label(alias)

        elif isinstance(t, ast.BetweenOperation):
            col0 = self.to_expression(t.args[0])
            lim_down = self.to_expression(t.args[1])
            lim_up = self.to_expression(t.args[2])

            col = sa.between(col0, lim_down, lim_up)
        elif isinstance(t, ast.Interval):
            col = INTERVAL(t.args[0])
            if t.alias:
                alias = self.get_alias(t.alias)
                col = col.label(alias)

        elif isinstance(t, ast.WindowFunction):
            func = self.to_expression(t.function)

            partition = None
            if t.partition is not None:
                partition = [self.to_expression(i) for i in t.partition]

            order_by = None
            if t.order_by is not None:
                order_by = []
                for f in t.order_by:
                    col0 = self.to_expression(f.field)
                    if f.direction == "DESC":
                        col0 = col0.desc()
                    order_by.append(col0)

            rows, range_ = None, None
            if t.modifier is not None:
                words = t.modifier.lower().split()
                if words[1] == "between" and words[4] == "and":
                    # frame options
                    # rows/groups BETWEEN <> <> AND <> <>
                    # https://docs.sqlalchemy.org/en/20/core/sqlelement.html#sqlalchemy.sql.expression.over
                    items = []
                    for word1, word2 in (words[2:4], words[5:7]):
                        if word1 == "unbounded":
                            items.append(None)
                        elif (word1, word2) == ("current", "row"):
                            items.append(0)
                        elif word1.isdigits():
                            val = int(word1)
                            if word2 == "preceding":
                                val = -val
                            elif word2 != "following":
                                continue
                            items.append(val)
                    if len(items) == 2:
                        if words[0] == "rows":
                            rows = tuple(items)
                        elif words[0] == "range":
                            range_ = tuple(items)

            col = sa.over(func, partition_by=partition, order_by=order_by, range_=range_, rows=rows)

            if t.alias:
                col = col.label(self.get_alias(t.alias))
        elif isinstance(t, ast.TypeCast):
            arg = self.to_expression(t.arg)
            type = self.get_type(t.type_name)
            if t.precision is not None:
                type = type(*t.precision)
            col = sa.cast(arg, type)

            if t.alias:
                alias = self.get_alias(t.alias)
                col = col.label(alias)
            else:
                alias = self.make_unique_alias("cast")
                if alias:
                    col = col.label(alias)
        elif isinstance(t, ast.Parameter):
            col = sa.column(t.value, is_literal=True)
            if t.alias:
                raise RenderError()
        elif isinstance(t, ast.Tuple):
            col = [self.to_expression(i) for i in t.items]
        elif isinstance(t, ast.Variable):
            col = sa.column(t.to_string(), is_literal=True)
        elif isinstance(t, ast.Latest):
            col = sa.column(t.to_string(), is_literal=True)
        elif isinstance(t, ast.Exists):
            sub_stmt = self.prepare_select(t.query)
            col = sub_stmt.exists()
        elif isinstance(t, ast.NotExists):
            sub_stmt = self.prepare_select(t.query)
            col = ~sub_stmt.exists()
        elif isinstance(t, ast.Case):
            col = self.prepare_case(t)
        else:
            # some other complex object?
            raise NotImplementedError(f"Column {t}")

        return col

    def prepare_case(self, t: ast.Case):
        conditions = []
        for condition, result in t.rules:
            conditions.append((self.to_expression(condition), self.to_expression(result)))
        default = None
        if t.default is not None:
            default = self.to_expression(t.default)

        value = None
        if t.arg is not None:
            value = self.to_expression(t.arg)

        col = sa.case(*conditions, else_=default, value=value)
        if t.alias:
            col = col.label(self.get_alias(t.alias))
        return col

    def to_function(self, t):
        op = getattr(sa.func, t.op)
        if t.from_arg is not None:
            arg = t.args[0].to_string()
            from_arg = self.to_expression(t.from_arg)

            fnc = op(arg, from_arg)
        else:
            args = [self.to_expression(i) for i in t.args]
            if t.distinct:
                # set first argument to distinct
                args[0] = args[0].distinct()
            fnc = op(*args)
        return fnc

    def get_type(self, typename):
        # TODO how to get type
        if not isinstance(typename, str):
            # sqlalchemy type
            return typename

        typename = typename.upper()
        if re.match(r"^INT[\d]+$", typename):
            typename = "BIGINT"
        if re.match(r"^FLOAT[\d]+$", typename):
            typename = "FLOAT"

        return types_map[typename]

    def prepare_join(self, join):
        # join tree to table list

        if isinstance(join.right, ast.Join):
            raise NotImplementedError("Wrong join AST")

        items = []

        if isinstance(join.left, ast.Join):
            # dive to next level
            items.extend(self.prepare_join(join.left))
        else:
            # this is first table
            items.append(dict(table=join.left))

        # all properties set to right table
        items.append(
            dict(table=join.right, join_type=join.join_type, is_implicit=join.implicit, condition=join.condition)
        )

        return items

    def get_table_name(self, table_name):
        schema = None
        if isinstance(table_name, ast.Identifier):
            parts = table_name.parts
            quoted = get_is_quoted(table_name)

            if len(parts) > 2:
                # TODO tests is failing
                raise NotImplementedError(f"Path to long: {table_name.parts}")

            if len(parts) == 2:
                schema = AttributedStr(parts[-2], quoted[-2])

            table_name = AttributedStr(parts[-1], quoted[-1])

        return schema, table_name

    def to_table(self, node, is_lateral=False):
        if isinstance(node, ast.Identifier):
            schema, table_name = self.get_table_name(node)

            table = sa.table(table_name, schema=schema)

            if node.alias:
                table = aliased(table, name=self.get_alias(node.alias))

        elif isinstance(node, (ast.Select, ast.Union, ast.Intersect, ast.Except)):
            sub_stmt = self.prepare_select(node)
            alias = None
            if node.alias:
                alias = self.get_alias(node.alias)
            if is_lateral:
                table = sub_stmt.lateral(alias)
            else:
                table = sub_stmt.subquery(alias)

        else:
            # TODO tests are failing
            raise NotImplementedError(f"Table {node.__name__}")

        return table

    def prepare_select(self, node):
        if isinstance(node, (ast.Union, ast.Except, ast.Intersect)):
            return self.prepare_union(node)

        cols = []

        self.selects_stack.append({"aliases": []})

        for t in node.targets:
            col = self.to_expression(t)
            cols.append(col)

        query = sa.select(*cols)

        if node.cte is not None:
            for cte in node.cte:
                if cte.columns is not None and len(cte.columns) > 0:
                    raise NotImplementedError("CTE columns")

                stmt = self.prepare_select(cte.query)
                alias = cte.name

                query = query.add_cte(stmt.cte(self.get_alias(alias), nesting=True))

        if node.distinct is True:
            query = query.distinct()
        elif isinstance(node.distinct, list):
            columns = [self.to_expression(c) for c in node.distinct]
            query = query.distinct(*columns)

        if node.from_table is not None:
            from_table = node.from_table

            if isinstance(from_table, ast.Join):
                join_list = self.prepare_join(from_table)
                # first table
                table = self.to_table(join_list[0]["table"])
                query = query.select_from(table)

                # other tables
                has_explicit_join = False
                for item in join_list[1:]:
                    join_type = item["join_type"]
                    table = self.to_table(item["table"], is_lateral=("LATERAL" in join_type))
                    if item["is_implicit"]:
                        # add to from clause
                        if has_explicit_join:
                            # sqlalchemy doesn't support implicit join after explicit
                            # convert it to explicit
                            query = query.join(table, sa.text("1=1"))
                        else:
                            query = query.select_from(table)
                    else:
                        has_explicit_join = True
                        if item["condition"] is None:
                            # otherwise, sqlalchemy raises "Don't know how to join to ..."
                            condition = sa.text("1=1")
                        else:
                            condition = self.to_expression(item["condition"])

                        if "ASOF" in join_type:
                            raise NotImplementedError(f"Unsupported join type: {join_type}")
                        method = "join"
                        is_full = False
                        if join_type == "LEFT JOIN":
                            method = "outerjoin"
                        if join_type == "FULL JOIN":
                            is_full = True

                        # perform join
                        query = getattr(query, method)(table, condition, full=is_full)
            elif isinstance(from_table, (ast.Union, ast.Intersect, ast.Except)):
                alias = None
                if from_table.alias:
                    alias = self.get_alias(from_table.alias)
                table = self.prepare_union(from_table).subquery(alias)
                query = query.select_from(table)

            elif isinstance(from_table, ast.Select):
                table = self.to_table(from_table)
                query = query.select_from(table)

            elif isinstance(from_table, ast.Identifier):
                table = self.to_table(from_table)
                query = query.select_from(table)

            elif isinstance(from_table, ast.NativeQuery):
                alias = None
                if from_table.alias:
                    alias = from_table.alias.parts[-1]
                table = sa.text(from_table.query).columns().subquery(alias)
                query = query.select_from(table)
            else:
                raise NotImplementedError(f"Select from {from_table}")

        if node.where is not None:
            query = query.filter(self.to_expression(node.where))

        if node.group_by is not None:
            cols = [self.to_expression(i) for i in node.group_by]
            query = query.group_by(*cols)

        if node.having is not None:
            query = query.having(self.to_expression(node.having))

        if node.order_by is not None:
            order_by = []
            for f in node.order_by:
                col0 = self.to_expression(f.field)
                if f.direction.upper() == "DESC":
                    col0 = col0.desc()
                elif f.direction.upper() == "ASC":
                    col0 = col0.asc()
                if f.nulls.upper() == "NULLS FIRST":
                    col0 = sa.nullsfirst(col0)
                elif f.nulls.upper() == "NULLS LAST":
                    col0 = sa.nullslast(col0)
                order_by.append(col0)

            query = query.order_by(*order_by)

        if node.limit is not None:
            query = query.limit(node.limit.value)

        if node.offset is not None:
            query = query.offset(node.offset.value)

        if node.mode is not None:
            if node.mode == "FOR UPDATE":
                query = query.with_for_update()
            else:
                raise NotImplementedError(f"Select mode: {node.mode}")

        self.selects_stack.pop()

        return query

    def prepare_union(self, from_table):
        step1 = self.prepare_select(from_table.left)
        step2 = self.prepare_select(from_table.right)

        if isinstance(from_table, ast.Except):
            func = sa.except_ if from_table.unique else sa.except_all
        elif isinstance(from_table, ast.Intersect):
            func = sa.intersect if from_table.unique else sa.intersect_all
        else:
            func = sa.union if from_table.unique else sa.union_all

        return func(step1, step2)

    def prepare_create_table(self, ast_query):
        columns = []

        for col in ast_query.columns:
            default = None
            if col.default is not None:
                if isinstance(col.default, str):
                    default = sa.text(col.default)

            if isinstance(col.type, str) and col.type.lower() == "serial":
                col.is_primary_key = True
                col.type = "INT"

            kwargs = {
                "primary_key": col.is_primary_key,
                "server_default": default,
            }
            if col.nullable is not None:
                kwargs["nullable"] = col.nullable

            columns.append(sa.Column(col.name, self.get_type(col.type), **kwargs))

        schema, table_name = self.get_table_name(ast_query.name)

        metadata = sa.MetaData()
        table = sa.Table(table_name, metadata, schema=schema, *columns)

        return CreateTable(table)

    def prepare_drop_table(self, ast_query):
        if len(ast_query.tables) != 1:
            raise NotImplementedError("Only one table is supported")

        schema, table_name = self.get_table_name(ast_query.tables[0])

        metadata = sa.MetaData()
        table = sa.Table(table_name, metadata, schema=schema)
        return DropTable(table, if_exists=ast_query.if_exists)

    def prepare_insert(self, ast_query, with_params=False):
        params = None
        schema, table_name = self.get_table_name(ast_query.table)

        names = []
        columns = []

        if ast_query.columns is None:
            raise NotImplementedError("Columns is required in insert query")
        for col in ast_query.columns:
            columns.append(
                sa.Column(
                    col.name,
                    # self.get_type(col.type)
                )
            )
            # check doubles
            if col.name in names:
                raise RenderError(f"Columns name double: {col.name}")
            names.append(col.name)

        table = sa.table(table_name, schema=schema, *columns)

        if ast_query.values is not None:
            values = []

            if ast_query.is_plain and with_params:
                for i in range(len(ast_query.columns)):
                    values.append(sa.column("%s", is_literal=True))

                values = [values]
                params = ast_query.values
            else:
                for row in ast_query.values:
                    row = [self.to_expression(val) for val in row]
                    values.append(row)

            stmt = table.insert().values(values)
        else:
            # is insert from subselect
            subquery = self.prepare_select(ast_query.from_select)
            stmt = table.insert().from_select(names, subquery)

        return stmt, params

    def prepare_update(self, ast_query):
        if ast_query.from_select is not None:
            raise NotImplementedError("Render of update with sub-select is not implemented")

        schema, table_name = self.get_table_name(ast_query.table)

        columns = []

        to_update = {}
        for col, value in ast_query.update_columns.items():
            columns.append(
                sa.Column(
                    col,
                )
            )

            to_update[col] = self.to_expression(value)

        table = sa.table(table_name, schema=schema, *columns)

        stmt = table.update().values(**to_update)

        if ast_query.where is not None:
            stmt = stmt.where(self.to_expression(ast_query.where))

        return stmt

    def prepare_delete(self, ast_query: ast.Delete):
        schema, table_name = self.get_table_name(ast_query.table)

        columns = []

        table = sa.table(table_name, schema=schema, *columns)

        stmt = table.delete()

        if ast_query.where is not None:
            stmt = stmt.where(self.to_expression(ast_query.where))

        return stmt

    def get_query(self, ast_query, with_params=False):
        params = None
        if isinstance(ast_query, (ast.Select, ast.Union, ast.Except, ast.Intersect)):
            stmt = self.prepare_select(ast_query)
        elif isinstance(ast_query, ast.Insert):
            stmt, params = self.prepare_insert(ast_query, with_params=with_params)
        elif isinstance(ast_query, ast.Update):
            stmt = self.prepare_update(ast_query)
        elif isinstance(ast_query, ast.Delete):
            stmt = self.prepare_delete(ast_query)
        elif isinstance(ast_query, ast.CreateTable):
            stmt = self.prepare_create_table(ast_query)
        elif isinstance(ast_query, ast.DropTables):
            stmt = self.prepare_drop_table(ast_query)
        else:
            raise NotImplementedError(f"Unknown statement: {ast_query.__class__.__name__}")
        return stmt, params

    def get_string(self, ast_query, with_failback=True):
        """
        Render query to sql string

        :param ast_query: query to render
        :param with_failback:  switch to standard render in case of error
        :return:
        """
        sql, _ = self.get_exec_params(ast_query, with_failback=with_failback, with_params=False)
        return sql

    def get_exec_params(self, ast_query, with_failback=True, with_params=True):
        """
        Render query with separated parameters and placeholders
        :param ast_query: query to render
        :param with_failback: switch to standard render in case of error
        :return: sql query and parameters
        """

        if isinstance(ast_query, (ast.CreateTable, ast.DropTables)):
            render_func = render_ddl_query
        else:
            render_func = render_dml_query

        try:
            stmt, params = self.get_query(ast_query, with_params=with_params)

            sql = render_func(stmt, self.dialect)

            return sql, params

        except (SQLAlchemyError, NotImplementedError) as e:
            if not with_failback:
                raise e

            sql_query = str(ast_query)
            if self.dialect.name == "postgresql":
                sql_query = sql_query.replace("`", "")
            return sql_query, None


def render_dml_query(statement, dialect):
    class LiteralCompiler(dialect.statement_compiler):
        def render_literal_value(self, value, type_):
            if isinstance(value, (str, dt.date, dt.datetime, dt.timedelta)):
                return "'{}'".format(str(value).replace("'", "''"))

            return super(LiteralCompiler, self).render_literal_value(value, type_)

    return str(LiteralCompiler(dialect, statement, compile_kwargs={"literal_binds": True}))


def render_ddl_query(statement, dialect):
    class LiteralCompiler(dialect.ddl_compiler):
        def render_literal_value(self, value, type_):
            if isinstance(value, (str, dt.date, dt.datetime, dt.timedelta)):
                return "'{}'".format(str(value).replace("'", "''"))

            return super(LiteralCompiler, self).render_literal_value(value, type_)

    return str(LiteralCompiler(dialect, statement, compile_kwargs={"literal_binds": True}))
