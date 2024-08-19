from sly import Parser

from mindsdb_sql.parser.ast import *
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.parser.lexer import SQLLexer
from mindsdb_sql.parser.logger import ParserLogger
from mindsdb_sql.parser.utils import ensure_select_keyword_order, JoinType


class SQLParser(Parser):
    log = ParserLogger()
    tokens = SQLLexer.tokens

    precedence = (
        ('left', OR),
        ('left', AND),
        ('right', UNOT),
        ('left', EQUALS, NEQUALS),
        ('left', PLUS, MINUS),
        ('left', STAR, DIVIDE),
        ('right', UMINUS),  # Unary minus operator, unary not
        ('nonassoc', LESS, LEQ, GREATER, GEQ, IN, BETWEEN, IS, IS_NOT, LIKE),
    )

    # Top-level statements
    @_('show',
       'start_transaction',
       'commit_transaction',
       'rollback_transaction',
       'alter_table',
       'explain',
       'set',
       'use',
       'describe',
       'union',
       'select',
       'insert',
       'update',
       'delete',
       'drop_view',
       )
    def query(self, p):
        return p[0]

    # Explain
    @_('EXPLAIN identifier')
    def explain(self, p):
        return Explain(target=p.identifier)

    # Alter table
    @_('ALTER TABLE identifier id id')
    def alter_table(self, p):
        return AlterTable(target=p.identifier,
                          arg=' '.join([p.id0, p.id1]))

    # DROP VEW
    @_('DROP VIEW identifier',
       'DROP VIEW IF_EXISTS identifier')
    def drop_view(self, p):
        if_exists = hasattr(p, 'IF_EXISTS')
        return DropView([p.identifier], if_exists=if_exists)

    @_('DROP VIEW enumeration',
       'DROP VIEW IF_EXISTS enumeration')
    def drop_view(self, p):
        if_exists = hasattr(p, 'IF_EXISTS')
        return DropView(p.enumeration, if_exists=if_exists)

    # Transactions

    @_('START TRANSACTION')
    def start_transaction(self, p):
        return StartTransaction()

    @_('COMMIT')
    def commit_transaction(self, p):
        return CommitTransaction()

    @_('ROLLBACK')
    def rollback_transaction(self, p):
        return RollbackTransaction()

    # Set

    @_('SET expr')
    def set(self, p):
        return Set(arg=p.expr)

    @_('SET id identifier')
    def set(self, p):
        if not p.id.lower() == 'names':
            raise ParsingException(f'Expected "SET names", got "SET {p.id}"')
        return Set(category=p.id.lower(), arg=p.identifier)

    # Show
    @_('show WHERE expr')
    def show(self, p):
        command = p.show
        command.where = p.expr
        return command

    @_('show LIKE string')
    def show(self, p):
        command = p.show
        command.like = p.string
        return command

    @_('show FROM identifier')
    def show(self, p):
        command = p.show
        value0 = command.from_table
        value1 = p.identifier
        if value0 is not None:
            value1.parts = value1.parts + value0.parts

        command.from_table = value1
        return command

    @_('show IN identifier')
    def show(self, p):
        command = p.show
        value0 = command.in_table
        value1 = p.identifier
        if value0 is not None:
            value1.parts = value1.parts + value0.parts

        command.in_table = value1
        return command

    @_('SHOW show_category',
       'SHOW show_modifier_list show_category')
    def show(self, p):
        modes = getattr(p, 'show_modifier_list', None)
        return Show(
            category=p.show_category,
            modes=modes
        )

    @_('SCHEMAS',
       'DATABASES',
       'TABLES',
       'OPEN TABLES',
       'TRIGGERS',
       'COLUMNS',
       'FIELDS',
       'PLUGINS',
       'VARIABLES',
       'INDEXES',
       'KEYS',
       'SESSION VARIABLES',
       'GLOBAL VARIABLES',
       'GLOBAL STATUS',
       'SESSION STATUS',
       'PROCEDURE STATUS',
       'FUNCTION STATUS',
       'TABLE STATUS',
       'MASTER STATUS',
       'STATUS',
       'STORAGE ENGINES',
       'PROCESSLIST',
       'INDEX',
       'CREATE TABLE',
       'WARNINGS',
       'ENGINES',
       'CHARSET',
       'CHARACTER SET',
       'COLLATION',
       'BINARY LOGS',
       'MASTER LOGS',
       'PRIVILEGES',
       'PROFILES',
       'REPLICAS',
       'SLAVE HOSTS',
       )
    def show_category(self, p):
        return ' '.join([x for x in p])

    @_('show_modifier',
       'show_modifier_list show_modifier')
    def show_modifier_list(self, p):
        if hasattr(p, 'empty'):
            return None
        params = getattr(p, 'show_modifier_list', [])
        params.append(p.show_modifier)
        return params

    @_('EXTENDED',
       'FULL')
    def show_modifier(self, p):
        return p[0]

    # DELETE
    @_('DELETE FROM from_table WHERE expr',
       'DELETE FROM from_table')
    def delete(self, p):
        where = getattr(p, 'expr', None)

        if where is not None and not isinstance(where, Operation):
            raise ParsingException(
                f"WHERE must contain an operation that evaluates to a boolean, got: {str(where)}")

        return Delete(table=p.from_table, where=where)

    # UPDATE
    @_('UPDATE identifier SET update_parameter_list',
       'UPDATE identifier SET update_parameter_list WHERE expr')
    def update(self, p):
        where = getattr(p, 'expr', None)
        return Update(table=p.identifier,
                      update_columns=p.update_parameter_list,
                      where=where)

    # INSERT
    @_('INSERT INTO from_table LPAREN result_columns RPAREN select',
       'INSERT INTO from_table select')
    def insert(self, p):
        columns = getattr(p, 'result_columns', None)
        return Insert(table=p.from_table, columns=columns, from_select=p.select)

    @_('INSERT INTO from_table LPAREN result_columns RPAREN VALUES expr_list_set',
       'INSERT INTO from_table VALUES expr_list_set')
    def insert(self, p):
        columns = getattr(p, 'result_columns', None)
        return Insert(table=p.from_table, columns=columns, values=p.expr_list_set)

    @_('expr_list_set COMMA expr_list_set')
    def expr_list_set(self, p):
        return p.expr_list_set0 + p.expr_list_set1

    @_('LPAREN expr_list RPAREN')
    def expr_list_set(self, p):
        return [p.expr_list]

    # DESCRIBE

    @_('DESCRIBE identifier')
    def describe(self, p):
        return Describe(value=p.identifier)

    # USE

    @_('USE identifier')
    def use(self, p):
        return Use(value=p.identifier)

    # UNION / UNION ALL
    @_('select UNION select')
    def union(self, p):
        return Union(left=p.select0, right=p.select1, unique=True)

    @_('select UNION ALL select')
    def union(self, p):
        return Union(left=p.select0, right=p.select1, unique=False)

    # WITH
    @_('ctes select')
    def select(self, p):
        select = p.select
        select.cte = p.ctes
        return select

    @_('ctes COMMA identifier cte_columns_or_nothing AS LPAREN select RPAREN')
    def ctes(self, p):
        ctes = p.ctes
        ctes = ctes + [
            CommonTableExpression(
                name=p.identifier,
                columns=p.cte_columns_or_nothing,
                query=p.select)
        ]
        return ctes

    @_('WITH identifier cte_columns_or_nothing AS LPAREN select RPAREN')
    def ctes(self, p):
        return [
            CommonTableExpression(
                name=p.identifier,
                columns=p.cte_columns_or_nothing,
                query=p.select)
        ]

    @_('empty')
    def cte_columns_or_nothing(self, p):
        pass

    @_('LPAREN enumeration RPAREN')
    def cte_columns_or_nothing(self, p):
        return p.enumeration

    # SELECT
    @_('select OFFSET constant')
    def select(self, p):
        select = p.select
        if select.offset is not None:
            raise ParsingException(f'OFFSET already specified for this query')
        ensure_select_keyword_order(select, 'OFFSET')
        if not isinstance(p.constant.value, int):
            raise ParsingException(f'OFFSET must be an integer value, got: {p.constant.value}')

        select.offset = p.constant
        return select

    @_('select LIMIT constant')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'LIMIT')
        if not isinstance(p.constant.value, int):
            raise ParsingException(f'LIMIT must be an integer value, got: {p.constant.value}')
        select.limit = p.constant
        return select

    @_('select LIMIT constant COMMA constant')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'LIMIT')
        if not isinstance(p.constant0.value, int) or not isinstance(p.constant1.value, int):
            raise ParsingException(f'LIMIT must have integer arguments, got: {p.constant0.value}, {p.constant1.value}')
        select.offset = p.constant0
        select.limit = p.constant1
        return select

    @_('select ORDER_BY ordering_terms')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'ORDER BY')
        select.order_by = p.ordering_terms
        return select

    @_('ordering_terms COMMA ordering_term')
    def ordering_terms(self, p):
        terms = p.ordering_terms
        terms.append(p.ordering_term)
        return terms

    @_('ordering_term')
    def ordering_terms(self, p):
        return [p.ordering_term]

    @_('ordering_term NULLS_FIRST')
    def ordering_term(self, p):
        p.ordering_term.nulls = p.NULLS_FIRST
        return p.ordering_term

    @_('ordering_term NULLS_LAST')
    def ordering_term(self, p):
        p.ordering_term.nulls = p.NULLS_LAST
        return p.ordering_term

    @_('identifier DESC')
    def ordering_term(self, p):
        return OrderBy(field=p.identifier, direction='DESC')

    @_('identifier ASC')
    def ordering_term(self, p):
        return OrderBy(field=p.identifier, direction='ASC')

    @_('identifier')
    def ordering_term(self, p):
        return OrderBy(field=p.identifier, direction='default')

    @_('select HAVING expr')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'HAVING')
        having = p.expr
        if not isinstance(having, Operation):
            raise ParsingException(
                f"HAVING must contain an operation that evaluates to a boolean, got: {str(having)}")
        select.having = having
        return select

    @_('select GROUP_BY expr_list')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'GROUP BY')
        group_by = p.expr_list
        if not isinstance(group_by, list):
            group_by = [group_by]

        select.group_by = group_by
        return select

    @_('select WHERE expr')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'WHERE')
        where_expr = p.expr
        if not isinstance(where_expr, Operation):
            raise ParsingException(
                f"WHERE must contain an operation that evaluates to a boolean, got: {str(where_expr)}")
        select.where = where_expr
        return select

    @_('select FROM from_table_aliased',
       'select FROM join_tables_implicit',
       'select FROM join_tables')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'FROM')
        select.from_table = p[2]
        return select

    # --- join ---
    @_('from_table_aliased join_clause from_table_aliased',
       'join_tables join_clause from_table_aliased')
    def join_tables(self, p):
        return Join(left=p[0],
                    right=p[2],
                    join_type=p.join_clause)

    @_('from_table_aliased join_clause from_table_aliased ON expr',
       'join_tables join_clause from_table_aliased ON expr')
    def join_tables(self, p):
        return Join(left=p[0],
                    right=p[2],
                    join_type=p.join_clause,
                    condition=p.expr)

    @_('from_table_aliased COMMA from_table_aliased',
       'join_tables_implicit COMMA from_table_aliased')
    def join_tables_implicit(self, p):
        return Join(left=p[0],
                    right=p[2],
                    join_type=JoinType.INNER_JOIN,
                    implicit=True)

    @_('from_table AS identifier',
       'from_table identifier',
       'from_table')
    def from_table_aliased(self, p):
        entity = p.from_table
        if hasattr(p, 'identifier'):
            entity.alias = p.identifier
        return entity

    @_('LPAREN query RPAREN')
    def from_table(self, p):
        query = p.query
        query.parentheses = True
        return query

    # keywords for table
    @_('PLUGINS',
       'ENGINES')
    def from_table(self, p):
        return Identifier.from_path_str(p[0])

    @_('identifier')
    def from_table(self, p):
        return p.identifier

    @_('parameter')
    def from_table(self, p):
        return p.parameter

    @_('JOIN',
       'LEFT JOIN',
       'RIGHT JOIN',
       'INNER JOIN',
       'FULL JOIN',
       'CROSS JOIN',
       'OUTER JOIN',
       )
    def join_clause(self, p):
        return ' '.join([x for x in p])

    @_('SELECT DISTINCT result_columns')
    def select(self, p):
        targets = p.result_columns
        return Select(targets=targets, distinct=True)

    @_('SELECT result_columns')
    def select(self, p):
        targets = p.result_columns
        return Select(targets=targets)

    @_('result_columns COMMA result_column')
    def result_columns(self, p):
        p.result_columns.append(p.result_column)
        return p.result_columns

    @_('result_column')
    def result_columns(self, p):
        return [p.result_column]

    @_('result_column AS identifier',
       'result_column identifier')
    def result_column(self, p):
        col = p.result_column
        if col.alias:
            raise ParsingException(f'Attempt to provide two aliases for {str(col)}')
        col.alias = p.identifier
        return col

    @_('LPAREN select RPAREN')
    def result_column(self, p):
        select = p.select
        select.parentheses = True
        return select

    @_('star')
    def result_column(self, p):
        return p.star

    @_('expr',
       'function',
       'window_function')
    def result_column(self, p):
        return p[0]

    # Window function
    @_('function OVER LPAREN window RPAREN')
    def window_function(self, p):

        return WindowFunction(
            function=p.function,
            order_by=p.window.get('order_by'),
            partition=p.window.get('partition'),
        )

    @_('window PARTITION_BY expr_list')
    def window(self, p):
        window = p.window
        part_by = p.expr_list
        if not isinstance(part_by, list):
            part_by = [part_by]

        window['partition'] = part_by
        return window

    @_('window ORDER_BY ordering_terms')
    def window(self, p):
        window = p.window
        window['order_by'] = p.ordering_terms
        return window

    @_('empty')
    def window(self, p):
        return {}

    # OPERATIONS

    @_('LPAREN select RPAREN')
    def expr(self, p):
        select = p.select
        select.parentheses = True
        return select

    @_('LPAREN expr RPAREN')
    def expr(self, p):
        if isinstance(p.expr, ASTNode):
            p.expr.parentheses = True
        return p.expr

    @_('id LPAREN expr FROM expr RPAREN')
    def function(self, p):
        return Function(op=p.id, args=[p.expr0], from_arg=p.expr1)

    @_('id LPAREN DISTINCT expr_list RPAREN')
    def function(self, p):
        return Function(op=p.id, distinct=True, args=p.expr_list)

    @_('id LPAREN expr_list_or_nothing RPAREN')
    def function(self, p):
        args = p.expr_list_or_nothing
        if not args:
            args = []
        return Function(op=p.id, args=args)

    # arguments are optional in functions, so that things like `select database()` are possible
    @_('expr BETWEEN expr AND expr')
    def expr(self, p):
        return BetweenOperation(args=(p.expr0, p.expr1, p.expr2))

    @_('expr_list')
    def expr_list_or_nothing(self, p):
        return p.expr_list

    @_('empty')
    def expr_list_or_nothing(self, p):
        pass

    @_('CAST LPAREN expr AS id LPAREN integer RPAREN RPAREN')
    def expr(self, p):
        return TypeCast(arg=p.expr, type_name=str(p.id), length=p.integer)

    @_('CAST LPAREN expr AS id RPAREN')
    def expr(self, p):
        return TypeCast(arg=p.expr, type_name=str(p.id))

    @_('enumeration')
    def expr_list(self, p):
        return p.enumeration

    @_('expr')
    def expr_list(self, p):
        return [p.expr]

    @_('LPAREN enumeration RPAREN')
    def expr(self, p):
        tup = Tuple(items=p.enumeration)
        return tup

    @_('STAR')
    def star(self, p):
        return Star()

    @_('expr NOT IN expr')
    def expr(self, p):
        op = p[1] + ' ' + p[2]
        return BinaryOperation(op=op, args=(p.expr0, p.expr1))

    @_('expr PLUS expr',
       'expr MINUS expr',
       'expr STAR expr',
       'expr DIVIDE expr',
       'expr MODULO expr',
       'expr EQUALS expr',
       'expr NEQUALS expr',
       'expr GEQ expr',
       'expr GREATER expr',
       'expr LEQ expr',
       'expr LESS expr',
       'expr AND expr',
       'expr OR expr',
       'expr IS_NOT expr',
       'expr NOT expr',
       'expr IS expr',
       'expr LIKE expr',
       'expr CONCAT expr',
       'expr IN expr')
    def expr(self, p):
        return BinaryOperation(op=p[1], args=(p.expr0, p.expr1))


    @_('MINUS expr %prec UMINUS',
       'NOT expr %prec UNOT', )
    def expr(self, p):
        return UnaryOperation(op=p[0], args=(p.expr,))

    # update fields list
    @_('update_parameter',
       'update_parameter_list COMMA update_parameter')
    def update_parameter_list(self, p):
        params = getattr(p, 'update_parameter_list', {})
        params.update(p.update_parameter)
        return params

    @_('id EQUALS expr')
    def update_parameter(self, p):
        return {p.id: p.expr}

    # EXPRESSIONS

    @_('enumeration COMMA expr')
    def enumeration(self, p):
        return p.enumeration + [p.expr]

    @_('expr COMMA expr')
    def enumeration(self, p):
        return [p.expr0, p.expr1]

    @_('identifier')
    def expr(self, p):
        return p.identifier

    @_('parameter')
    def expr(self, p):
        return p.parameter

    @_('constant')
    def expr(self, p):
        return p.constant

    @_('NULL')
    def constant(self, p):
        return NullConstant()

    @_('TRUE')
    def constant(self, p):
        return Constant(value=True)

    @_('FALSE')
    def constant(self, p):
        return Constant(value=False)

    @_('integer')
    def constant(self, p):
        return Constant(value=int(p.integer))

    @_('float')
    def constant(self, p):
        return Constant(value=float(p.float))

    @_('string')
    def constant(self, p):
        return Constant(value=str(p[0]))

    @_('identifier DOT identifier')
    def identifier(self, p):
        p.identifier0.parts += p.identifier1.parts
        return p.identifier0

    @_('id')
    def identifier(self, p):
        value = p[0]
        return Identifier.from_path_str(value)

    @_('quote_string',
       'dquote_string')
    def string(self, p):
        return p[0]

    @_('PARAMETER')
    def parameter(self, p):
        return Parameter(value=p.PARAMETER)

    # convert to types
    @_('ID',
       'CHARSET',
       'TABLES',
       'STATUS',
       'VIEW',
       'DATABASES',
       'DATABASE',
       'SCHEMA',
       'FIELDS',
       'EXTENDED',
       'PROCESSLIST',
       'MUTEX',
       'CODE',
       'SLAVE',
       'REPLICA',
       'REPLICAS',
       'CHANNEL',
       'TRIGGERS',
       'STORAGE',
       'LOGS',
       'MASTER',
       'KEYS',
       'PRIVILEGES',
       'PROFILES',
       'HOSTS',
       'OPEN',
       'INDEXES',)
    def id(self, p):
        return p[0]

    @_('FLOAT')
    def float(self, p):
        return float(p[0])

    @_('INTEGER')
    def integer(self, p):
        return int(p[0])

    @_('QUOTE_STRING')
    def quote_string(self, p):
        return p[0].strip('\'')

    @_('DQUOTE_STRING')
    def dquote_string(self, p):
        return p[0].strip('\"')

    @_('')
    def empty(self, p):
        pass

    def error(self, p, expected_tokens=None):
        if p:
            raise ParsingException(f"Syntax error at token {p.type}: \"{p.value}\"")
        else:
            raise ParsingException("Syntax error at EOF")
