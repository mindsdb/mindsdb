import re
from collections import deque

from walrus.containers import Set
from walrus.containers import ZSet


OP_AND = 'and'
OP_OR = 'or'
OP_EQ = '=='
OP_NE = '!='
OP_LT = '<'
OP_LTE = '<='
OP_GT = '>'
OP_GTE = '>='
OP_BETWEEN = 'between'
OP_MATCH = 'match'

ABSOLUTE = set([OP_EQ, OP_NE])
CONTINUOUS = set([OP_LT, OP_LTE, OP_GT, OP_GTE, OP_BETWEEN])
FTS = set([OP_MATCH])


class Lexer(object):
    def __init__(self, query, default_conjunction='AND'):
        self.query = query
        self.default_conjunction = default_conjunction

        def yield_symbol(symbol_type):
            def callback(scanner, token):
                return (symbol_type, token)
            return callback

        def yield_string(scanner, token):
            return ('STRING', token[1:-1].lower())

        def yield_simple_string(scanner, token):
            return ('STRING', token.lower())

        self.scanner = re.Scanner([
            (r'"[^\n"\\]*(?:\\.[^\n"\\]*)*"', yield_string),
            (r"'[^\n'\\]*(?:\\.[^\n'\\]*)*'", yield_string),
            (r'\bAND\b', yield_symbol('AND')),
            (r'\bOR\b', yield_symbol('OR')),
            (r'[@_\-\w]+', yield_simple_string),
            (r'&', yield_symbol('AND')),
            (r'\|', yield_symbol('OR')),
            (r'\(', yield_symbol('LPAREN')),
            (r'\)', yield_symbol('RPAREN')),
            (r'\s+', None),
        ], re.U)

    def lex(self):
        symbols, _ = self.scanner.scan(self.query)
        last = None
        for (symbol, sval) in symbols:
            if symbol == 'STRING' and last == 'STRING':
                # Handle default conjunctions.
                yield self.default_conjunction, None
            yield symbol, sval
            last = symbol


class BaseSymbol(object):
    """Base-class for a symbol in the AST."""
    __slots__ = []

    def code(self):
        raise NotImplementedError


class Symbol(BaseSymbol):
    """An interior node of the AST, with left and optionally right children."""
    __slots__ = ['left', 'right']

    def __init__(self, left, right):
        self.left = left
        self.right = right


class Leaf(BaseSymbol):
    """Leaf node of the AST, with a value."""
    __slots__ = ['value']

    def __init__(self, value):
        self.value = value

    def code(self):
        return lambda f, s: Expression(f, OP_MATCH, self.value)


class And(Symbol):
    def code(self):
        return lambda f, s: Expression(
            self.left.code()(f, s),
            OP_AND,
            self.right.code()(f, s))


class Or(Symbol):
    def code(self):
        return lambda f, s: Expression(
            self.left.code()(f, s),
            OP_OR,
            self.right.code()(f, s))


class Parser(object):
    def __init__(self, lexer):
        self.lexer = lexer
        self.symbol_stream = lexer.lex()
        self.root = None
        self.current = None
        self.finished = False

    def get_symbol(self):
        try:
            self.current, self.sval = next(self.symbol_stream)
        except StopIteration:
            self.finished = True
        return self.current

    def parse(self):
        self._expression()
        if not self.finished:
            raise ValueError('Malformed expression: %s.' % self.lexer.query)
        return self.root

    def _expression(self):
        self._term()
        while (self.current == 'OR'):
            left = self.root
            self._term()
            self.root = Or(left, self.root)

    def _term(self):
        self._factor()
        while (self.current == 'AND'):
            left = self.root
            self._factor()
            self.root = And(left, self.root)

    def _factor(self):
        symbol = self.get_symbol()
        if symbol == 'STRING':
            self.root = Leaf(self.sval)
            self.get_symbol()
        elif symbol == 'LPAREN':
            self._expression()
            self.get_symbol()
        else:
            raise ValueError('Malformed expression: %s.' % self.lexer.query)


def parse(s, field, default_conjunction='AND'):
    if not s.strip():
        return None
    lexer = Lexer(s, default_conjunction=default_conjunction)
    parser = Parser(lexer)
    ast = parser.parse()
    return ast.code()(field, s)


class Node(object):
    def __init__(self):
        self._ordering = None

    def desc(self):
        return Desc(self)

    def between(self, low, high):
        return Expression(self, OP_BETWEEN, (low, high))

    def match(self, term):
        return Expression(self, OP_MATCH, term)

    def search(self, search_query, default_conjunction=OP_AND):
        return parse(search_query, self, default_conjunction)

    def _e(op, inv=False):
        def inner(self, rhs):
            if inv:
                return Expression(rhs, op, self)
            return Expression(self, op, rhs)
        return inner
    __and__ = _e(OP_AND)
    __or__ = _e(OP_OR)
    __rand__ = _e(OP_AND, inv=True)
    __ror__ = _e(OP_OR, inv=True)
    __eq__ = _e(OP_EQ)
    __ne__ = _e(OP_NE)
    __lt__ = _e(OP_LT)
    __le__ = _e(OP_LTE)
    __gt__ = _e(OP_GT)
    __ge__ = _e(OP_GTE)


class Desc(Node):
    def __init__(self, node):
        self.node = node


class Expression(Node):
    def __init__(self, lhs, op, rhs):
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def __repr__(self):
        return '(%s %s %s)' % (self.lhs, self.op, self.rhs)


class Executor(object):
    """
    Given an arbitrarily complex expression, recursively execute
    it and return the resulting set (or sorted set). The set will
    correspond to the primary hash keys of matching objects.

    The executor works *only on fields with secondary indexes* or
    the global "all" index created for all models.
    """
    def __init__(self, database, temp_key_expire=15):
        self.database = database
        self.temp_key_expire = temp_key_expire
        self._mapping = {
            OP_OR: self.execute_or,
            OP_AND: self.execute_and,
            OP_EQ: self.execute_eq,
            OP_NE: self.execute_ne,
            OP_GT: self.execute_gt,
            OP_GTE: self.execute_gte,
            OP_LT: self.execute_lt,
            OP_LTE: self.execute_lte,
            OP_BETWEEN: self.execute_between,
            OP_MATCH: self.execute_match,
        }

    def execute(self, expression):
        op = expression.op
        return self._mapping[op](expression.lhs, expression.rhs)

    def execute_eq(self, lhs, rhs):
        index = lhs.get_index(OP_EQ)
        return index.get_key(lhs.db_value(rhs))

    def execute_ne(self, lhs, rhs):
        all_set = lhs.model_class._query.all_index()
        index = lhs.get_index(OP_NE)
        exclude_set = index.get_key(lhs.db_value(rhs))
        tmp_set = all_set.diffstore(self.database.get_temp_key(), exclude_set)
        tmp_set.expire(self.temp_key_expire)
        return tmp_set

    def _zset_score_filter(self, zset, low, high):
        tmp_set = self.database.Set(self.database.get_temp_key())
        self.database.run_script(
            'zset_score_filter',
            keys=[zset.key, tmp_set.key],
            args=[low, high])
        tmp_set.expire(self.temp_key_expire)
        return tmp_set

    def execute_between(self, lhs, rhs):
        index = lhs.get_index(OP_BETWEEN)
        low, high = map(lhs.db_value, rhs)
        zset = index.get_key(None)  # No value necessary.
        return self._zset_score_filter(zset, low, high)

    def execute_lte(self, lhs, rhs):
        index = lhs.get_index(OP_LTE)
        db_value = lhs.db_value(rhs)
        zset = index.get_key(db_value)
        return self._zset_score_filter(zset, float('-inf'), db_value)

    def execute_gte(self, lhs, rhs):
        index = lhs.get_index(OP_GTE)
        db_value = lhs.db_value(rhs)
        zset = index.get_key(db_value)
        return self._zset_score_filter(zset, db_value, float('inf'))

    def execute_lt(self, lhs, rhs):
        index = lhs.get_index(OP_LTE)
        db_value = lhs.db_value(rhs)
        zset = index.get_key(db_value)
        return self._zset_score_filter(zset, float('-inf'), '(%s' % db_value)

    def execute_gt(self, lhs, rhs):
        index = lhs.get_index(OP_GTE)
        db_value = lhs.db_value(rhs)
        zset = index.get_key(db_value)
        return self._zset_score_filter(zset, '(%s' % db_value, float('inf'))

    def execute_match(self, lhs, rhs):
        index = lhs.get_index(OP_MATCH)
        db_value = lhs.db_value(rhs)
        words = index.tokenizer.tokenize(db_value)
        index_keys = []
        for word in words:
            index_keys.append(index.get_key(word).key)

        results = self.database.ZSet(self.database.get_temp_key())
        if index_keys:
            self.database.zinterstore(results.key, index_keys)
        results.expire(self.temp_key_expire)

        return results

    def _combine_sets(self, lhs, rhs, operation):
        if not isinstance(lhs, (Set, ZSet)):
            lhs = self.execute(lhs)
        if not isinstance(rhs, (Set, ZSet)):
            rhs = self.execute(rhs)

        source, dest = lhs, rhs
        if type(lhs) != type(rhs):
            # We'll perform the operation using the ZSet, as you can't call
            # SINTERSTORE or SUNIONSTORE with a ZSet.
            if isinstance(rhs, ZSet):
                source, dest = rhs, lhs

        if operation == 'AND':
            method = source.interstore
        elif operation == 'OR':
            method = source.unionstore
        else:
            raise ValueError('Unrecognized operation: "%s".' % operation)

        tmp_set = method(self.database.get_temp_key(), dest)
        tmp_set.expire(self.temp_key_expire)
        return tmp_set

    def execute_or(self, lhs, rhs):
        return self._combine_sets(lhs, rhs, 'OR')

    def execute_and(self, lhs, rhs):
        return self._combine_sets(lhs, rhs, 'AND')
