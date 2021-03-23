from moz_sql_parser.keywords import join_keywords, binary_ops, unary_ops
import re
import operator

unary_ops = list(unary_ops.values())
binary_ops = list(binary_ops.values())

unary_ops.extend(['missing', 'exists'])
binary_ops.extend(['like', 'in', 'between'])


def f_and(*args):
    for i in args:
        if not i:
            return False
    return True


def f_or(*args):
    for i in args:
        if i:
            return True
    return False


def f_like(s, p):
    p = '^{}$'.format(p.replace('%', '[\s\S]*'))

    return re.match(p, s) is not None


def f_add(*args):
    # strings and numbers are supported
    # maybe it is not true sql-way

    out = args[0] + args[1]
    for i in args[2:]:
        out += i
    return out

def f_ne(a,b):
    if a is None or b is None: return False
    return operator.ne(a,b)

def f_eq(a,b):
    if a is None or b is None: return False
    return operator.eq(a,b)

operator_map = {
    'concat': operator.concat,
    'mul': operator.mul,
    'div': operator.truediv,
    'mod': operator.mod,
    'add': f_add,
    'sub': operator.sub,
    'binary_and': operator.and_,
    'binary_or': operator.or_,
    'lt': operator.lt,
    'lte': operator.le,
    'gt': operator.gt,
    'gte': operator.ge,
    'eq': f_eq,  # operator.eq,
    'neq': f_ne,  # operator.ne,
    'nin': lambda v, l: v not in l,
    'in': lambda v, l: v in l,
    'nlike': lambda s, p: not f_like(s, p),
    'like': f_like,
    'not_between': lambda v, a, b: v < a or v > b,
    'between': lambda v, a, b: v > a and v < b,
    'or': f_or,
    'and': f_and,

    'missing': lambda x: x is None,
    'exists': lambda x: x is not None,

    "neg": operator.neg,
    "binary_not": operator.inv,
}
