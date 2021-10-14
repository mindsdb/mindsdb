import re
import operator


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


def f_ne(a, b):
    if a is None or b is None:
        return False
    return operator.ne(a, b)


def f_eq(a, b):
    if a is None or b is None:
        return False
    return operator.eq(a, b)


operator_map = {
    '+': f_add,
    '-': operator.sub,
    '/': operator.truediv,
    '*': operator.mul,
    '%': operator.mod,
    '=': f_eq,
    '!=': f_ne,
    '>': operator.gt,
    '<': operator.lt,
    '>=': operator.ge,
    '<=': operator.le,
    'IS': operator.eq,
    'IS NOT': operator.ne,
    'LIKE': f_like,
    'NOT LIKE': lambda s, p: not f_like(s, p),
    'IN': lambda v, l: v in l,
    'NOT IN': lambda v, l: v not in l,
    'AND': f_and,
    'OR': f_or,
    '||': f_add
    # binary and, binary not, exists, missing, etc
}
