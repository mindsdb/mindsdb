#!/usr/bin/env python
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Copyright (c) 2019-2022 The Uncertainty Quantification Foundation.
# License: 3-clause BSD.  The full license text is available at:
#  - https://github.com/uqfoundation/dill/blob/master/LICENSE

import functools
import dill
import sys
dill.settings['recurse'] = True


def function_a(a):
    return a


def function_b(b, b1):
    return b + b1


def function_c(c, c1=1):
    return c + c1


def function_d(d, d1, d2=1):
    """doc string"""
    return d + d1 + d2

function_d.__module__ = 'a module'


exec('''
def function_e(e, *e1, e2=1, e3=2):
    return e + sum(e1) + e2 + e3''')

globalvar = 0

@functools.lru_cache(None)
def function_with_cache(x):
    global globalvar
    globalvar += x
    return globalvar


def function_with_unassigned_variable():
    if False:
        value = None
    return (lambda: value)


def test_issue_510():
    # A very bizzare use of functions and methods that pickle doesn't get
    # correctly for odd reasons.
    class Foo:
        def __init__(self):
                def f2(self):
                        return self
                self.f2 = f2.__get__(self)

    import dill, pickletools
    f = Foo()
    f1 = dill.copy(f)
    assert f1.f2() is f1


def test_functions():
    dumped_func_a = dill.dumps(function_a)
    assert dill.loads(dumped_func_a)(0) == 0

    dumped_func_b = dill.dumps(function_b)
    assert dill.loads(dumped_func_b)(1,2) == 3

    dumped_func_c = dill.dumps(function_c)
    assert dill.loads(dumped_func_c)(1) == 2
    assert dill.loads(dumped_func_c)(1, 2) == 3

    dumped_func_d = dill.dumps(function_d)
    assert dill.loads(dumped_func_d).__doc__ == function_d.__doc__
    assert dill.loads(dumped_func_d).__module__ == function_d.__module__
    assert dill.loads(dumped_func_d)(1, 2) == 4
    assert dill.loads(dumped_func_d)(1, 2, 3) == 6
    assert dill.loads(dumped_func_d)(1, 2, d2=3) == 6

    function_with_cache(1)
    globalvar = 0
    dumped_func_cache = dill.dumps(function_with_cache)
    assert function_with_cache(2) == 3
    assert function_with_cache(1) == 1
    assert function_with_cache(3) == 6
    assert function_with_cache(2) == 3

    empty_cell = function_with_unassigned_variable()
    cell_copy = dill.loads(dill.dumps(empty_cell))
    assert 'empty' in str(cell_copy.__closure__[0])
    try:
        cell_copy()
    except Exception:
        # this is good
        pass
    else:
        raise AssertionError('cell_copy() did not read an empty cell')

    exec('''
dumped_func_e = dill.dumps(function_e)
assert dill.loads(dumped_func_e)(1, 2) == 6
assert dill.loads(dumped_func_e)(1, 2, 3) == 9
assert dill.loads(dumped_func_e)(1, 2, e2=3) == 8
assert dill.loads(dumped_func_e)(1, 2, e2=3, e3=4) == 10
assert dill.loads(dumped_func_e)(1, 2, 3, e2=4) == 12
assert dill.loads(dumped_func_e)(1, 2, 3, e2=4, e3=5) == 15''')

def test_code_object():
    from dill._dill import ALL_CODE_PARAMS, CODE_PARAMS, CODE_VERSION, _create_code
    code = function_c.__code__
    LNOTAB = getattr(code, 'co_lnotab', b'')
    fields = {f: getattr(code, 'co_'+f) for f in CODE_PARAMS}
    fields.setdefault('posonlyargcount', 0)         # python >= 3.8
    fields.setdefault('lnotab', LNOTAB)             # python <= 3.9
    fields.setdefault('linetable', b'')             # python >= 3.10
    fields.setdefault('qualname', fields['name'])   # python >= 3.11
    fields.setdefault('exceptiontable', b'')        # python >= 3.11
    fields.setdefault('endlinetable', None)         # python == 3.11a
    fields.setdefault('columntable', None)          # python == 3.11a

    for version, _, params in ALL_CODE_PARAMS:
        args = tuple(fields[p] for p in params.split())
        try:
            _create_code(*args)
            if version >= (3,10):
                _create_code(fields['lnotab'], *args)
        except Exception as error:
            raise Exception("failed to construct code object with format version {}".format(version)) from error

if __name__ == '__main__':
    test_functions()
    test_issue_510()
    test_code_object()
