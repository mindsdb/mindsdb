#!/usr/bin/env python
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Copyright (c) 2019-2022 The Uncertainty Quantification Foundation.
# License: 3-clause BSD.  The full license text is available at:
#  - https://github.com/uqfoundation/dill/blob/master/LICENSE

import dill
from functools import partial
import warnings


def copy(obj, byref=False, recurse=False):
    if byref:
        try:
            return dill.copy(obj, byref=byref, recurse=recurse)
        except Exception:
            pass
        else:
            raise AssertionError('Copy of %s with byref=True should have given a warning!' % (obj,))

        warnings.simplefilter('ignore')
        val = dill.copy(obj, byref=byref, recurse=recurse)
        warnings.simplefilter('error')
        return val
    else:
        return dill.copy(obj, byref=byref, recurse=recurse)


class obj1(object):
    def __init__(self):
        super(obj1, self).__init__()

class obj2(object):
    def __init__(self):
        super(obj2, self).__init__()

class obj3(object):
    super_ = super
    def __init__(self):
        obj3.super_(obj3, self).__init__()


def test_super():
    assert copy(obj1(), byref=True)
    assert copy(obj1(), byref=True, recurse=True)
    assert copy(obj1(), recurse=True)
    assert copy(obj1())

    assert copy(obj2(), byref=True)
    assert copy(obj2(), byref=True, recurse=True)
    assert copy(obj2(), recurse=True)
    assert copy(obj2())

    assert copy(obj3(), byref=True)
    assert copy(obj3(), byref=True, recurse=True)
    assert copy(obj3(), recurse=True)
    assert copy(obj3())


def get_trigger(model):
    pass

class Machine(object):
    def __init__(self):
        self.child = Model()
        self.trigger = partial(get_trigger, self)
        self.child.trigger = partial(get_trigger, self.child)

class Model(object):
    pass



def test_partial():
    assert copy(Machine(), byref=True)
    assert copy(Machine(), byref=True, recurse=True)
    assert copy(Machine(), recurse=True)
    assert copy(Machine())


class Machine2(object):
    def __init__(self):
        self.go = partial(self.member, self)
    def member(self, model):
        pass


class SubMachine(Machine2):
    def __init__(self):
        super(SubMachine, self).__init__()


def test_partials():
    assert copy(SubMachine(), byref=True)
    assert copy(SubMachine(), byref=True, recurse=True)
    assert copy(SubMachine(), recurse=True)
    assert copy(SubMachine())


class obj4(object):
    def __init__(self):
        super(obj4, self).__init__()
        a = self
        class obj5(object):
            def __init__(self):
                super(obj5, self).__init__()
                self.a = a
        self.b = obj5()


def test_circular_reference():
    assert copy(obj4())
    obj4_copy = dill.loads(dill.dumps(obj4()))
    assert type(obj4_copy) is type(obj4_copy).__init__.__closure__[0].cell_contents
    assert type(obj4_copy.b) is type(obj4_copy.b).__init__.__closure__[0].cell_contents


def f():
    def g():
       return g
    return g


def test_function_cells():
    assert copy(f())


def fib(n):
    assert n >= 0
    if n <= 1:
        return n
    else:
        return fib(n-1) + fib(n-2)


def test_recursive_function():
    global fib
    fib2 = copy(fib, recurse=True)
    fib3 = copy(fib)
    fib4 = fib
    del fib
    assert fib2(5) == 5
    for _fib in (fib3, fib4):
        try:
            _fib(5)
        except Exception:
            # This is expected to fail because fib no longer exists
            pass
        else:
            raise AssertionError("Function fib shouldn't have been found")
    fib = fib4


def collection_function_recursion():
    d = {}
    def g():
        return d
    d['g'] = g
    return g


def test_collection_function_recursion():
    g = copy(collection_function_recursion())
    assert g()['g'] is g


if __name__ == '__main__':
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        test_super()
        test_partial()
        test_partials()
        test_circular_reference()
        test_function_cells()
        test_recursive_function()
        test_collection_function_recursion()
