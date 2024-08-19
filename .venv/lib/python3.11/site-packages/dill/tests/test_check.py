#!/usr/bin/env python
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Copyright (c) 2008-2016 California Institute of Technology.
# Copyright (c) 2016-2022 The Uncertainty Quantification Foundation.
# License: 3-clause BSD.  The full license text is available at:
#  - https://github.com/uqfoundation/dill/blob/master/LICENSE

from dill import check
import sys

from dill.temp import capture


#FIXME: this doesn't catch output... it's from the internal call
def raise_check(func, **kwds):
    try:
        with capture('stdout') as out:
            check(func, **kwds)
    except Exception:
        e = sys.exc_info()[1]
        raise AssertionError(str(e))
    else:
        assert 'Traceback' not in out.getvalue()
    finally:
        out.close()


f = lambda x:x**2


def test_simple(verbose=None):
    raise_check(f, verbose=verbose)


def test_recurse(verbose=None):
    raise_check(f, recurse=True, verbose=verbose)


def test_byref(verbose=None):
    raise_check(f, byref=True, verbose=verbose)


def test_protocol(verbose=None):
    raise_check(f, protocol=True, verbose=verbose)


def test_python(verbose=None):
    raise_check(f, python=None, verbose=verbose)


#TODO: test incompatible versions
#TODO: test dump failure
#TODO: test load failure


if __name__ == '__main__':
    test_simple()
    test_recurse()
    test_byref()
    test_protocol()
    test_python()
