#!/usr/bin/env python
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Copyright (c) 2008-2016 California Institute of Technology.
# Copyright (c) 2016-2022 The Uncertainty Quantification Foundation.
# License: 3-clause BSD.  The full license text is available at:
#  - https://github.com/uqfoundation/dill/blob/master/LICENSE

import dill
dill.settings['recurse'] = True
import weakref

class _class:
    def _method(self):
        pass

class _callable_class:
    def __call__(self):
        pass

def _function():
    pass


def test_weakref():
    o = _class()
    oc = _callable_class()
    f = _function
    x = _class

    # ReferenceType
    r = weakref.ref(o)
    d_r = weakref.ref(_class())
    fr = weakref.ref(f)
    xr = weakref.ref(x)

    # ProxyType
    p = weakref.proxy(o)
    d_p = weakref.proxy(_class())

    # CallableProxyType
    cp = weakref.proxy(oc)
    d_cp = weakref.proxy(_callable_class())
    fp = weakref.proxy(f)
    xp = weakref.proxy(x)

    objlist = [r,d_r,fr,xr, p,d_p, cp,d_cp,fp,xp]
    #dill.detect.trace(True)

    for obj in objlist:
      res = dill.detect.errors(obj)
      if res:
        print ("%r:\n  %s" % (obj, res))
    # else:
    #   print ("PASS: %s" % obj)
      assert not res

def test_dictproxy():
    from dill._dill import DictProxyType
    try:
        m = DictProxyType({"foo": "bar"})
    except Exception:
        m = type.__dict__
    mp = dill.copy(m)   
    assert mp.items() == m.items()


if __name__ == '__main__':
    test_weakref()
    from dill._dill import IS_PYPY
    if not IS_PYPY:
        test_dictproxy()
