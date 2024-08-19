import dill
from dill._objects import failures, registered, succeeds
import warnings
warnings.filterwarnings('ignore')

def check(d, ok=True):
    res = []
    for k,v in d.items():
        try:
            z = dill.copy(v)
            if ok: res.append(k)
        except:
            if not ok: res.append(k)
    return res

fails = check(failures)
try:
    assert not bool(fails)
except AssertionError as e:
    print("FAILS: %s" % fails)
    raise e from None

register = check(registered, ok=False)
try:
    assert not bool(register)
except AssertionError as e:
    print("REGISTER: %s" % register)
    raise e from None

success = check(succeeds, ok=False)
try:
    assert not bool(success)
except AssertionError as e:
    print("SUCCESS: %s" % success)
    raise e from None

import builtins
import types
q = dill._dill._reverse_typemap
p = {k:v for k,v in q.items() if k not in vars(builtins) and k not in vars(types)}

diff = set(p.keys()).difference(registered.keys())
try:
    assert not bool(diff)
except AssertionError as e:
    print("DIFF: %s" % diff)
    raise e from None

miss = set(registered.keys()).difference(p.keys())
try:
    assert not bool(miss)
except AssertionError as e:
    print("MISS: %s" % miss)
    raise e from None
