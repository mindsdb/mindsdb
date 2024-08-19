#!/usr/bin/env python
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Author: Anirudh Vegesana (avegesan@cs.stanford.edu)
# Copyright (c) 2021-2022 The Uncertainty Quantification Foundation.
# License: 3-clause BSD.  The full license text is available at:
#  - https://github.com/uqfoundation/dill/blob/master/LICENSE

import dill
from dill._dill import OLD310, MAPPING_PROXY_TRICK, DictProxyType

def test_dictproxy():
    assert dill.copy(DictProxyType({'a': 2}))

def test_dictviews():
    x = {'a': 1}
    assert dill.copy(x.keys())
    assert dill.copy(x.values())
    assert dill.copy(x.items())

def test_dictproxy_trick():
    if not OLD310 and MAPPING_PROXY_TRICK:
        x = {'a': 1}
        all_views = (x.values(), x.items(), x.keys(), x)
        seperate_views = dill.copy(all_views)
        new_x = seperate_views[-1]
        new_x['b'] = 2
        new_x['c'] = 1
        assert len(new_x) == 3 and len(x) == 1
        assert len(seperate_views[0]) == 3 and len(all_views[0]) == 1
        assert len(seperate_views[1]) == 3 and len(all_views[1]) == 1
        assert len(seperate_views[2]) == 3 and len(all_views[2]) == 1
        assert dict(all_views[1]) == x
        assert dict(seperate_views[1]) == new_x

if __name__ == '__main__':
    test_dictproxy()
    test_dictviews()
    test_dictproxy_trick()
