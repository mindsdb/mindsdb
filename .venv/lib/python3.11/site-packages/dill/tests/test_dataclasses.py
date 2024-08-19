#!/usr/bin/env python
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Author: Anirudh Vegesana (avegesan@cs.stanford.edu)
# Copyright (c) 2022 The Uncertainty Quantification Foundation.
# License: 3-clause BSD.  The full license text is available at:
#  - https://github.com/uqfoundation/dill/blob/master/LICENSE
"""
test pickling a dataclass
"""

import dill
import dataclasses

def test_dataclasses():
    # Issue #500
    @dataclasses.dataclass
    class A:
        x: int
        y: str

    @dataclasses.dataclass
    class B:
        a: A

    a = A(1, "test")
    before = B(a)
    save = dill.dumps(before)
    after = dill.loads(save)
    assert before != after # classes don't match
    assert before == B(A(**dataclasses.asdict(after.a)))
    assert dataclasses.asdict(before) == dataclasses.asdict(after)

if __name__ == '__main__':
    test_dataclasses()
