#!/usr/bin/env python
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Copyright (c) 2018-2022 The Uncertainty Quantification Foundation.
# License: 3-clause BSD.  The full license text is available at:
#  - https://github.com/uqfoundation/dill/blob/master/LICENSE

import glob
import os
import sys
import subprocess as sp
python = sys.executable
try:
    import pox
    python = pox.which_python(version=True) or python
except ImportError:
    pass
shell = sys.platform[:3] == 'win'

suite = os.path.dirname(__file__) or os.path.curdir
tests = glob.glob(suite + os.path.sep + 'test_*.py')


if __name__ == '__main__':

    failed = 0
    for test in tests:
        p = sp.Popen([python, test], shell=shell).wait()
        if p:
            print('F', end='', flush=True)
            failed = 1
        else:
            print('.', end='', flush=True)
    print('')
    exit(failed)
