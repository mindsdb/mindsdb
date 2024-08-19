# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

import sys

PY2 = sys.version_info[0] == 2

if PY2:
    import mock  # pylint: disable=import-error
else:
    from unittest import mock
