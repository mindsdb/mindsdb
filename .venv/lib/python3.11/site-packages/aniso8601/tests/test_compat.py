# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

import unittest

from aniso8601.compat import PY2, is_string


class TestCompatFunctions(unittest.TestCase):
    def test_is_string(self):
        self.assertTrue(is_string("asdf"))
        self.assertTrue(is_string(""))

        # pylint: disable=undefined-variable
        if PY2 is True:
            self.assertTrue(is_string(unicode("asdf")))

        self.assertFalse(is_string(None))
        self.assertFalse(is_string(123))
        self.assertFalse(is_string(4.56))
        self.assertFalse(is_string([]))
        self.assertFalse(is_string({}))
