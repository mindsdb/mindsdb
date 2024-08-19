# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.


def normalize(value):
    """Returns the string with decimal separators normalized."""
    return value.replace(",", ".")
