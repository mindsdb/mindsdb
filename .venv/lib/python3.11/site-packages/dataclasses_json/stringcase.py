# The MIT License (MIT)
#
# Copyright (c) 2015 Taka Okunishi
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Copyright © 2015-2018 Taka Okunishi <okunishinishi@gmail.com>.
# Copyright © 2020 Louis-Philippe Véronneau <pollo@debian.org>

import re


def uplowcase(string, case):
    """Convert string into upper or lower case.

    Args:
        string: String to convert.

    Returns:
        string: Uppercase or lowercase case string.

    """
    if case == 'up':
        return str(string).upper()
    elif case == 'low':
        return str(string).lower()


def capitalcase(string):
    """Convert string into capital case.
    First letters will be uppercase.

    Args:
        string: String to convert.

    Returns:
        string: Capital case string.

    """

    string = str(string)
    if not string:
        return string
    return uplowcase(string[0], 'up') + string[1:]


def camelcase(string):
    """ Convert string into camel case.

    Args:
        string: String to convert.

    Returns:
        string: Camel case string.

    """

    string = re.sub(r"^[\-_\.]", '', str(string))
    if not string:
        return string
    return (uplowcase(string[0], 'low')
            + re.sub(r"[\-_\.\s]([a-z0-9])",
                     lambda matched: uplowcase(matched.group(1), 'up'),
                     string[1:]))


def snakecase(string):
    """Convert string into snake case.
    Join punctuation with underscore

    Args:
        string: String to convert.

    Returns:
        string: Snake cased string.

    """

    string = re.sub(r"[\-\.\s]", '_', str(string))
    if not string:
        return string
    return (uplowcase(string[0], 'low')
            + re.sub(r"[A-Z0-9]",
                     lambda matched: '_' + uplowcase(matched.group(0), 'low'),
                     string[1:]))


def spinalcase(string):
    """Convert string into spinal case.
    Join punctuation with hyphen.

    Args:
        string: String to convert.

    Returns:
        string: Spinal cased string.

    """

    return re.sub(r"_", "-", snakecase(string))


def pascalcase(string):
    """Convert string into pascal case.

    Args:
        string: String to convert.

    Returns:
        string: Pascal case string.

    """

    return capitalcase(camelcase(string))
