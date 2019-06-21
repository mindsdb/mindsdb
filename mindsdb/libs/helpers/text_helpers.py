"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""

from mindsdb.libs.constants.mindsdb import *
import json
import hashlib
from dateutil.parser import parse as parse_datetime
import numpy

def clean_float(val):
    if type(val) in [type(int(1)), type(1.0)] :
        return float(val)

    if isinstance(val, numpy.float64) or isinstance(val, float) or isinstance(val, int):
        return val

    val = str(val)
    val = val.replace(',','.')
    val = val.rstrip('"').lstrip('"')

    if val == '' or val == 'None':
        return None

    return float(val)


def gen_chars(length, character):
    """
    # lambda to Generates a string consisting of `length` consiting of repeating `character`
    :param length:
    :param character:
    :return:
    """
    return ''.join([character for i in range(length)])

def cast_string_to_python_type(string):
    """ Returns an integer, float or a string from a string"""
    try:
        if string is None:
            return None
        return int(string)
    except:
        try:
            return clean_float(string)
        except ValueError:
            if string == '':
                return None
            else:
                return string

def splitRecursive(word, tokens):
    words = [str(word)]
    for token in tokens:
        new_split = []
        for word in words:
            new_split += word.split(token)
        words = new_split
    words = [word for word in words if word not in ['', None] ]
    return words

def hashtext(cell):
    text = json.dumps(cell)
    hash = hashlib.md5(text.encode('utf8')).hexdigest()
    return hash

def test():
    log.info(splitRecursive('ABC.C HELLO, one:123.45 67', WORD_SEPARATORS))

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
