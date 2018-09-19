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

def splitRecursive(word, tokens):
    words = [word]
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
    print(splitRecursive('ABC.C HELLO, one:123.45 67', WORD_SEPARATORS))

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()

