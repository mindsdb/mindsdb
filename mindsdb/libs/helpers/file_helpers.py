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

import csv
import sys
import traceback


def fixFileIfPossible(filepath):
    """
    Tries to fix a file header if it finds header or encoding issues
    :param filepath: the filepath to fix if possible
    :return: fixed, error
    """
    fixed = False
    error = False
    rows = []
    try:
        with open(filepath, newline='') as f:
            reader = csv.reader(f)
            header = None
            max_len = 0
            for row in reader:
                if header is None:
                    header = row
                    for i, col in enumerate(row):
                        if col in [None, '']:
                            fixed = True
                            header[i] = 'col_{i}'.format(i=i+1)
                rows += [row]
                length = int(len(row))
                if length > max_len:
                    max_len = length
                    log.info(max_len)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error = traceback.format_exception(exc_type, exc_value,
                                           exc_traceback)
        return fixed, error
    if len(header) < max_len or fixed == True:
        rightCell = lambda h, i: 'col_{i}'.format(i=i+1) if i > len(header) else h
        row = [rightCell(header_col, i) for i, header_col in enumerate(header)]
        rows[0] = row

        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(rows)

    return fixed, error




def test():
    log.info(fixFileIfPossible('/Users/jorge/Downloads/tweets (1).csv'))

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()