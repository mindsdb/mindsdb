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


def get_file_type(data):
    # try to guess if its an excel file
    xlsx_sig = b'\x50\x4B\x05\06'
    xlsx_sig2 = b'\x50\x4B\x03\x04'
    xls_sig = b'\x09\x08\x10\x00\x00\x06\x05\x00'

    # differnt whence, offset, size for different types
    excel_meta = [ ('xls', 0, 512, 8), ('xlsx', 2, -22, 4)]

    for filename, whence, offset, size in excel_meta:

        try:
            data.seek(offset, whence)  # Seek to the offset.
            bytes = data.read(size)  # Capture the specified number of bytes.
            data.seek(0)
            codecs.getencoder('hex')(bytes)

            if bytes == xls_sig:
                return 'xls', dialect
            elif bytes == xlsx_sig:
                return 'xlsx', dialect

        except:
            data.seek(0)

    # if not excel it can be a json file or a CSV, convert from binary to stringio

    byte_str = data.read()
    # Move it to StringIO
    try:
        data = StringIO(byte_str.decode('UTF-8'))
    except:
        log.error(traceback.format_exc())
        log.error('Could not load into string')

    # see if its JSON
    buffer = data.read(100)
    data.seek(0)
    text = buffer.strip()
    # analyze first n characters
    if len(text) > 0:
        text = text.strip()
        # it it looks like a json, then try to parse it
        if text != "" and ((text[0] == "{") or (text[0] == "[")):
            try:
                json.loads(data.read())
                data.seek(0)
                return 'json', dialect
            except:
                data.seek(0)
                return None, dialect

    # lets try to figure out if its a csv
    data.seek(0)
    first_few_lines = []
    i = 0
    for line in data:
        i += 1
        first_few_lines.append(line)
        if i > 0:
            break

    accepted_delimiters = [',','\t']
    dialect = csv.Sniffer().sniff(''.join(first_few_lines[0]), delimiters=accepted_delimiters)
    data.seek(0)
    # if csv dialect identified then return csv
    if dialect:
        return 'csv', dialect
    else:
        return None, dialect



def test():
    log.info(fixFileIfPossible('/Users/jorge/Downloads/tweets (1).csv'))

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
