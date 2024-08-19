#!/usr/bin/env python

# Author: Leonardo Gama (@leogama)
# Copyright (c) 2022 The Uncertainty Quantification Foundation.
# License: 3-clause BSD.  The full license text is available at:
#  - https://github.com/uqfoundation/dill/blob/master/LICENSE

import logging
import re
import tempfile

import dill
from dill import detect
from dill.logger import stderr_handler, adapter as logger

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

test_obj = {'a': (1, 2), 'b': object(), 'f': lambda x: x**2, 'big': list(range(10))}

def test_logging(should_trace):
    buffer = StringIO()
    handler = logging.StreamHandler(buffer)
    logger.addHandler(handler)
    try:
        dill.dumps(test_obj)
        if should_trace:
            regex = re.compile(r'(\S*┬ \w.*[^)]'              # begin pickling object
                               r'|│*└ # \w.* \[\d+ (\wi)?B])' # object written (with size)
                               )
            for line in buffer.getvalue().splitlines():
                assert regex.fullmatch(line)
            return buffer.getvalue()
        else:
            assert buffer.getvalue() == ""
    finally:
        logger.removeHandler(handler)
        buffer.close()

def test_trace_to_file(stream_trace):
    file = tempfile.NamedTemporaryFile(mode='r')
    with detect.trace(file.name, mode='w'):
        dill.dumps(test_obj)
    file_trace = file.read()
    file.close()
    # Apparently, objects can change location in memory...
    reghex = re.compile(r'0x[0-9A-Za-z]+')
    file_trace, stream_trace = reghex.sub('0x', file_trace), reghex.sub('0x', stream_trace)
    # PyPy prints dictionary contents with repr(dict)...
    regdict = re.compile(r'(dict\.__repr__ of ).*')
    file_trace, stream_trace = regdict.sub(r'\1{}>', file_trace), regdict.sub(r'\1{}>', stream_trace)
    assert file_trace == stream_trace

if __name__ == '__main__':
    logger.removeHandler(stderr_handler)
    test_logging(should_trace=False)
    detect.trace(True)
    test_logging(should_trace=True)
    detect.trace(False)
    test_logging(should_trace=False)

    loglevel = logging.ERROR
    logger.setLevel(loglevel)
    with detect.trace():
        stream_trace = test_logging(should_trace=True)
    test_logging(should_trace=False)
    assert logger.getEffectiveLevel() == loglevel
    test_trace_to_file(stream_trace)
