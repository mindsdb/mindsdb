#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: Leonardo Gama (@leogama)
# Copyright (c) 2022 The Uncertainty Quantification Foundation.
# License: 3-clause BSD.  The full license text is available at:
#  - https://github.com/uqfoundation/dill/blob/master/LICENSE
"""
Logging utilities for dill.

The 'logger' object is dill's top-level logger.

The 'adapter' object wraps the logger and implements a 'trace()' method that
generates a detailed tree-style trace for the pickling call at log level INFO.

The 'trace()' function sets and resets dill's logger log level, enabling and
disabling the pickling trace.

The trace shows a tree structure depicting the depth of each object serialized
*with dill save functions*, but not the ones that use save functions from
'pickle._Pickler.dispatch'. If the information is available, it also displays
the size in bytes that the object contributed to the pickle stream (including
its child objects).  Sample trace output:

    >>> import dill, dill.tests
    >>> dill.detect.trace(True)
    >>> dill.dump_session(main=dill.tests)
    ┬ M1: <module 'dill.tests' from '.../dill/tests/__init__.py'>
    ├┬ F2: <function _import_module at 0x7f0d2dce1b80>
    │└ # F2 [32 B]
    ├┬ D2: <dict object at 0x7f0d2e98a540>
    │├┬ T4: <class '_frozen_importlib.ModuleSpec'>
    ││└ # T4 [35 B]
    │├┬ D2: <dict object at 0x7f0d2ef0e8c0>
    ││├┬ T4: <class '_frozen_importlib_external.SourceFileLoader'>
    │││└ # T4 [50 B]
    ││├┬ D2: <dict object at 0x7f0d2e988a40>
    │││└ # D2 [84 B]
    ││└ # D2 [413 B]
    │└ # D2 [763 B]
    └ # M1 [813 B]
"""

__all__ = ['adapter', 'logger', 'trace']

import codecs
import contextlib
import locale
import logging
import math
import os
from functools import partial
from typing import TextIO, Union

import dill

# Tree drawing characters: Unicode to ASCII map.
ASCII_MAP = str.maketrans({"│": "|", "├": "|", "┬": "+", "└": "`"})

## Notes about the design choices ##

# Here is some domumentation of the Standard Library's logging internals that
# can't be found completely in the official documentation.  dill's logger is
# obtained by calling logging.getLogger('dill') and therefore is an instance of
# logging.getLoggerClass() at the call time.  As this is controlled by the user,
# in order to add some functionality to it it's necessary to use a LoggerAdapter
# to wrap it, overriding some of the adapter's methods and creating new ones.
#
# Basic calling sequence
# ======================
#
# Python's logging functionality can be conceptually divided into five steps:
#   0. Check logging level -> abort if call level is greater than logger level
#   1. Gather information -> construct a LogRecord from passed arguments and context
#   2. Filter (optional) -> discard message if the record matches a filter
#   3. Format -> format message with args, then format output string with message plus record
#   4. Handle -> write the formatted string to output as defined in the handler
#
# dill.logging.logger.log ->        # or logger.info, etc.
#   Logger.log ->               \
#     Logger._log ->             }- accept 'extra' parameter for custom record entries
#       Logger.makeRecord ->    /
#         LogRecord.__init__
#       Logger.handle ->
#         Logger.callHandlers ->
#           Handler.handle ->
#             Filterer.filter ->
#               Filter.filter
#             StreamHandler.emit ->
#               Handler.format ->
#                 Formatter.format ->
#                   LogRecord.getMessage        # does: record.message = msg % args
#                   Formatter.formatMessage ->
#                     PercentStyle.format       # does: self._fmt % vars(record)
#
# NOTE: All methods from the second line on are from logging.__init__.py

class TraceAdapter(logging.LoggerAdapter):
    """
    Tracks object tree depth and calculates pickled object size.

    A single instance of this wraps the module's logger, as the logging API
    doesn't allow setting it directly with a custom Logger subclass.  The added
    'trace()' method receives a pickle instance as the first argument and
    creates extra values to be added in the LogRecord from it, then calls
    'info()'.

    Usage of logger with 'trace()' method:

    >>> from dill.logger import adapter as logger  #NOTE: not dill.logger.logger
    >>> ...
    >>> def save_atype(pickler, obj):
    >>>     logger.trace(pickler, "Message with %s and %r etc. placeholders", 'text', obj)
    >>>     ...
    """
    def __init__(self, logger):
        self.logger = logger
    def addHandler(self, handler):
        formatter = TraceFormatter("%(prefix)s%(message)s%(suffix)s", handler=handler)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    def removeHandler(self, handler):
        self.logger.removeHandler(handler)
    def process(self, msg, kwargs):
        # A no-op override, as we don't have self.extra.
        return msg, kwargs
    def trace_setup(self, pickler):
        # Called by Pickler.dump().
        if not dill._dill.is_dill(pickler, child=False):
            return
        if self.isEnabledFor(logging.INFO):
            pickler._trace_depth = 1
            pickler._size_stack = []
        else:
            pickler._trace_depth = None
    def trace(self, pickler, msg, *args, **kwargs):
        if not hasattr(pickler, '_trace_depth'):
            logger.info(msg, *args, **kwargs)
            return
        if pickler._trace_depth is None:
            return
        extra = kwargs.get('extra', {})
        pushed_obj = msg.startswith('#')
        size = None
        try:
            # Streams are not required to be tellable.
            size = pickler._file.tell()
            frame = pickler.framer.current_frame
            try:
                size += frame.tell()
            except AttributeError:
                # PyPy may use a BytesBuilder as frame
                size += len(frame)
        except (AttributeError, TypeError):
            pass
        if size is not None:
            if not pushed_obj:
                pickler._size_stack.append(size)
            else:
                size -= pickler._size_stack.pop()
                extra['size'] = size
        if pushed_obj:
            pickler._trace_depth -= 1
        extra['depth'] = pickler._trace_depth
        kwargs['extra'] = extra
        self.info(msg, *args, **kwargs)
        if not pushed_obj:
            pickler._trace_depth += 1

class TraceFormatter(logging.Formatter):
    """
    Generates message prefix and suffix from record.

    This Formatter adds prefix and suffix strings to the log message in trace
    mode (an also provides empty string defaults for normal logs).
    """
    def __init__(self, *args, handler=None, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            encoding = handler.stream.encoding
            if encoding is None:
                raise AttributeError
        except AttributeError:
            encoding = locale.getpreferredencoding()
        try:
            encoding = codecs.lookup(encoding).name
        except LookupError:
            self.is_utf8 = False
        else:
            self.is_utf8 = (encoding == codecs.lookup('utf-8').name)
    def format(self, record):
        fields = {'prefix': "", 'suffix': ""}
        if getattr(record, 'depth', 0) > 0:
            if record.msg.startswith("#"):
                prefix = (record.depth - 1)*"│" + "└"
            elif record.depth == 1:
                prefix = "┬"
            else:
                prefix = (record.depth - 2)*"│" + "├┬"
            if not self.is_utf8:
                prefix = prefix.translate(ASCII_MAP) + "-"
            fields['prefix'] = prefix + " "
        if hasattr(record, 'size'):
            # Show object size in human-redable form.
            power = int(math.log(record.size, 2)) // 10
            size = record.size >> power*10
            fields['suffix'] = " [%d %sB]" % (size, "KMGTP"[power] + "i" if power else "")
        vars(record).update(fields)
        return super().format(record)

logger = logging.getLogger('dill')
logger.propagate = False
adapter = TraceAdapter(logger)
stderr_handler = logging._StderrHandler()
adapter.addHandler(stderr_handler)

def trace(arg: Union[bool, TextIO, str, os.PathLike] = None, *, mode: str = 'a') -> None:
    """print a trace through the stack when pickling; useful for debugging

    With a single boolean argument, enable or disable the tracing.

    Example usage:

        >>> import dill
        >>> dill.detect.trace(True)
        >>> dill.dump_session()

    Alternatively, ``trace()`` can be used as a context manager. With no
    arguments, it just takes care of restoring the tracing state on exit.
    Either a file handle, or a file name and (optionally) a file mode may be
    specitfied to redirect the tracing output in the ``with`` block context. A
    log function is yielded by the manager so the user can write extra
    information to the file.

    Example usage:

        >>> from dill import detect
        >>> D = {'a': 42, 'b': {'x': None}}
        >>> with detect.trace():
        >>>     dumps(D)
        ┬ D2: <dict object at 0x7f2721804800>
        ├┬ D2: <dict object at 0x7f27217f5c40>
        │└ # D2 [8 B]
        └ # D2 [22 B]
        >>> squared = lambda x: x**2
        >>> with detect.trace('output.txt', mode='w') as log:
        >>>     log("> D = %r", D)
        >>>     dumps(D)
        >>>     log("> squared = %r", squared)
        >>>     dumps(squared)

    Arguments:
        arg: a boolean value, or an optional file-like or path-like object for the context manager
        mode: mode string for ``open()`` if a file name is passed as the first argument
    """
    if not isinstance(arg, bool):
        return TraceManager(file=arg, mode=mode)
    logger.setLevel(logging.INFO if arg else logging.WARNING)

class TraceManager(contextlib.AbstractContextManager):
    """context manager version of trace(); can redirect the trace to a file"""
    def __init__(self, file, mode):
        self.file = file
        self.mode = mode
        self.redirect = file is not None
        self.file_is_stream = hasattr(file, 'write')
    def __enter__(self):
        if self.redirect:
            stderr_handler.flush()
            if self.file_is_stream:
                self.handler = logging.StreamHandler(self.file)
            else:
                self.handler = logging.FileHandler(self.file, self.mode)
            adapter.removeHandler(stderr_handler)
            adapter.addHandler(self.handler)
        self.old_level = adapter.getEffectiveLevel()
        adapter.setLevel(logging.INFO)
        return adapter.info
    def __exit__(self, *exc_info):
        adapter.setLevel(self.old_level)
        if self.redirect:
            adapter.removeHandler(self.handler)
            adapter.addHandler(stderr_handler)
            if not self.file_is_stream:
                self.handler.close()
