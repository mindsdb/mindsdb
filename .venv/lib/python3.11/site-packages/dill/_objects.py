#!/usr/bin/env python
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Copyright (c) 2008-2016 California Institute of Technology.
# Copyright (c) 2016-2022 The Uncertainty Quantification Foundation.
# License: 3-clause BSD.  The full license text is available at:
#  - https://github.com/uqfoundation/dill/blob/master/LICENSE
"""
all Python Standard Library objects (currently: CH 1-15 @ 2.7)
and some other common objects (i.e. numpy.ndarray)
"""

__all__ = ['registered','failures','succeeds']

# helper imports
import warnings; warnings.filterwarnings("ignore", category=DeprecationWarning)
import sys
import queue as Queue
import dbm as anydbm
from io import BytesIO as StringIO
import re
import array
import collections
import codecs
import struct
import dataclasses
import datetime
import calendar
import weakref
import pprint
import decimal
import numbers
import functools
import itertools
import operator
import tempfile
import shelve
import zlib
import gzip
import zipfile
import tarfile
import xdrlib
import csv
import hashlib
import hmac
import os
import logging
import logging.handlers
import optparse
#import __hello__
import threading
import socket
import contextlib
try:
    import bz2
    import sqlite3
    import dbm.ndbm as dbm
    HAS_ALL = True
except ImportError: # Ubuntu
    HAS_ALL = False
try:
    #import curses
    #from curses import textpad, panel
    HAS_CURSES = True
except ImportError: # Windows
    HAS_CURSES = False
try:
    import ctypes
    HAS_CTYPES = True
    # if using `pypy`, pythonapi is not found
    IS_PYPY = not hasattr(ctypes, 'pythonapi')
except ImportError: # MacPorts
    HAS_CTYPES = False
    IS_PYPY = False

# helper objects
class _class:
    def _method(self):
        pass
#   @classmethod
#   def _clsmethod(cls): #XXX: test me
#       pass
#   @staticmethod
#   def _static(self): #XXX: test me
#       pass
class _class2:
    def __call__(self):
        pass
_instance2 = _class2()
class _newclass(object):
    def _method(self):
        pass
#   @classmethod
#   def _clsmethod(cls): #XXX: test me
#       pass
#   @staticmethod
#   def _static(self): #XXX: test me
#       pass
class _newclass2(object):
    __slots__ = ['descriptor']
def _function(x): yield x
def _function2():
    try: raise
    except Exception:
        from sys import exc_info
        e, er, tb = exc_info()
        return er, tb
if HAS_CTYPES:
    class _Struct(ctypes.Structure):
        pass
    _Struct._fields_ = [("_field", ctypes.c_int),("next", ctypes.POINTER(_Struct))]
_filedescrip, _tempfile = tempfile.mkstemp('r') # deleted in cleanup
_tmpf = tempfile.TemporaryFile('w')

# objects used by dill for type declaration
registered = d = {}
# objects dill fails to pickle
failures = x = {}
# all other type objects
succeeds = a = {}

# types module (part of CH 8)
a['BooleanType'] = bool(1)
a['BuiltinFunctionType'] = len
a['BuiltinMethodType'] = a['BuiltinFunctionType']
a['BytesType'] = _bytes = codecs.latin_1_encode('\x00')[0] # bytes(1)
a['ClassType'] = _class
a['ComplexType'] = complex(1)
a['DictType'] = _dict = {}
a['DictionaryType'] = a['DictType']
a['FloatType'] = float(1)
a['FunctionType'] = _function
a['InstanceType'] = _instance = _class()
a['IntType'] = _int = int(1)
a['ListType'] = _list = []
a['NoneType'] = None
a['ObjectType'] = object()
a['StringType'] = _str = str(1)
a['TupleType'] = _tuple = ()
a['TypeType'] = type
a['LongType'] = _int
a['UnicodeType'] = _str
# built-in constants (CH 4)
a['CopyrightType'] = copyright
# built-in types (CH 5)
a['ClassObjectType'] = _newclass # <type 'type'>
a['ClassInstanceType'] = _newclass() # <type 'class'>
a['SetType'] = _set = set()
a['FrozenSetType'] = frozenset()
# built-in exceptions (CH 6)
a['ExceptionType'] = _exception = _function2()[0]
# string services (CH 7)
a['SREPatternType'] = _srepattern = re.compile('')
# data types (CH 8)
a['ArrayType'] = array.array("f")
a['DequeType'] = collections.deque([0])
a['DefaultDictType'] = collections.defaultdict(_function, _dict)
a['TZInfoType'] = datetime.tzinfo()
a['DateTimeType'] = datetime.datetime.today()
a['CalendarType'] = calendar.Calendar()
# numeric and mathematical types (CH 9)
a['DecimalType'] = decimal.Decimal(1)
a['CountType'] = itertools.count(0)
# data compression and archiving (CH 12)
a['TarInfoType'] = tarfile.TarInfo()
# generic operating system services (CH 15)
a['LoggerType'] = _logger = logging.getLogger()
a['FormatterType'] = logging.Formatter() # pickle ok
a['FilterType'] = logging.Filter() # pickle ok
a['LogRecordType'] = logging.makeLogRecord(_dict) # pickle ok
a['OptionParserType'] = _oparser = optparse.OptionParser() # pickle ok
a['OptionGroupType'] = optparse.OptionGroup(_oparser,"foo") # pickle ok
a['OptionType'] = optparse.Option('--foo') # pickle ok
if HAS_CTYPES:
    z = x if IS_PYPY else a
    z['CCharType'] = _cchar = ctypes.c_char()
    z['CWCharType'] = ctypes.c_wchar() # fail == 2.6
    z['CByteType'] = ctypes.c_byte()
    z['CUByteType'] = ctypes.c_ubyte()
    z['CShortType'] = ctypes.c_short()
    z['CUShortType'] = ctypes.c_ushort()
    z['CIntType'] = ctypes.c_int()
    z['CUIntType'] = ctypes.c_uint()
    z['CLongType'] = ctypes.c_long()
    z['CULongType'] = ctypes.c_ulong()
    z['CLongLongType'] = ctypes.c_longlong()
    z['CULongLongType'] = ctypes.c_ulonglong()
    z['CFloatType'] = ctypes.c_float()
    z['CDoubleType'] = ctypes.c_double()
    z['CSizeTType'] = ctypes.c_size_t()
    del z
    a['CLibraryLoaderType'] = ctypes.cdll
    a['StructureType'] = _Struct
    # if not IS_PYPY:
    #     a['BigEndianStructureType'] = ctypes.BigEndianStructure()
#NOTE: also LittleEndianStructureType and UnionType... abstract classes
#NOTE: remember for ctypesobj.contents creates a new python object
#NOTE: ctypes.c_int._objects is memberdescriptor for object's __dict__
#NOTE: base class of all ctypes data types is non-public _CData

import fractions
import io
from io import StringIO as TextIO
# built-in functions (CH 2)
a['ByteArrayType'] = bytearray([1])
# numeric and mathematical types (CH 9)
a['FractionType'] = fractions.Fraction()
a['NumberType'] = numbers.Number()
# generic operating system services (CH 15)
a['IOBaseType'] = io.IOBase()
a['RawIOBaseType'] = io.RawIOBase()
a['TextIOBaseType'] = io.TextIOBase()
a['BufferedIOBaseType'] = io.BufferedIOBase()
a['UnicodeIOType'] = TextIO() # the new StringIO
a['LoggerAdapterType'] = logging.LoggerAdapter(_logger,_dict) # pickle ok
if HAS_CTYPES:
    z = x if IS_PYPY else a
    z['CBoolType'] = ctypes.c_bool(1)
    z['CLongDoubleType'] = ctypes.c_longdouble()
    del z
import argparse
# data types (CH 8)
a['OrderedDictType'] = collections.OrderedDict(_dict)
a['CounterType'] = collections.Counter(_dict)
if HAS_CTYPES:
    z = x if IS_PYPY else a
    z['CSSizeTType'] = ctypes.c_ssize_t()
    del z
# generic operating system services (CH 15)
a['NullHandlerType'] = logging.NullHandler() # pickle ok  # new 2.7
a['ArgParseFileType'] = argparse.FileType() # pickle ok

# -- pickle fails on all below here -----------------------------------------
# types module (part of CH 8)
a['CodeType'] = compile('','','exec')
a['DictProxyType'] = type.__dict__
a['DictProxyType2'] = _newclass.__dict__
a['EllipsisType'] = Ellipsis
a['ClosedFileType'] = open(os.devnull, 'wb', buffering=0).close()
a['GetSetDescriptorType'] = array.array.typecode
a['LambdaType'] = _lambda = lambda x: lambda y: x #XXX: works when not imported!
a['MemberDescriptorType'] = _newclass2.descriptor
if not IS_PYPY:
    a['MemberDescriptorType2'] = datetime.timedelta.days
a['MethodType'] = _method = _class()._method #XXX: works when not imported!
a['ModuleType'] = datetime
a['NotImplementedType'] = NotImplemented
a['SliceType'] = slice(1)
a['UnboundMethodType'] = _class._method #XXX: works when not imported!
d['TextWrapperType'] = open(os.devnull, 'r') # same as mode='w','w+','r+'
d['BufferedRandomType'] = open(os.devnull, 'r+b') # same as mode='w+b'
d['BufferedReaderType'] = open(os.devnull, 'rb') # (default: buffering=-1)
d['BufferedWriterType'] = open(os.devnull, 'wb')
try: # oddities: deprecated
    from _pyio import open as _open
    d['PyTextWrapperType'] = _open(os.devnull, 'r', buffering=-1)
    d['PyBufferedRandomType'] = _open(os.devnull, 'r+b', buffering=-1)
    d['PyBufferedReaderType'] = _open(os.devnull, 'rb', buffering=-1)
    d['PyBufferedWriterType'] = _open(os.devnull, 'wb', buffering=-1)
except ImportError:
    pass
# other (concrete) object types
z = d if sys.hexversion < 0x30800a2 else a
z['CellType'] = (_lambda)(0).__closure__[0]
del z
a['XRangeType'] = _xrange = range(1)
a['MethodDescriptorType'] = type.__dict__['mro']
a['WrapperDescriptorType'] = type.__repr__
#a['WrapperDescriptorType2'] = type.__dict__['__module__']#XXX: GetSetDescriptor
a['ClassMethodDescriptorType'] = type.__dict__['__prepare__']
# built-in functions (CH 2)
_methodwrap = (1).__lt__
a['MethodWrapperType'] = _methodwrap
a['StaticMethodType'] = staticmethod(_method)
a['ClassMethodType'] = classmethod(_method)
a['PropertyType'] = property()
d['SuperType'] = super(Exception, _exception)
# string services (CH 7)
_in = _bytes
a['InputType'] = _cstrI = StringIO(_in)
a['OutputType'] = _cstrO = StringIO()
# data types (CH 8)
a['WeakKeyDictionaryType'] = weakref.WeakKeyDictionary()
a['WeakValueDictionaryType'] = weakref.WeakValueDictionary()
a['ReferenceType'] = weakref.ref(_instance)
a['DeadReferenceType'] = weakref.ref(_class())
a['ProxyType'] = weakref.proxy(_instance)
a['DeadProxyType'] = weakref.proxy(_class())
a['CallableProxyType'] = weakref.proxy(_instance2)
a['DeadCallableProxyType'] = weakref.proxy(_class2())
a['QueueType'] = Queue.Queue()
# numeric and mathematical types (CH 9)
d['PartialType'] = functools.partial(int,base=2)
a['IzipType'] = zip('0','1')
a['ChainType'] = itertools.chain('0','1')
d['ItemGetterType'] = operator.itemgetter(0)
d['AttrGetterType'] = operator.attrgetter('__repr__')
# file and directory access (CH 10)
_fileW = _cstrO
# data persistence (CH 11)
if HAS_ALL:
    x['ConnectionType'] = _conn = sqlite3.connect(':memory:')
    x['CursorType'] = _conn.cursor()
a['ShelveType'] = shelve.Shelf({})
# data compression and archiving (CH 12)
if HAS_ALL:
    x['BZ2FileType'] = bz2.BZ2File(os.devnull)
    x['BZ2CompressorType'] = bz2.BZ2Compressor()
    x['BZ2DecompressorType'] = bz2.BZ2Decompressor()
#x['ZipFileType'] = _zip = zipfile.ZipFile(os.devnull,'w')
#_zip.write(_tempfile,'x') [causes annoying warning/error printed on import]
#a['ZipInfoType'] = _zip.getinfo('x')
a['TarFileType'] = tarfile.open(fileobj=_fileW,mode='w')
# file formats (CH 13)
x['DialectType'] = csv.get_dialect('excel')
a['PackerType'] = xdrlib.Packer()
# optional operating system services (CH 16)
a['LockType'] = threading.Lock()
a['RLockType'] = threading.RLock()
# generic operating system services (CH 15) # also closed/open and r/w/etc...
a['NamedLoggerType'] = _logger = logging.getLogger(__name__)
#a['FrozenModuleType'] = __hello__ #FIXME: prints "Hello world..."
# interprocess communication (CH 17)
x['SocketType'] = _socket = socket.socket()
x['SocketPairType'] = socket.socketpair()[0]
# python runtime services (CH 27)
a['GeneratorContextManagerType'] = contextlib.contextmanager(max)([1])

try: # ipython
    __IPYTHON__ is True # is ipython
except NameError:
    # built-in constants (CH 4)
    a['QuitterType'] = quit
    d['ExitType'] = a['QuitterType']
try: # numpy #FIXME: slow... 0.05 to 0.1 sec to import numpy
    from numpy import ufunc as _numpy_ufunc
    from numpy import array as _numpy_array
    from numpy import int32 as _numpy_int32
    a['NumpyUfuncType'] = _numpy_ufunc
    a['NumpyArrayType'] = _numpy_array
    a['NumpyInt32Type'] = _numpy_int32
except ImportError:
    pass
# numeric and mathematical types (CH 9)
a['ProductType'] = itertools.product('0','1')
# generic operating system services (CH 15)
a['FileHandlerType'] = logging.FileHandler(os.devnull)
a['RotatingFileHandlerType'] = logging.handlers.RotatingFileHandler(os.devnull)
a['SocketHandlerType'] = logging.handlers.SocketHandler('localhost',514)
a['MemoryHandlerType'] = logging.handlers.MemoryHandler(1)
# data types (CH 8)
a['WeakSetType'] = weakref.WeakSet() # 2.7
# generic operating system services (CH 15) [errors when dill is imported]
#a['ArgumentParserType'] = _parser = argparse.ArgumentParser('PROG')
#a['NamespaceType'] = _parser.parse_args() # pickle ok
#a['SubParsersActionType'] = _parser.add_subparsers()
#a['MutuallyExclusiveGroupType'] = _parser.add_mutually_exclusive_group()
#a['ArgumentGroupType'] = _parser.add_argument_group()

# -- dill fails in some versions below here ---------------------------------
# types module (part of CH 8)
d['FileType'] = open(os.devnull, 'rb', buffering=0) # same 'wb','wb+','rb+'
# built-in functions (CH 2)
# Iterators:
a['ListIteratorType'] = iter(_list) # empty vs non-empty
a['SetIteratorType'] = iter(_set) #XXX: empty vs non-empty #FIXME: list_iterator
a['TupleIteratorType']= iter(_tuple) # empty vs non-empty
a['XRangeIteratorType'] = iter(_xrange) # empty vs non-empty
a["BytesIteratorType"] = iter(b'')
a["BytearrayIteratorType"] = iter(bytearray(b''))
z = x if IS_PYPY else a
z["CallableIteratorType"] = iter(iter, None)
del z
x["MemoryIteratorType"] = iter(memoryview(b''))
a["ListReverseiteratorType"] = reversed([])
X = a['OrderedDictType']
d["OdictKeysType"] = X.keys()
d["OdictValuesType"] = X.values()
d["OdictItemsType"] = X.items()
a["OdictIteratorType"] = iter(X.keys()) #FIXME: list_iterator
del X
#FIXME: list_iterator
a['DictionaryItemIteratorType'] = iter(type.__dict__.items())
a['DictionaryKeyIteratorType'] = iter(type.__dict__.keys())
a['DictionaryValueIteratorType'] = iter(type.__dict__.values())
if sys.hexversion >= 0x30800a0:
    a["DictReversekeyiteratorType"] = reversed({}.keys())
    a["DictReversevalueiteratorType"] = reversed({}.values())
    a["DictReverseitemiteratorType"] = reversed({}.items())

try:
    import symtable
    #FIXME: fails to pickle
    x["SymtableEntryType"] = symtable.symtable("", "string", "exec")._table
except ImportError:
    pass

if sys.hexversion >= 0x30a00a0:
    x['LineIteratorType'] = compile('3', '', 'eval').co_lines()

if sys.hexversion >= 0x30b00b0:
    from types import GenericAlias
    d["GenericAliasIteratorType"] = iter(GenericAlias(list, (int,)))
    x['PositionsIteratorType'] = compile('3', '', 'eval').co_positions()

# data types (CH 8)
a['PrettyPrinterType'] = pprint.PrettyPrinter()
# numeric and mathematical types (CH 9)
a['CycleType'] = itertools.cycle('0')
# file and directory access (CH 10)
a['TemporaryFileType'] = _tmpf
# data compression and archiving (CH 12)
x['GzipFileType'] = gzip.GzipFile(fileobj=_fileW)
# generic operating system services (CH 15)
a['StreamHandlerType'] = logging.StreamHandler()
# numeric and mathematical types (CH 9)
a['PermutationsType'] = itertools.permutations('0')
a['CombinationsType'] = itertools.combinations('0',1)
a['RepeatType'] = itertools.repeat(0)
a['CompressType'] = itertools.compress('0',[1])
#XXX: ...and etc

# -- dill fails on all below here -------------------------------------------
# types module (part of CH 8)
x['GeneratorType'] = _generator = _function(1) #XXX: priority
x['FrameType'] = _generator.gi_frame #XXX: inspect.currentframe()
x['TracebackType'] = _function2()[1] #(see: inspect.getouterframes,getframeinfo)
# other (concrete) object types
# (also: Capsule / CObject ?)
# built-in functions (CH 2)
# built-in types (CH 5)
# string services (CH 7)
x['StructType'] = struct.Struct('c')
x['CallableIteratorType'] = _srepattern.finditer('')
x['SREMatchType'] = _srepattern.match('')
x['SREScannerType'] = _srepattern.scanner('')
x['StreamReader'] = codecs.StreamReader(_cstrI) #XXX: ... and etc
# python object persistence (CH 11)
# x['DbShelveType'] = shelve.open('foo','n')#,protocol=2) #XXX: delete foo
if HAS_ALL:
    z = a if IS_PYPY else x
    z['DbmType'] = dbm.open(_tempfile,'n')
    del z
# x['DbCursorType'] = _dbcursor = anydbm.open('foo','n') #XXX: delete foo
# x['DbType'] = _dbcursor.db
# data compression and archiving (CH 12)
x['ZlibCompressType'] = zlib.compressobj()
x['ZlibDecompressType'] = zlib.decompressobj()
# file formats (CH 13)
x['CSVReaderType'] = csv.reader(_cstrI)
x['CSVWriterType'] = csv.writer(_cstrO)
x['CSVDictReaderType'] = csv.DictReader(_cstrI)
x['CSVDictWriterType'] = csv.DictWriter(_cstrO,{})
# cryptographic services (CH 14)
x['HashType'] = hashlib.md5()
if (sys.hexversion < 0x30800a1):
    x['HMACType'] = hmac.new(_in)
else:
    x['HMACType'] = hmac.new(_in, digestmod='md5')
# generic operating system services (CH 15)
if HAS_CURSES: pass
    #x['CursesWindowType'] = _curwin = curses.initscr() #FIXME: messes up tty
    #x['CursesTextPadType'] = textpad.Textbox(_curwin)
    #x['CursesPanelType'] = panel.new_panel(_curwin)
if HAS_CTYPES:
    x['CCharPType'] = ctypes.c_char_p()
    x['CWCharPType'] = ctypes.c_wchar_p()
    x['CVoidPType'] = ctypes.c_void_p()
    if sys.platform[:3] == 'win':
        x['CDLLType'] = _cdll = ctypes.cdll.msvcrt
    else:
        x['CDLLType'] = _cdll = ctypes.CDLL(None)
    if not IS_PYPY:
        x['PyDLLType'] = _pydll = ctypes.pythonapi
    x['FuncPtrType'] = _cdll._FuncPtr()
    x['CCharArrayType'] = ctypes.create_string_buffer(1)
    x['CWCharArrayType'] = ctypes.create_unicode_buffer(1)
    x['CParamType'] = ctypes.byref(_cchar)
    x['LPCCharType'] = ctypes.pointer(_cchar)
    x['LPCCharObjType'] = _lpchar = ctypes.POINTER(ctypes.c_char)
    x['NullPtrType'] = _lpchar()
    x['NullPyObjectType'] = ctypes.py_object()
    x['PyObjectType'] = ctypes.py_object(lambda :None)
    z = a if IS_PYPY else x
    z['FieldType'] = _field = _Struct._field
    z['CFUNCTYPEType'] = _cfunc = ctypes.CFUNCTYPE(ctypes.c_char)
    x['CFunctionType'] = _cfunc(str)
    del z
# numeric and mathematical types (CH 9)
a['MethodCallerType'] = operator.methodcaller('mro') # 2.6
# built-in types (CH 5)
x['MemoryType'] = memoryview(_in) # 2.7
x['MemoryType2'] = memoryview(bytearray(_in)) # 2.7
d['DictItemsType'] = _dict.items() # 2.7
d['DictKeysType'] = _dict.keys() # 2.7
d['DictValuesType'] = _dict.values() # 2.7
# generic operating system services (CH 15)
a['RawTextHelpFormatterType'] = argparse.RawTextHelpFormatter('PROG')
a['RawDescriptionHelpFormatterType'] = argparse.RawDescriptionHelpFormatter('PROG')
a['ArgDefaultsHelpFormatterType'] = argparse.ArgumentDefaultsHelpFormatter('PROG')
z = a if IS_PYPY else x
z['CmpKeyType'] = _cmpkey = functools.cmp_to_key(_methodwrap) # 2.7, >=3.2
z['CmpKeyObjType'] = _cmpkey('0') #2.7, >=3.2
del z
# oddities: removed, etc
x['BufferType'] = x['MemoryType']

from dill._dill import _testcapsule
if _testcapsule is not None:
    d['PyCapsuleType'] = _testcapsule
del _testcapsule

if hasattr(dataclasses, '_HAS_DEFAULT_FACTORY'):
    a['DataclassesHasDefaultFactoryType'] = dataclasses._HAS_DEFAULT_FACTORY

if hasattr(dataclasses, 'MISSING'):
    a['DataclassesMissingType'] = dataclasses.MISSING

if hasattr(dataclasses, 'KW_ONLY'):
    a['DataclassesKWOnlyType'] = dataclasses.KW_ONLY

if hasattr(dataclasses, '_FIELD_BASE'):
    a['DataclassesFieldBaseType'] = dataclasses._FIELD

# -- cleanup ----------------------------------------------------------------
a.update(d) # registered also succeed
if sys.platform[:3] == 'win':
    os.close(_filedescrip) # required on win32
os.remove(_tempfile)


# EOF
