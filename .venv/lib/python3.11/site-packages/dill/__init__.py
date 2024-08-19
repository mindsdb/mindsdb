#!/usr/bin/env python
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Copyright (c) 2008-2016 California Institute of Technology.
# Copyright (c) 2016-2022 The Uncertainty Quantification Foundation.
# License: 3-clause BSD.  The full license text is available at:
#  - https://github.com/uqfoundation/dill/blob/master/LICENSE

# author, version, license, and long description
try: # the package is installed
    from .__info__ import __version__, __author__, __doc__, __license__
except: # pragma: no cover
    import os
    import sys 
    parent = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
    sys.path.append(parent)
    # get distribution meta info 
    from version import (__version__, __author__,
                         get_license_text, get_readme_as_rst)
    __license__ = get_license_text(os.path.join(parent, 'LICENSE'))
    __license__ = "\n%s" % __license__
    __doc__ = get_readme_as_rst(os.path.join(parent, 'README.md'))
    del os, sys, parent, get_license_text, get_readme_as_rst


from ._dill import (
    Pickler, Unpickler,
    check, copy, dump, dumps, load, loads, pickle, pickles, register,
    DEFAULT_PROTOCOL, HIGHEST_PROTOCOL, CONTENTS_FMODE, FILE_FMODE, HANDLE_FMODE,
    PickleError, PickleWarning, PicklingError, PicklingWarning, UnpicklingError,
    UnpicklingWarning,
)
from .session import (
    dump_module, load_module, load_module_asdict,
    dump_session, load_session # backward compatibility
)
from . import detect, logger, session, source, temp

# get global settings
from .settings import settings

# make sure "trace" is turned off
logger.trace(False)

from importlib import reload

objects = {}
# local import of dill._objects
#from . import _objects
#objects.update(_objects.succeeds)
#del _objects

# local import of dill.objtypes
from . import objtypes as types

def load_types(pickleable=True, unpickleable=True):
    """load pickleable and/or unpickleable types to ``dill.types``

    ``dill.types`` is meant to mimic the ``types`` module, providing a
    registry of object types.  By default, the module is empty (for import
    speed purposes). Use the ``load_types`` function to load selected object
    types to the ``dill.types`` module.

    Args:
        pickleable (bool, default=True): if True, load pickleable types.
        unpickleable (bool, default=True): if True, load unpickleable types.

    Returns:
        None
    """
    # local import of dill.objects
    from . import _objects
    if pickleable:
        objects.update(_objects.succeeds)
    else:
        [objects.pop(obj,None) for obj in _objects.succeeds]
    if unpickleable:
        objects.update(_objects.failures)
    else:
        [objects.pop(obj,None) for obj in _objects.failures]
    objects.update(_objects.registered)
    del _objects
    # reset contents of types to 'empty'
    [types.__dict__.pop(obj) for obj in list(types.__dict__.keys()) \
                             if obj.find('Type') != -1]
    # add corresponding types from objects to types
    reload(types)

def extend(use_dill=True):
    '''add (or remove) dill types to/from the pickle registry

    by default, ``dill`` populates its types to ``pickle.Pickler.dispatch``.
    Thus, all ``dill`` types are available upon calling ``'import pickle'``.
    To drop all ``dill`` types from the ``pickle`` dispatch, *use_dill=False*.

    Args:
        use_dill (bool, default=True): if True, extend the dispatch table.

    Returns:
        None
    '''
    from ._dill import _revert_extension, _extend
    if use_dill: _extend()
    else: _revert_extension()
    return

extend()


def license():
    """print license"""
    print (__license__)
    return

def citation():
    """print citation"""
    print (__doc__[-491:-118])
    return

# end of file
