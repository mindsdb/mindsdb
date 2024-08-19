#!/usr/bin/env python
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Copyright (c) 2008-2016 California Institute of Technology.
# Copyright (c) 2016-2022 The Uncertainty Quantification Foundation.
# License: 3-clause BSD.  The full license text is available at:
#  - https://github.com/uqfoundation/dill/blob/master/LICENSE
"""
Methods for detecting objects leading to pickling failures.
"""

import dis
from inspect import ismethod, isfunction, istraceback, isframe, iscode

from .pointers import parent, reference, at, parents, children
from .logger import trace

__all__ = ['baditems','badobjects','badtypes','code','errors','freevars',
           'getmodule','globalvars','nestedcode','nestedglobals','outermost',
           'referredglobals','referrednested','trace','varnames']

def getmodule(object, _filename=None, force=False):
    """get the module of the object"""
    from inspect import getmodule as getmod
    module = getmod(object, _filename)
    if module or not force: return module
    import builtins
    from .source import getname
    name = getname(object, force=True)
    return builtins if name in vars(builtins).keys() else None

def outermost(func): # is analogous to getsource(func,enclosing=True)
    """get outermost enclosing object (i.e. the outer function in a closure)

    NOTE: this is the object-equivalent of getsource(func, enclosing=True)
    """
    if ismethod(func):
        _globals = func.__func__.__globals__ or {}
    elif isfunction(func):
        _globals = func.__globals__ or {}
    else:
        return #XXX: or raise? no matches
    _globals = _globals.items()
    # get the enclosing source
    from .source import getsourcelines
    try: lines,lnum = getsourcelines(func, enclosing=True)
    except Exception: #TypeError, IOError
        lines,lnum = [],None
    code = ''.join(lines)
    # get all possible names,objects that are named in the enclosing source
    _locals = ((name,obj) for (name,obj) in _globals if name in code)
    # now only save the objects that generate the enclosing block
    for name,obj in _locals: #XXX: don't really need 'name'
        try:
            if getsourcelines(obj) == (lines,lnum): return obj
        except Exception: #TypeError, IOError
            pass
    return #XXX: or raise? no matches

def nestedcode(func, recurse=True): #XXX: or return dict of {co_name: co} ?
    """get the code objects for any nested functions (e.g. in a closure)"""
    func = code(func)
    if not iscode(func): return [] #XXX: or raise? no matches
    nested = set()
    for co in func.co_consts:
        if co is None: continue
        co = code(co)
        if co:
            nested.add(co)
            if recurse: nested |= set(nestedcode(co, recurse=True))
    return list(nested)

def code(func):
    """get the code object for the given function or method

    NOTE: use dill.source.getsource(CODEOBJ) to get the source code
    """
    if ismethod(func): func = func.__func__
    if isfunction(func): func = func.__code__
    if istraceback(func): func = func.tb_frame
    if isframe(func): func = func.f_code
    if iscode(func): return func
    return

#XXX: ugly: parse dis.dis for name after "<code object" in line and in globals?
def referrednested(func, recurse=True): #XXX: return dict of {__name__: obj} ?
    """get functions defined inside of func (e.g. inner functions in a closure)

    NOTE: results may differ if the function has been executed or not.
    If len(nestedcode(func)) > len(referrednested(func)), try calling func().
    If possible, python builds code objects, but delays building functions
    until func() is called.
    """
    import gc
    funcs = set()
    # get the code objects, and try to track down by referrence
    for co in nestedcode(func, recurse):
        # look for function objects that refer to the code object
        for obj in gc.get_referrers(co):
            # get methods
            _ = getattr(obj, '__func__', None) # ismethod
            if getattr(_, '__code__', None) is co: funcs.add(obj)
            # get functions
            elif getattr(obj, '__code__', None) is co: funcs.add(obj)
            # get frame objects
            elif getattr(obj, 'f_code', None) is co: funcs.add(obj)
            # get code objects
            elif hasattr(obj, 'co_code') and obj is co: funcs.add(obj)
#     frameobjs => func.__code__.co_varnames not in func.__code__.co_cellvars
#     funcobjs => func.__code__.co_cellvars not in func.__code__.co_varnames
#     frameobjs are not found, however funcobjs are...
#     (see: test_mixins.quad ... and test_mixins.wtf)
#     after execution, code objects get compiled, and then may be found by gc
    return list(funcs)


def freevars(func):
    """get objects defined in enclosing code that are referred to by func

    returns a dict of {name:object}"""
    if ismethod(func): func = func.__func__
    if isfunction(func):
        closures = func.__closure__ or ()
        func = func.__code__.co_freevars # get freevars
    else:
        return {}

    def get_cell_contents():
        for name, c in zip(func, closures):
            try:
                cell_contents = c.cell_contents
            except ValueError: # cell is empty
                continue
            yield name, c.cell_contents

    return dict(get_cell_contents())

# thanks to Davies Liu for recursion of globals
def nestedglobals(func, recurse=True):
    """get the names of any globals found within func"""
    func = code(func)
    if func is None: return list()
    import sys
    from .temp import capture
    CAN_NULL = sys.hexversion >= 0x30b00a7 # NULL may be prepended >= 3.11a7
    names = set()
    with capture('stdout') as out:
        dis.dis(func) #XXX: dis.dis(None) disassembles last traceback
    for line in out.getvalue().splitlines():
        if '_GLOBAL' in line:
            name = line.split('(')[-1].split(')')[0]
            if CAN_NULL:
                names.add(name.replace('NULL + ', ''))
            else:
                names.add(name)
    for co in getattr(func, 'co_consts', tuple()):
        if co and recurse and iscode(co):
            names.update(nestedglobals(co, recurse=True))
    return list(names)

def referredglobals(func, recurse=True, builtin=False):
    """get the names of objects in the global scope referred to by func"""
    return globalvars(func, recurse, builtin).keys()

def globalvars(func, recurse=True, builtin=False):
    """get objects defined in global scope that are referred to by func

    return a dict of {name:object}"""
    if ismethod(func): func = func.__func__
    if isfunction(func):
        globs = vars(getmodule(sum)).copy() if builtin else {}
        # get references from within closure
        orig_func, func = func, set()
        for obj in orig_func.__closure__ or {}:
            try:
                cell_contents = obj.cell_contents
            except ValueError: # cell is empty
                pass
            else:
                _vars = globalvars(cell_contents, recurse, builtin) or {}
                func.update(_vars) #XXX: (above) be wary of infinte recursion?
                globs.update(_vars)
        # get globals
        globs.update(orig_func.__globals__ or {})
        # get names of references
        if not recurse:
            func.update(orig_func.__code__.co_names)
        else:
            func.update(nestedglobals(orig_func.__code__))
            # find globals for all entries of func
            for key in func.copy(): #XXX: unnecessary...?
                nested_func = globs.get(key)
                if nested_func is orig_func:
                   #func.remove(key) if key in func else None
                    continue  #XXX: globalvars(func, False)?
                func.update(globalvars(nested_func, True, builtin))
    elif iscode(func):
        globs = vars(getmodule(sum)).copy() if builtin else {}
       #globs.update(globals())
        if not recurse:
            func = func.co_names # get names
        else:
            orig_func = func.co_name # to stop infinite recursion
            func = set(nestedglobals(func))
            # find globals for all entries of func
            for key in func.copy(): #XXX: unnecessary...?
                if key is orig_func:
                   #func.remove(key) if key in func else None
                    continue  #XXX: globalvars(func, False)?
                nested_func = globs.get(key)
                func.update(globalvars(nested_func, True, builtin))
    else:
        return {}
    #NOTE: if name not in __globals__, then we skip it...
    return dict((name,globs[name]) for name in func if name in globs)


def varnames(func):
    """get names of variables defined by func

    returns a tuple (local vars, local vars referrenced by nested functions)"""
    func = code(func)
    if not iscode(func):
        return () #XXX: better ((),())? or None?
    return func.co_varnames, func.co_cellvars


def baditems(obj, exact=False, safe=False): #XXX: obj=globals() ?
    """get items in object that fail to pickle"""
    if not hasattr(obj,'__iter__'): # is not iterable
        return [j for j in (badobjects(obj,0,exact,safe),) if j is not None]
    obj = obj.values() if getattr(obj,'values',None) else obj
    _obj = [] # can't use a set, as items may be unhashable
    [_obj.append(badobjects(i,0,exact,safe)) for i in obj if i not in _obj]
    return [j for j in _obj if j is not None]


def badobjects(obj, depth=0, exact=False, safe=False):
    """get objects that fail to pickle"""
    from dill import pickles
    if not depth:
        if pickles(obj,exact,safe): return None
        return obj
    return dict(((attr, badobjects(getattr(obj,attr),depth-1,exact,safe)) \
           for attr in dir(obj) if not pickles(getattr(obj,attr),exact,safe)))

def badtypes(obj, depth=0, exact=False, safe=False):
    """get types for objects that fail to pickle"""
    from dill import pickles
    if not depth:
        if pickles(obj,exact,safe): return None
        return type(obj)
    return dict(((attr, badtypes(getattr(obj,attr),depth-1,exact,safe)) \
           for attr in dir(obj) if not pickles(getattr(obj,attr),exact,safe)))

def errors(obj, depth=0, exact=False, safe=False):
    """get errors for objects that fail to pickle"""
    from dill import pickles, copy
    if not depth:
        try:
            pik = copy(obj)
            if exact:
                assert pik == obj, \
                    "Unpickling produces %s instead of %s" % (pik,obj)
            assert type(pik) == type(obj), \
                "Unpickling produces %s instead of %s" % (type(pik),type(obj))
            return None
        except Exception:
            import sys
            return sys.exc_info()[1]
    _dict = {}
    for attr in dir(obj):
        try:
            _attr = getattr(obj,attr)
        except Exception:
            import sys
            _dict[attr] = sys.exc_info()[1]
            continue
        if not pickles(_attr,exact,safe):
            _dict[attr] = errors(_attr,depth-1,exact,safe)
    return _dict


# EOF
