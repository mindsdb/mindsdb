# sly/ast.py
import sys

class AST0(object):
    
    @classmethod
    def __init_subclass__(cls, **kwargs):
        mod = sys.modules[cls.__module__]
        if not hasattr(cls, '__annotations__'):
            return

        hints = list(cls.__annotations__.items())

        def __init__(self, *args, **kwargs):
            if len(hints) != len(args):
                raise TypeError(f'Expected {len(hints)} arguments')
            for arg, (name, val) in zip(args, hints):
                if isinstance(val, str):
                    val = getattr(mod, val)
                if not isinstance(arg, val):
                    raise TypeError(f'{name} argument must be {val}')
                setattr(self, name, arg)

        cls.__init__ = __init__

