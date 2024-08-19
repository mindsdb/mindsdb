import inspect
from typing import Dict, Callable, TypedDict


class Module(TypedDict):
    """
    Modules are blocks of code, representing either object instantiations or function calls. 
    See the mindsdb/lightwood/api/types:Module equivalent for more details.

    :param module: Name of the module (function or class name)
    :param args: Argument to pass to the function or constructor
    """ # noqa
    module: str
    args: Dict[str, str]


def filter_fn_args(fn: Callable, args: Dict) -> Dict:
    filtered = {}
    for k, v in args.items():
        if k in inspect.signature(fn).parameters:
            filtered[k] = v
    return filtered
