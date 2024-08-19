from pathlib import Path
from typing import Callable, List, Optional, Union

def dirhash(
    dirname: Union[str, Path],
    hashfunc: str='md5',
    excluded_files: Optional[List[str]]=None,
    ignore_hidden: bool=False,
    followlinks: bool=False,
    excluded_extensions: Optional[List[str]]=None,
    include_paths: bool=False
) -> str: ...
def _filehash(filepath: str, hashfunc: Callable) -> str: ...
def _reduce_hash(hashlist: List[str], hashfunc: Callable) -> str: ...

__version__: str
