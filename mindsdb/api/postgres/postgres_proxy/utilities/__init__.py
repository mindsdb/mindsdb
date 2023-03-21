import re
from typing import Union


def strip_null_byte(x: Union[str, bytes], encoding=None):
    if type(x) == bytes:
        if encoding is None:
            encoding = "UTF-8"
        x = x.decode(encoding=encoding)
    return re.sub(r'[\s\x00]+$', '', x)