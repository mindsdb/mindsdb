
import base64
from typing import Any
from copy import deepcopy
from contextvars import ContextVar


class Context:
    ''' Thread independent storage
    '''
    __slots__ = ('_storage',)

    def __init__(self, storage) -> None:
        object.__setattr__(self, '_storage', storage)
        self.set_default()

    def set_default(self) -> None:
        self._storage.set({
            'user_id': None,
            'company_id': None,
            'encryption_key': None,
            'user_class': 0,
            'profiling': {
                'level': 0,
                'enabled': False,
                'pointer': None,
                'tree': None
            }
        })

    @property
    def encryption_key_bytes(self) -> bytes:
        encryption_key = self.encryption_key
        if encryption_key is None:
            return None
        return base64.b64decode(encryption_key.encode('utf-8'))

    def __getattr__(self, name: str) -> Any:
        storage = self._storage.get({})
        if name not in storage:
            raise AttributeError(name)
        return storage[name]

    def __setattr__(self, name: str, value: Any) -> None:
        storage = deepcopy(self._storage.get({}))
        storage[name] = value
        self._storage.set(storage)

    def __delattr__(self, name: str) -> None:
        storage = deepcopy(self._storage.get({}))
        if name not in storage:
            raise AttributeError(name)
        del storage['name']
        self._storage.set(storage)

    def dump(self) -> dict:
        storage = deepcopy(self._storage.get({}))
        return storage

    def load(self, storage: dict) -> None:
        self._storage.set(storage)


_context_var = ContextVar('mindsdb.context')
context = Context(_context_var)
