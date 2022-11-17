
from contextvars import ContextVar
from typing import Any
from copy import deepcopy


class Context:
    ''' Thread independent storage
    '''
    __slots__ = ('_storage',)

    def __init__(self) -> None:
        self._storage = ContextVar({})

    def __getattr__(self, name: str) -> Any:
        storage = self._storage.get({})
        if name in storage:
            return storage[name]
        raise AttributeError(name)

    def __setattr__(self, name: str, value: Any) -> None:
        storage = deepcopy(self._storage.get({}))
        storage[name] = value
        self._storage = storage

    def __delattr__(self, name: str) -> None:
        storage = deepcopy(self._storage.get({}))
        if name not in storage:
            raise AttributeError(name)
        del storage['name']
        self._storage.set(storage)


context = Context()
