
from contextvars import ContextVar
from typing import Any
from copy import deepcopy


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
            'session_id': "",
            'task_id': None,
            'user_class': 0,
            'profiling': {
                'level': 0,
                'enabled': False,
                'pointer': None,
                'tree': None
            },
            'email_confirmed': 0,
        })

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

    def get_metadata(self, **kwargs) -> dict:
        return {
            'user_id': self.user_id or "",
            'company_id': self.company_id or "",
            'session_id': self.session_id,
            'user_class': self.user_class,
            **kwargs
        }


_context_var = ContextVar('mindsdb.context')
context = Context(_context_var)
