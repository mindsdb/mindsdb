
from contextvars import ContextVar
from typing import Any
from copy import deepcopy
import time
from datetime import datetime, timezone

import mindsdb.utilities.hooks as hooks
from mindsdb.utilities.config import Config


class Context:
    ''' Thread independent storage
    '''
    __slots__ = ('_storage',)

    def __init__(self, storage) -> None:
        object.__setattr__(self, '_storage', storage)

    def set_default(self) -> None:
        self._storage.set({
            'company_id': None,
            'user_class': 0,
            'profiling': {
                'pointer': None,
                'tree': None
            }
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

    def _get_current_node(self, profiling):
        current_node = profiling['tree']
        for child_index in profiling['pointer']:
            current_node = current_node['children'][child_index]
        return current_node

    def profiling_start_node(self, tag):
        profiling = self.profiling
        new_node = {
            'start_at': time.perf_counter(),
            'stop_at': None,
            'name': tag,
            'children': []
        }
        if profiling['pointer'] is None:
            profiling['pointer'] = []
            profiling['tree'] = new_node
            profiling['tree']['time_start_at'] = datetime.now(timezone.utc)
        else:
            current_node = self._get_current_node(profiling)
            profiling['pointer'].append(len(current_node['children']))
            current_node['children'].append(new_node)

    def profiling_set_meta(self, **kwargs):
        self.profiling.update(kwargs)

    def profiling_stop_current_node(self):
        profiling = self.profiling
        current_node = self._get_current_node(profiling)
        current_node['stop_at'] = time.perf_counter()
        if len(profiling['pointer']) > 0:
            profiling['pointer'] = profiling['pointer'][:-1]
        else:
            self._send_profiling_results()
            profiling['pointer'] = None

    def _send_profiling_results(self):
        self.profiling['company_id'] = self.company_id
        self.profiling['hostname'] = Config().get('aws_meta_data', {}).get('public-hostname', '?')
        hooks.send_profiling_results(self.profiling)


_context_var = ContextVar('mindsdb.context')
context = Context(_context_var)
