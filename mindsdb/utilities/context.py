from contextvars import ContextVar
from typing import Any
from copy import deepcopy
from contextlib import contextmanager

from mindsdb.utilities.constants import DEFAULT_COMPANY_ID, DEFAULT_USER_ID


class Context:
    """Thread independent storage"""

    __slots__ = ("_storage",)

    def __init__(self, storage) -> None:
        object.__setattr__(self, "_storage", storage)
        self.set_default()

    def set_default(self) -> None:
        self._storage.set(
            {
                "company_id": DEFAULT_COMPANY_ID,
                "user_id": DEFAULT_USER_ID,
                # When True, DB queries should be scoped by ctx.user_id (in addition to company_id).
                # Services can intentionally disable this to perform company-wide reads and apply
                # their own permissioning layer on top.
                "enforce_user_id": True,
                "session_id": "",
                "task_id": None,
                "user_class": 0,
                "profiling": {
                    "level": 0,
                    "enabled": False,
                    "pointer": None,
                    "tree": None,
                },
                "used_handlers": set(),
                "params": {},
            }
        )

    @contextmanager
    def without_user_id_scope(self):
        """Temporarily disable user_id scoping in this context."""
        previous = getattr(self, "enforce_user_id", True)
        self.enforce_user_id = False
        try:
            yield
        finally:
            self.enforce_user_id = previous

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
        del storage[name]
        self._storage.set(storage)

    def dump(self) -> dict:
        storage = deepcopy(self._storage.get({}))
        return storage

    def load(self, storage: dict) -> None:
        self._storage.set(storage)

    def get_metadata(self, **kwargs) -> dict:
        return {
            "company_id": self.company_id if self.company_id else DEFAULT_COMPANY_ID,
            "user_id": self.user_id if self.user_id else DEFAULT_USER_ID,
            "session_id": self.session_id,
            "enforce_user_id": self.enforce_user_id,
            "user_class": self.user_class,
            **kwargs,
        }


_context_var = ContextVar("mindsdb.context")
context = Context(_context_var)
