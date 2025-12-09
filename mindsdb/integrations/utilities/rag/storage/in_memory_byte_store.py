"""Custom in-memory byte store implementation to replace langchain's InMemoryByteStore"""

from typing import List, Tuple, Any, Dict


class InMemoryByteStore:
    """
    Custom implementation of InMemoryByteStore to replace langchain's version.
    Stores key-value pairs in memory using a dictionary.
    """

    def __init__(self):
        """Initialize the in-memory store with an empty dictionary"""
        self._store: Dict[str, Any] = {}

    def mset(self, key_value_pairs: List[Tuple[str, Any]]) -> None:
        """
        Store multiple key-value pairs

        Args:
            key_value_pairs: List of (key, value) tuples to store
        """
        for key, value in key_value_pairs:
            self._store[str(key)] = value

    def mget(self, keys: List[str]) -> List[Any]:
        """
        Retrieve multiple values by keys

        Args:
            keys: List of keys to retrieve

        Returns:
            List of values corresponding to keys (None for missing keys)
        """
        return [self._store.get(str(key)) for key in keys]

    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieve a single value by key

        Args:
            key: Key to retrieve
            default: Default value to return if key is not found

        Returns:
            Value associated with key, or default if not found
        """
        return self._store.get(str(key), default)

    def set(self, key: str, value: Any) -> None:
        """
        Store a single key-value pair

        Args:
            key: Key to store
            value: Value to store
        """
        self._store[str(key)] = value

    def delete(self, key: str) -> bool:
        """
        Delete a key-value pair

        Args:
            key: Key to delete

        Returns:
            True if key was found and deleted, False otherwise
        """
        key_str = str(key)
        if key_str in self._store:
            del self._store[key_str]
            return True
        return False

    def clear(self) -> None:
        """Clear all stored key-value pairs"""
        self._store.clear()

    def keys(self) -> List[str]:
        """
        Get all keys in the store

        Returns:
            List of all keys
        """
        return list(self._store.keys())

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in the store"""
        return str(key) in self._store

    def __len__(self) -> int:
        """Get the number of key-value pairs in the store"""
        return len(self._store)

