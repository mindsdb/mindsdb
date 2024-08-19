"""@private"""

from datetime import datetime
from typing import Optional, Dict

from langfuse.model import PromptClient


DEFAULT_PROMPT_CACHE_TTL_SECONDS = 60


class PromptCacheItem:
    def __init__(self, prompt: PromptClient, ttl_seconds: int):
        self.value = prompt
        self._expiry = ttl_seconds + self.get_epoch_seconds()

    def is_expired(self) -> bool:
        return self.get_epoch_seconds() > self._expiry

    @staticmethod
    def get_epoch_seconds() -> int:
        return int(datetime.now().timestamp())


class PromptCache:
    _cache: Dict[str, PromptCacheItem]

    def __init__(self):
        self._cache = {}

    def get(self, key: str) -> Optional[PromptCacheItem]:
        return self._cache.get(key, None)

    def set(self, key: str, value: PromptClient, ttl_seconds: Optional[int]):
        if ttl_seconds is None:
            ttl_seconds = DEFAULT_PROMPT_CACHE_TTL_SECONDS

        self._cache[key] = PromptCacheItem(value, ttl_seconds)

    @staticmethod
    def generate_cache_key(
        name: str, *, version: Optional[int], label: Optional[str]
    ) -> str:
        parts = [name]

        if version is not None:
            parts.append(f"version:{version}")

        elif label is not None:
            parts.append(f"label:{label}")

        else:
            # Default to production labeled prompt
            parts.append("label:production")

        return "-".join(parts)
