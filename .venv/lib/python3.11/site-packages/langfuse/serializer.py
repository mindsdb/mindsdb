"""@private"""

from asyncio import Queue
from datetime import date, datetime
from dataclasses import is_dataclass, asdict
import enum
from json import JSONEncoder
from typing import Any
from uuid import UUID
from collections.abc import Sequence
from langfuse.api.core import serialize_datetime
from pathlib import Path

from pydantic import BaseModel

# Attempt to import Serializable
try:
    from langchain.load.serializable import Serializable
except ImportError:
    # If Serializable is not available, set it to NoneType
    Serializable = type(None)


class EventSerializer(JSONEncoder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen = set()  # Track seen objects to detect circular references

    def default(self, obj: Any):
        if isinstance(obj, (datetime)):
            # Timezone-awareness check
            return serialize_datetime(obj)

        # LlamaIndex StreamingAgentChatResponse and StreamingResponse is not serializable by default as it is a generator
        # Attention: These LlamaIndex objects are a also a dataclasses, so check for it first
        if "Streaming" in type(obj).__name__:
            return str(obj)

        if isinstance(obj, enum.Enum):
            return obj.value

        if isinstance(obj, Queue):
            return type(obj).__name__

        if is_dataclass(obj):
            return asdict(obj)

        if isinstance(obj, UUID):
            return str(obj)

        if isinstance(obj, bytes):
            return obj.decode("utf-8")

        if isinstance(obj, (date)):
            return obj.isoformat()

        if isinstance(obj, BaseModel):
            return obj.dict()

        if isinstance(obj, Path):
            return str(obj)

        # if langchain is not available, the Serializable type is NoneType
        if Serializable is not None and isinstance(obj, Serializable):
            return obj.to_json()

        # Standard JSON-encodable types
        if isinstance(obj, (dict, list, str, int, float, type(None))):
            return obj

        if isinstance(obj, (tuple, set, frozenset)):
            return list(obj)

        # Important: this needs to be always checked after str and bytes types
        # Useful for serializing protobuf messages
        if isinstance(obj, Sequence):
            return [self.default(item) for item in obj]

        if hasattr(obj, "__slots__"):
            return self.default(
                {slot: getattr(obj, slot, None) for slot in obj.__slots__}
            )
        elif hasattr(obj, "__dict__"):
            obj_id = id(obj)

            if obj_id in self.seen:
                # Break on circular references
                return type(obj).__name__
            else:
                self.seen.add(obj_id)
                result = {k: self.default(v) for k, v in vars(obj).items()}
                self.seen.remove(obj_id)

                return result

        else:
            # Return object type rather than JSONEncoder.default(obj) which simply raises a TypeError
            return f"<{type(obj).__name__}>"

    def encode(self, obj: Any) -> str:
        self.seen.clear()  # Clear seen objects before each encode call

        try:
            return super().encode(obj)
        except Exception:
            return f'"<not serializable object of type: {type(obj).__name__}>"'  # escaping the string to avoid JSON parsing errors
