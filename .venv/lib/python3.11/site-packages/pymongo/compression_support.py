# Copyright 2018 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import warnings
from typing import Any, Iterable, Optional, Union

from pymongo.hello import HelloCompat
from pymongo.helpers import _SENSITIVE_COMMANDS

_SUPPORTED_COMPRESSORS = {"snappy", "zlib", "zstd"}
_NO_COMPRESSION = {HelloCompat.CMD, HelloCompat.LEGACY_CMD}
_NO_COMPRESSION.update(_SENSITIVE_COMMANDS)


def _have_snappy() -> bool:
    try:
        import snappy  # type:ignore[import]  # noqa: F401

        return True
    except ImportError:
        return False


def _have_zlib() -> bool:
    try:
        import zlib  # noqa: F401

        return True
    except ImportError:
        return False


def _have_zstd() -> bool:
    try:
        import zstandard  # noqa: F401

        return True
    except ImportError:
        return False


def validate_compressors(dummy: Any, value: Union[str, Iterable[str]]) -> list[str]:
    try:
        # `value` is string.
        compressors = value.split(",")  # type: ignore[union-attr]
    except AttributeError:
        # `value` is an iterable.
        compressors = list(value)

    for compressor in compressors[:]:
        if compressor not in _SUPPORTED_COMPRESSORS:
            compressors.remove(compressor)
            warnings.warn(f"Unsupported compressor: {compressor}", stacklevel=2)
        elif compressor == "snappy" and not _have_snappy():
            compressors.remove(compressor)
            warnings.warn(
                "Wire protocol compression with snappy is not available. "
                "You must install the python-snappy module for snappy support.",
                stacklevel=2,
            )
        elif compressor == "zlib" and not _have_zlib():
            compressors.remove(compressor)
            warnings.warn(
                "Wire protocol compression with zlib is not available. "
                "The zlib module is not available.",
                stacklevel=2,
            )
        elif compressor == "zstd" and not _have_zstd():
            compressors.remove(compressor)
            warnings.warn(
                "Wire protocol compression with zstandard is not available. "
                "You must install the zstandard module for zstandard support.",
                stacklevel=2,
            )
    return compressors


def validate_zlib_compression_level(option: str, value: Any) -> int:
    try:
        level = int(value)
    except Exception:
        raise TypeError(f"{option} must be an integer, not {value!r}.") from None
    if level < -1 or level > 9:
        raise ValueError("%s must be between -1 and 9, not %d." % (option, level))
    return level


class CompressionSettings:
    def __init__(self, compressors: list[str], zlib_compression_level: int):
        self.compressors = compressors
        self.zlib_compression_level = zlib_compression_level

    def get_compression_context(
        self, compressors: Optional[list[str]]
    ) -> Union[SnappyContext, ZlibContext, ZstdContext, None]:
        if compressors:
            chosen = compressors[0]
            if chosen == "snappy":
                return SnappyContext()
            elif chosen == "zlib":
                return ZlibContext(self.zlib_compression_level)
            elif chosen == "zstd":
                return ZstdContext()
            return None
        return None


class SnappyContext:
    compressor_id = 1

    @staticmethod
    def compress(data: bytes) -> bytes:
        import snappy

        return snappy.compress(data)


class ZlibContext:
    compressor_id = 2

    def __init__(self, level: int):
        self.level = level

    def compress(self, data: bytes) -> bytes:
        import zlib

        return zlib.compress(data, self.level)


class ZstdContext:
    compressor_id = 3

    @staticmethod
    def compress(data: bytes) -> bytes:
        # ZstdCompressor is not thread safe.
        # TODO: Use a pool?
        import zstandard

        return zstandard.ZstdCompressor().compress(data)


def decompress(data: bytes, compressor_id: int) -> bytes:
    if compressor_id == SnappyContext.compressor_id:
        # python-snappy doesn't support the buffer interface.
        # https://github.com/andrix/python-snappy/issues/65
        # This only matters when data is a memoryview since
        # id(bytes(data)) == id(data) when data is a bytes.
        import snappy

        return snappy.uncompress(bytes(data))
    elif compressor_id == ZlibContext.compressor_id:
        import zlib

        return zlib.decompress(data)
    elif compressor_id == ZstdContext.compressor_id:
        # ZstdDecompressor is not thread safe.
        # TODO: Use a pool?
        import zstandard

        return zstandard.ZstdDecompressor().decompress(data)
    else:
        raise ValueError("Unknown compressorId %d" % (compressor_id,))
