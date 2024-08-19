##############################################################################
#
# Copyright (c) 2001-2004 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Buffers
"""
from io import BytesIO

# copy_bytes controls the size of temp. strings for shuffling data around.
COPY_BYTES = 1 << 18  # 256K

# The maximum number of bytes to buffer in a simple string.
STRBUF_LIMIT = 8192


class FileBasedBuffer:
    remain = 0

    def __init__(self, file, from_buffer=None):
        self.file = file
        if from_buffer is not None:
            from_file = from_buffer.getfile()
            read_pos = from_file.tell()
            from_file.seek(0)
            while True:
                data = from_file.read(COPY_BYTES)
                if not data:
                    break
                file.write(data)
            self.remain = int(file.tell() - read_pos)
            from_file.seek(read_pos)
            file.seek(read_pos)

    def __len__(self):
        return self.remain

    def __bool__(self):
        return True

    def append(self, s):
        file = self.file
        read_pos = file.tell()
        file.seek(0, 2)
        file.write(s)
        file.seek(read_pos)
        self.remain = self.remain + len(s)

    def get(self, numbytes=-1, skip=False):
        file = self.file
        if not skip:
            read_pos = file.tell()
        if numbytes < 0:
            # Read all
            res = file.read()
        else:
            res = file.read(numbytes)
        if skip:
            self.remain -= len(res)
        else:
            file.seek(read_pos)
        return res

    def skip(self, numbytes, allow_prune=0):
        if self.remain < numbytes:
            raise ValueError(
                "Can't skip %d bytes in buffer of %d bytes" % (numbytes, self.remain)
            )
        self.file.seek(numbytes, 1)
        self.remain = self.remain - numbytes

    def newfile(self):
        raise NotImplementedError()

    def prune(self):
        file = self.file
        if self.remain == 0:
            read_pos = file.tell()
            file.seek(0, 2)
            sz = file.tell()
            file.seek(read_pos)
            if sz == 0:
                # Nothing to prune.
                return
        nf = self.newfile()
        while True:
            data = file.read(COPY_BYTES)
            if not data:
                break
            nf.write(data)
        self.file = nf

    def getfile(self):
        return self.file

    def close(self):
        if hasattr(self.file, "close"):
            self.file.close()
        self.remain = 0


class TempfileBasedBuffer(FileBasedBuffer):
    def __init__(self, from_buffer=None):
        FileBasedBuffer.__init__(self, self.newfile(), from_buffer)

    def newfile(self):
        from tempfile import TemporaryFile

        return TemporaryFile("w+b")


class BytesIOBasedBuffer(FileBasedBuffer):
    def __init__(self, from_buffer=None):
        if from_buffer is not None:
            FileBasedBuffer.__init__(self, BytesIO(), from_buffer)
        else:
            # Shortcut. :-)
            self.file = BytesIO()

    def newfile(self):
        return BytesIO()


def _is_seekable(fp):
    if hasattr(fp, "seekable"):
        return fp.seekable()
    return hasattr(fp, "seek") and hasattr(fp, "tell")


class ReadOnlyFileBasedBuffer(FileBasedBuffer):
    # used as wsgi.file_wrapper

    def __init__(self, file, block_size=32768):
        self.file = file
        self.block_size = block_size  # for __iter__

        # This is for the benefit of anyone that is attempting to wrap this
        # wsgi.file_wrapper in a WSGI middleware and wants to seek, this is
        # useful for instance for support Range requests
        if _is_seekable(self.file):
            if hasattr(self.file, "seekable"):
                self.seekable = self.file.seekable

            self.seek = self.file.seek
            self.tell = self.file.tell

    def prepare(self, size=None):
        if _is_seekable(self.file):
            start_pos = self.file.tell()
            self.file.seek(0, 2)
            end_pos = self.file.tell()
            self.file.seek(start_pos)
            fsize = end_pos - start_pos
            if size is None:
                self.remain = fsize
            else:
                self.remain = min(fsize, size)
        return self.remain

    def get(self, numbytes=-1, skip=False):
        # never read more than self.remain (it can be user-specified)
        if numbytes == -1 or numbytes > self.remain:
            numbytes = self.remain
        file = self.file
        if not skip:
            read_pos = file.tell()
        res = file.read(numbytes)
        if skip:
            self.remain -= len(res)
        else:
            file.seek(read_pos)
        return res

    def __iter__(self):  # called by task if self.filelike has no seek/tell
        return self

    def next(self):
        val = self.file.read(self.block_size)
        if not val:
            raise StopIteration
        return val

    __next__ = next  # py3

    def append(self, s):
        raise NotImplementedError


class OverflowableBuffer:
    """
    This buffer implementation has four stages:
    - No data
    - Bytes-based buffer
    - BytesIO-based buffer
    - Temporary file storage
    The first two stages are fastest for simple transfers.
    """

    overflowed = False
    buf = None
    strbuf = b""  # Bytes-based buffer.

    def __init__(self, overflow):
        # overflow is the maximum to be stored in a StringIO buffer.
        self.overflow = overflow

    def __len__(self):
        buf = self.buf
        if buf is not None:
            # use buf.__len__ rather than len(buf) FBO of not getting
            # OverflowError on Python 2
            return buf.__len__()
        else:
            return self.strbuf.__len__()

    def __bool__(self):
        # use self.__len__ rather than len(self) FBO of not getting
        # OverflowError on Python 2
        return self.__len__() > 0

    def _create_buffer(self):
        strbuf = self.strbuf
        if len(strbuf) >= self.overflow:
            self._set_large_buffer()
        else:
            self._set_small_buffer()
        buf = self.buf
        if strbuf:
            buf.append(self.strbuf)
            self.strbuf = b""
        return buf

    def _set_small_buffer(self):
        oldbuf = self.buf
        self.buf = BytesIOBasedBuffer(oldbuf)

        # Attempt to close the old buffer
        if hasattr(oldbuf, "close"):
            oldbuf.close()

        self.overflowed = False

    def _set_large_buffer(self):
        oldbuf = self.buf
        self.buf = TempfileBasedBuffer(oldbuf)

        # Attempt to close the old buffer
        if hasattr(oldbuf, "close"):
            oldbuf.close()

        self.overflowed = True

    def append(self, s):
        buf = self.buf
        if buf is None:
            strbuf = self.strbuf
            if len(strbuf) + len(s) < STRBUF_LIMIT:
                self.strbuf = strbuf + s
                return
            buf = self._create_buffer()
        buf.append(s)
        # use buf.__len__ rather than len(buf) FBO of not getting
        # OverflowError on Python 2
        sz = buf.__len__()
        if not self.overflowed:
            if sz >= self.overflow:
                self._set_large_buffer()

    def get(self, numbytes=-1, skip=False):
        buf = self.buf
        if buf is None:
            strbuf = self.strbuf
            if not skip:
                return strbuf
            buf = self._create_buffer()
        return buf.get(numbytes, skip)

    def skip(self, numbytes, allow_prune=False):
        buf = self.buf
        if buf is None:
            if allow_prune and numbytes == len(self.strbuf):
                # We could slice instead of converting to
                # a buffer, but that would eat up memory in
                # large transfers.
                self.strbuf = b""
                return
            buf = self._create_buffer()
        buf.skip(numbytes, allow_prune)

    def prune(self):
        """
        A potentially expensive operation that removes all data
        already retrieved from the buffer.
        """
        buf = self.buf
        if buf is None:
            self.strbuf = b""
            return
        buf.prune()
        if self.overflowed:
            # use buf.__len__ rather than len(buf) FBO of not getting
            # OverflowError on Python 2
            sz = buf.__len__()
            if sz < self.overflow:
                # Revert to a faster buffer.
                self._set_small_buffer()

    def getfile(self):
        buf = self.buf
        if buf is None:
            buf = self._create_buffer()
        return buf.getfile()

    def close(self):
        buf = self.buf
        if buf is not None:
            buf.close()
