# -*- coding: utf-8 -*-
#
# python-json-patch - An implementation of the JSON Patch format
# https://github.com/stefankoegl/python-json-patch
#
# Copyright (c) 2011 Stefan Kögl <stefan@skoegl.net>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. The name of the author may not be used to endorse or promote products
#    derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
# NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
# THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

""" Apply JSON-Patches (RFC 6902) """

from __future__ import unicode_literals

import collections
import copy
import functools
import json
import sys

try:
    from collections.abc import Sequence
except ImportError:  # Python 3
    from collections import Sequence

try:
    from types import MappingProxyType
except ImportError:
    # Python < 3.3
    MappingProxyType = dict

from jsonpointer import JsonPointer, JsonPointerException


_ST_ADD = 0
_ST_REMOVE = 1


try:
    from collections.abc import MutableMapping, MutableSequence

except ImportError:
    from collections import MutableMapping, MutableSequence
    str = unicode

# Will be parsed by setup.py to determine package metadata
__author__ = 'Stefan Kögl <stefan@skoegl.net>'
__version__ = '1.33'
__website__ = 'https://github.com/stefankoegl/python-json-patch'
__license__ = 'Modified BSD License'


# pylint: disable=E0611,W0404
if sys.version_info >= (3, 0):
    basestring = (bytes, str)  # pylint: disable=C0103,W0622


class JsonPatchException(Exception):
    """Base Json Patch exception"""


class InvalidJsonPatch(JsonPatchException):
    """ Raised if an invalid JSON Patch is created """


class JsonPatchConflict(JsonPatchException):
    """Raised if patch could not be applied due to conflict situation such as:
    - attempt to add object key when it already exists;
    - attempt to operate with nonexistence object key;
    - attempt to insert value to array at position beyond its size;
    - etc.
    """


class JsonPatchTestFailed(JsonPatchException, AssertionError):
    """ A Test operation failed """


def multidict(ordered_pairs):
    """Convert duplicate keys values to lists."""
    # read all values into lists
    mdict = collections.defaultdict(list)
    for key, value in ordered_pairs:
        mdict[key].append(value)

    return dict(
        # unpack lists that have only 1 item
        (key, values[0] if len(values) == 1 else values)
        for key, values in mdict.items()
    )


# The "object_pairs_hook" parameter is used to handle duplicate keys when
# loading a JSON object.
_jsonloads = functools.partial(json.loads, object_pairs_hook=multidict)


def apply_patch(doc, patch, in_place=False, pointer_cls=JsonPointer):
    """Apply list of patches to specified json document.

    :param doc: Document object.
    :type doc: dict

    :param patch: JSON patch as list of dicts or raw JSON-encoded string.
    :type patch: list or str

    :param in_place: While :const:`True` patch will modify target document.
                     By default patch will be applied to document copy.
    :type in_place: bool

    :param pointer_cls: JSON pointer class to use.
    :type pointer_cls: Type[JsonPointer]

    :return: Patched document object.
    :rtype: dict

    >>> doc = {'foo': 'bar'}
    >>> patch = [{'op': 'add', 'path': '/baz', 'value': 'qux'}]
    >>> other = apply_patch(doc, patch)
    >>> doc is not other
    True
    >>> other == {'foo': 'bar', 'baz': 'qux'}
    True
    >>> patch = [{'op': 'add', 'path': '/baz', 'value': 'qux'}]
    >>> apply_patch(doc, patch, in_place=True) == {'foo': 'bar', 'baz': 'qux'}
    True
    >>> doc == other
    True
    """

    if isinstance(patch, basestring):
        patch = JsonPatch.from_string(patch, pointer_cls=pointer_cls)
    else:
        patch = JsonPatch(patch, pointer_cls=pointer_cls)
    return patch.apply(doc, in_place)


def make_patch(src, dst, pointer_cls=JsonPointer):
    """Generates patch by comparing two document objects. Actually is
    a proxy to :meth:`JsonPatch.from_diff` method.

    :param src: Data source document object.
    :type src: dict

    :param dst: Data source document object.
    :type dst: dict

    :param pointer_cls: JSON pointer class to use.
    :type pointer_cls: Type[JsonPointer]

    >>> src = {'foo': 'bar', 'numbers': [1, 3, 4, 8]}
    >>> dst = {'baz': 'qux', 'numbers': [1, 4, 7]}
    >>> patch = make_patch(src, dst)
    >>> new = patch.apply(src)
    >>> new == dst
    True
    """

    return JsonPatch.from_diff(src, dst, pointer_cls=pointer_cls)


class PatchOperation(object):
    """A single operation inside a JSON Patch."""

    def __init__(self, operation, pointer_cls=JsonPointer):
        self.pointer_cls = pointer_cls

        if not operation.__contains__('path'):
            raise InvalidJsonPatch("Operation must have a 'path' member")

        if isinstance(operation['path'], self.pointer_cls):
            self.location = operation['path'].path
            self.pointer = operation['path']
        else:
            self.location = operation['path']
            try:
                self.pointer = self.pointer_cls(self.location)
            except TypeError as ex:
                raise InvalidJsonPatch("Invalid 'path'")

        self.operation = operation

    def apply(self, obj):
        """Abstract method that applies a patch operation to the specified object."""
        raise NotImplementedError('should implement the patch operation.')

    def __hash__(self):
        return hash(frozenset(self.operation.items()))

    def __eq__(self, other):
        if not isinstance(other, PatchOperation):
            return False
        return self.operation == other.operation

    def __ne__(self, other):
        return not(self == other)

    @property
    def path(self):
        return '/'.join(self.pointer.parts[:-1])

    @property
    def key(self):
        try:
            return int(self.pointer.parts[-1])
        except ValueError:
            return self.pointer.parts[-1]

    @key.setter
    def key(self, value):
        self.pointer.parts[-1] = str(value)
        self.location = self.pointer.path
        self.operation['path'] = self.location


class RemoveOperation(PatchOperation):
    """Removes an object property or an array element."""

    def apply(self, obj):
        subobj, part = self.pointer.to_last(obj)

        if isinstance(subobj, Sequence) and not isinstance(part, int):
            raise JsonPointerException("invalid array index '{0}'".format(part))

        try:
            del subobj[part]
        except (KeyError, IndexError) as ex:
            msg = "can't remove a non-existent object '{0}'".format(part)
            raise JsonPatchConflict(msg)

        return obj

    def _on_undo_remove(self, path, key):
        if self.path == path:
            if self.key >= key:
                self.key += 1
            else:
                key -= 1
        return key

    def _on_undo_add(self, path, key):
        if self.path == path:
            if self.key > key:
                self.key -= 1
            else:
                key -= 1
        return key


class AddOperation(PatchOperation):
    """Adds an object property or an array element."""

    def apply(self, obj):
        try:
            value = self.operation["value"]
        except KeyError as ex:
            raise InvalidJsonPatch(
                "The operation does not contain a 'value' member")

        subobj, part = self.pointer.to_last(obj)

        if isinstance(subobj, MutableSequence):
            if part == '-':
                subobj.append(value)  # pylint: disable=E1103

            elif part > len(subobj) or part < 0:
                raise JsonPatchConflict("can't insert outside of list")

            else:
                subobj.insert(part, value)  # pylint: disable=E1103

        elif isinstance(subobj, MutableMapping):
            if part is None:
                obj = value  # we're replacing the root
            else:
                subobj[part] = value

        else:
            if part is None:
                raise TypeError("invalid document type {0}".format(type(subobj)))
            else:
                raise JsonPatchConflict("unable to fully resolve json pointer {0}, part {1}".format(self.location, part))
        return obj

    def _on_undo_remove(self, path, key):
        if self.path == path:
            if self.key > key:
                self.key += 1
            else:
                key += 1
        return key

    def _on_undo_add(self, path, key):
        if self.path == path:
            if self.key > key:
                self.key -= 1
            else:
                key += 1
        return key


class ReplaceOperation(PatchOperation):
    """Replaces an object property or an array element by a new value."""

    def apply(self, obj):
        try:
            value = self.operation["value"]
        except KeyError as ex:
            raise InvalidJsonPatch(
                "The operation does not contain a 'value' member")

        subobj, part = self.pointer.to_last(obj)

        if part is None:
            return value

        if part == "-":
            raise InvalidJsonPatch("'path' with '-' can't be applied to 'replace' operation")

        if isinstance(subobj, MutableSequence):
            if part >= len(subobj) or part < 0:
                raise JsonPatchConflict("can't replace outside of list")

        elif isinstance(subobj, MutableMapping):
            if part not in subobj:
                msg = "can't replace a non-existent object '{0}'".format(part)
                raise JsonPatchConflict(msg)
        else:
            if part is None:
                raise TypeError("invalid document type {0}".format(type(subobj)))
            else:
                raise JsonPatchConflict("unable to fully resolve json pointer {0}, part {1}".format(self.location, part))

        subobj[part] = value
        return obj

    def _on_undo_remove(self, path, key):
        return key

    def _on_undo_add(self, path, key):
        return key


class MoveOperation(PatchOperation):
    """Moves an object property or an array element to a new location."""

    def apply(self, obj):
        try:
            if isinstance(self.operation['from'], self.pointer_cls):
                from_ptr = self.operation['from']
            else:
                from_ptr = self.pointer_cls(self.operation['from'])
        except KeyError as ex:
            raise InvalidJsonPatch(
                "The operation does not contain a 'from' member")

        subobj, part = from_ptr.to_last(obj)
        try:
            value = subobj[part]
        except (KeyError, IndexError) as ex:
            raise JsonPatchConflict(str(ex))

        # If source and target are equal, this is a no-op
        if self.pointer == from_ptr:
            return obj

        if isinstance(subobj, MutableMapping) and \
                self.pointer.contains(from_ptr):
            raise JsonPatchConflict('Cannot move values into their own children')

        obj = RemoveOperation({
            'op': 'remove',
            'path': self.operation['from']
        }, pointer_cls=self.pointer_cls).apply(obj)

        obj = AddOperation({
            'op': 'add',
            'path': self.location,
            'value': value
        }, pointer_cls=self.pointer_cls).apply(obj)

        return obj

    @property
    def from_path(self):
        from_ptr = self.pointer_cls(self.operation['from'])
        return '/'.join(from_ptr.parts[:-1])

    @property
    def from_key(self):
        from_ptr = self.pointer_cls(self.operation['from'])
        try:
            return int(from_ptr.parts[-1])
        except TypeError:
            return from_ptr.parts[-1]

    @from_key.setter
    def from_key(self, value):
        from_ptr = self.pointer_cls(self.operation['from'])
        from_ptr.parts[-1] = str(value)
        self.operation['from'] = from_ptr.path

    def _on_undo_remove(self, path, key):
        if self.from_path == path:
            if self.from_key >= key:
                self.from_key += 1
            else:
                key -= 1
        if self.path == path:
            if self.key > key:
                self.key += 1
            else:
                key += 1
        return key

    def _on_undo_add(self, path, key):
        if self.from_path == path:
            if self.from_key > key:
                self.from_key -= 1
            else:
                key -= 1
        if self.path == path:
            if self.key > key:
                self.key -= 1
            else:
                key += 1
        return key


class TestOperation(PatchOperation):
    """Test value by specified location."""

    def apply(self, obj):
        try:
            subobj, part = self.pointer.to_last(obj)
            if part is None:
                val = subobj
            else:
                val = self.pointer.walk(subobj, part)
        except JsonPointerException as ex:
            raise JsonPatchTestFailed(str(ex))

        try:
            value = self.operation['value']
        except KeyError as ex:
            raise InvalidJsonPatch(
                "The operation does not contain a 'value' member")

        if val != value:
            msg = '{0} ({1}) is not equal to tested value {2} ({3})'
            raise JsonPatchTestFailed(msg.format(val, type(val),
                                                 value, type(value)))

        return obj


class CopyOperation(PatchOperation):
    """ Copies an object property or an array element to a new location """

    def apply(self, obj):
        try:
            from_ptr = self.pointer_cls(self.operation['from'])
        except KeyError as ex:
            raise InvalidJsonPatch(
                "The operation does not contain a 'from' member")

        subobj, part = from_ptr.to_last(obj)
        try:
            value = copy.deepcopy(subobj[part])
        except (KeyError, IndexError) as ex:
            raise JsonPatchConflict(str(ex))

        obj = AddOperation({
            'op': 'add',
            'path': self.location,
            'value': value
        }, pointer_cls=self.pointer_cls).apply(obj)

        return obj


class JsonPatch(object):
    json_dumper = staticmethod(json.dumps)
    json_loader = staticmethod(_jsonloads)

    operations = MappingProxyType({
        'remove': RemoveOperation,
        'add': AddOperation,
        'replace': ReplaceOperation,
        'move': MoveOperation,
        'test': TestOperation,
        'copy': CopyOperation,
    })

    """A JSON Patch is a list of Patch Operations.

    >>> patch = JsonPatch([
    ...     {'op': 'add', 'path': '/foo', 'value': 'bar'},
    ...     {'op': 'add', 'path': '/baz', 'value': [1, 2, 3]},
    ...     {'op': 'remove', 'path': '/baz/1'},
    ...     {'op': 'test', 'path': '/baz', 'value': [1, 3]},
    ...     {'op': 'replace', 'path': '/baz/0', 'value': 42},
    ...     {'op': 'remove', 'path': '/baz/1'},
    ... ])
    >>> doc = {}
    >>> result = patch.apply(doc)
    >>> expected = {'foo': 'bar', 'baz': [42]}
    >>> result == expected
    True

    JsonPatch object is iterable, so you can easily access each patch
    statement in a loop:

    >>> lpatch = list(patch)
    >>> expected = {'op': 'add', 'path': '/foo', 'value': 'bar'}
    >>> lpatch[0] == expected
    True
    >>> lpatch == patch.patch
    True

    Also JsonPatch could be converted directly to :class:`bool` if it contains
    any operation statements:

    >>> bool(patch)
    True
    >>> bool(JsonPatch([]))
    False

    This behavior is very handy with :func:`make_patch` to write more readable
    code:

    >>> old = {'foo': 'bar', 'numbers': [1, 3, 4, 8]}
    >>> new = {'baz': 'qux', 'numbers': [1, 4, 7]}
    >>> patch = make_patch(old, new)
    >>> if patch:
    ...     # document have changed, do something useful
    ...     patch.apply(old)    #doctest: +ELLIPSIS
    {...}
    """
    def __init__(self, patch, pointer_cls=JsonPointer):
        self.patch = patch
        self.pointer_cls = pointer_cls

        # Verify that the structure of the patch document
        # is correct by retrieving each patch element.
        # Much of the validation is done in the initializer
        # though some is delayed until the patch is applied.
        for op in self.patch:
            # We're only checking for basestring in the following check
            # for two reasons:
            #
            # - It should come from JSON, which only allows strings as
            #   dictionary keys, so having a string here unambiguously means
            #   someone used: {"op": ..., ...} instead of [{"op": ..., ...}].
            #
            # - There's no possible false positive: if someone give a sequence
            #   of mappings, this won't raise.
            if isinstance(op, basestring):
                raise InvalidJsonPatch("Document is expected to be sequence of "
                                       "operations, got a sequence of strings.")

            self._get_operation(op)

    def __str__(self):
        """str(self) -> self.to_string()"""
        return self.to_string()

    def __bool__(self):
        return bool(self.patch)

    __nonzero__ = __bool__

    def __iter__(self):
        return iter(self.patch)

    def __hash__(self):
        return hash(tuple(self._ops))

    def __eq__(self, other):
        if not isinstance(other, JsonPatch):
            return False
        return self._ops == other._ops

    def __ne__(self, other):
        return not(self == other)

    @classmethod
    def from_string(cls, patch_str, loads=None, pointer_cls=JsonPointer):
        """Creates JsonPatch instance from string source.

        :param patch_str: JSON patch as raw string.
        :type patch_str: str

        :param loads: A function of one argument that loads a serialized
                      JSON string.
        :type loads: function

        :param pointer_cls: JSON pointer class to use.
        :type pointer_cls: Type[JsonPointer]

        :return: :class:`JsonPatch` instance.
        """
        json_loader = loads or cls.json_loader
        patch = json_loader(patch_str)
        return cls(patch, pointer_cls=pointer_cls)

    @classmethod
    def from_diff(
            cls, src, dst, optimization=True, dumps=None,
            pointer_cls=JsonPointer,
    ):
        """Creates JsonPatch instance based on comparison of two document
        objects. Json patch would be created for `src` argument against `dst`
        one.

        :param src: Data source document object.
        :type src: dict

        :param dst: Data source document object.
        :type dst: dict

        :param dumps: A function of one argument that produces a serialized
                      JSON string.
        :type dumps: function

        :param pointer_cls: JSON pointer class to use.
        :type pointer_cls: Type[JsonPointer]

        :return: :class:`JsonPatch` instance.

        >>> src = {'foo': 'bar', 'numbers': [1, 3, 4, 8]}
        >>> dst = {'baz': 'qux', 'numbers': [1, 4, 7]}
        >>> patch = JsonPatch.from_diff(src, dst)
        >>> new = patch.apply(src)
        >>> new == dst
        True
        """
        json_dumper = dumps or cls.json_dumper
        builder = DiffBuilder(src, dst, json_dumper, pointer_cls=pointer_cls)
        builder._compare_values('', None, src, dst)
        ops = list(builder.execute())
        return cls(ops, pointer_cls=pointer_cls)

    def to_string(self, dumps=None):
        """Returns patch set as JSON string."""
        json_dumper = dumps or self.json_dumper
        return json_dumper(self.patch)

    @property
    def _ops(self):
        return tuple(map(self._get_operation, self.patch))

    def apply(self, obj, in_place=False):
        """Applies the patch to a given object.

        :param obj: Document object.
        :type obj: dict

        :param in_place: Tweaks the way how patch would be applied - directly to
                         specified `obj` or to its copy.
        :type in_place: bool

        :return: Modified `obj`.
        """

        if not in_place:
            obj = copy.deepcopy(obj)

        for operation in self._ops:
            obj = operation.apply(obj)

        return obj

    def _get_operation(self, operation):
        if 'op' not in operation:
            raise InvalidJsonPatch("Operation does not contain 'op' member")

        op = operation['op']

        if not isinstance(op, basestring):
            raise InvalidJsonPatch("Operation's op must be a string")

        if op not in self.operations:
            raise InvalidJsonPatch("Unknown operation {0!r}".format(op))

        cls = self.operations[op]
        return cls(operation, pointer_cls=self.pointer_cls)


class DiffBuilder(object):

    def __init__(self, src_doc, dst_doc, dumps=json.dumps, pointer_cls=JsonPointer):
        self.dumps = dumps
        self.pointer_cls = pointer_cls
        self.index_storage = [{}, {}]
        self.index_storage2 = [[], []]
        self.__root = root = []
        self.src_doc = src_doc
        self.dst_doc = dst_doc
        root[:] = [root, root, None]

    def store_index(self, value, index, st):
        typed_key = (value, type(value))
        try:
            storage = self.index_storage[st]
            stored = storage.get(typed_key)
            if stored is None:
                storage[typed_key] = [index]
            else:
                storage[typed_key].append(index)

        except TypeError:
            self.index_storage2[st].append((typed_key, index))

    def take_index(self, value, st):
        typed_key = (value, type(value))
        try:
            stored = self.index_storage[st].get(typed_key)
            if stored:
                return stored.pop()

        except TypeError:
            storage = self.index_storage2[st]
            for i in range(len(storage)-1, -1, -1):
                if storage[i][0] == typed_key:
                    return storage.pop(i)[1]

    def insert(self, op):
        root = self.__root
        last = root[0]
        last[1] = root[0] = [last, root, op]
        return root[0]

    def remove(self, index):
        link_prev, link_next, _ = index
        link_prev[1] = link_next
        link_next[0] = link_prev
        index[:] = []

    def iter_from(self, start):
        root = self.__root
        curr = start[1]
        while curr is not root:
            yield curr[2]
            curr = curr[1]

    def __iter__(self):
        root = self.__root
        curr = root[1]
        while curr is not root:
            yield curr[2]
            curr = curr[1]

    def execute(self):
        root = self.__root
        curr = root[1]
        while curr is not root:
            if curr[1] is not root:
                op_first, op_second = curr[2], curr[1][2]
                if op_first.location == op_second.location and \
                        type(op_first) == RemoveOperation and \
                        type(op_second) == AddOperation:
                    yield ReplaceOperation({
                        'op': 'replace',
                        'path': op_second.location,
                        'value': op_second.operation['value'],
                    }, pointer_cls=self.pointer_cls).operation
                    curr = curr[1][1]
                    continue

            yield curr[2].operation
            curr = curr[1]

    def _item_added(self, path, key, item):
        index = self.take_index(item, _ST_REMOVE)
        if index is not None:
            op = index[2]
            if type(op.key) == int and type(key) == int:
                for v in self.iter_from(index):
                    op.key = v._on_undo_remove(op.path, op.key)

            self.remove(index)
            if op.location != _path_join(path, key):
                new_op = MoveOperation({
                    'op': 'move',
                    'from': op.location,
                    'path': _path_join(path, key),
                }, pointer_cls=self.pointer_cls)
                self.insert(new_op)
        else:
            new_op = AddOperation({
                'op': 'add',
                'path': _path_join(path, key),
                'value': item,
            }, pointer_cls=self.pointer_cls)
            new_index = self.insert(new_op)
            self.store_index(item, new_index, _ST_ADD)

    def _item_removed(self, path, key, item):
        new_op = RemoveOperation({
            'op': 'remove',
            'path': _path_join(path, key),
        }, pointer_cls=self.pointer_cls)
        index = self.take_index(item, _ST_ADD)
        new_index = self.insert(new_op)
        if index is not None:
            op = index[2]
            # We can't rely on the op.key type since PatchOperation casts
            # the .key property to int and this path wrongly ends up being taken
            # for numeric string dict keys while the intention is to only handle lists.
            # So we do an explicit check on the item affected by the op instead.
            added_item = op.pointer.to_last(self.dst_doc)[0]
            if type(added_item) == list:
                for v in self.iter_from(index):
                    op.key = v._on_undo_add(op.path, op.key)

            self.remove(index)
            if new_op.location != op.location:
                new_op = MoveOperation({
                    'op': 'move',
                    'from': new_op.location,
                    'path': op.location,
                }, pointer_cls=self.pointer_cls)
                new_index[2] = new_op

            else:
                self.remove(new_index)

        else:
            self.store_index(item, new_index, _ST_REMOVE)

    def _item_replaced(self, path, key, item):
        self.insert(ReplaceOperation({
            'op': 'replace',
            'path': _path_join(path, key),
            'value': item,
        }, pointer_cls=self.pointer_cls))

    def _compare_dicts(self, path, src, dst):
        src_keys = set(src.keys())
        dst_keys = set(dst.keys())
        added_keys = dst_keys - src_keys
        removed_keys = src_keys - dst_keys

        for key in removed_keys:
            self._item_removed(path, str(key), src[key])

        for key in added_keys:
            self._item_added(path, str(key), dst[key])

        for key in src_keys & dst_keys:
            self._compare_values(path, key, src[key], dst[key])

    def _compare_lists(self, path, src, dst):
        len_src, len_dst = len(src), len(dst)
        max_len = max(len_src, len_dst)
        min_len = min(len_src, len_dst)
        for key in range(max_len):
            if key < min_len:
                old, new = src[key], dst[key]
                if old == new:
                    continue

                elif isinstance(old, MutableMapping) and \
                    isinstance(new, MutableMapping):
                    self._compare_dicts(_path_join(path, key), old, new)

                elif isinstance(old, MutableSequence) and \
                        isinstance(new, MutableSequence):
                    self._compare_lists(_path_join(path, key), old, new)

                else:
                    self._item_removed(path, key, old)
                    self._item_added(path, key, new)

            elif len_src > len_dst:
                self._item_removed(path, len_dst, src[key])

            else:
                self._item_added(path, key, dst[key])

    def _compare_values(self, path, key, src, dst):
        if isinstance(src, MutableMapping) and \
                isinstance(dst, MutableMapping):
            self._compare_dicts(_path_join(path, key), src, dst)

        elif isinstance(src, MutableSequence) and \
                isinstance(dst, MutableSequence):
            self._compare_lists(_path_join(path, key), src, dst)

        # To ensure we catch changes to JSON, we can't rely on a simple
        # src == dst, because it would not recognize the difference between
        # 1 and True, among other things. Using json.dumps is the most
        # fool-proof way to ensure we catch type changes that matter to JSON
        # and ignore those that don't. The performance of this could be
        # improved by doing more direct type checks, but we'd need to be
        # careful to accept type changes that don't matter when JSONified.
        elif self.dumps(src) == self.dumps(dst):
            return

        else:
            self._item_replaced(path, key, dst)


def _path_join(path, key):
    if key is None:
        return path

    return path + '/' + str(key).replace('~', '~0').replace('/', '~1')
