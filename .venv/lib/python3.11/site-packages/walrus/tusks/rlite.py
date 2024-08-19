import fnmatch
import sys
import unittest

from hirlite.hirlite import Rlite

from walrus import *
from walrus.tusks.helpers import TestHelper


class WalrusLite(Walrus):
    _invalid_callbacks = ('SET', 'MSET', 'LSET')

    def __init__(self, filename=':memory:', encoding='utf-8'):
        self._filename = filename
        self._encoding = encoding
        self._db = Rlite(path=filename, encoding=encoding)
        self.response_callbacks = self.__class__.RESPONSE_CALLBACKS.copy()
        for callback in self._invalid_callbacks:
            del self.response_callbacks[callback]

    def execute_command(self, *args, **options):
        command_name = args[0]
        result = self._db.command(*args)
        return self.parse_response(result, command_name, **options)

    def parse_response(self, result, command_name, **options):
        try:
            return self.response_callbacks[command_name.upper()](
                result, **options)
        except KeyError:
            return result

    def __repr__(self):
        if self._filename == ':memory:':
            db_file = 'in-memory database'
        else:
            db_file = self._filename
        return '<WalrusLite: %s>' % db_file

    def _filtered_scan(self, results, match=None, count=None):
        if match is not None:
            results = fnmatch.filter(results, match)
        if count:
            results = results[:count]
        return results

    def hscan_iter(self, key, match=None, count=None):
        return self._filtered_scan(self.hgetall(key), match, count)

    def sscan_iter(self, key, match=None, count=None):
        return self._filtered_scan(self.smembers(key), match, count)

    def zscan_iter(self, key, match=None, count=None):
        return self._filtered_scan(self.zrange(key, 0, -1), match, count)


class TestWalrusLite(TestHelper, unittest.TestCase):
    def setUp(self):
        self.db = WalrusLite()

    def test_list_set_delete_item(self):
        l = self.db.List('list_obj')
        l.clear()
        l.extend(['i1', 'i2', 'i3', 'i4'])
        l[-1] = 'ix'
        l[1] = 'iy'
        self.assertEqual(list(l), ['i1', 'iy', 'i3', 'ix'])

        l.prepend('nuggie')
        for idx in [-1, 2, 9]:
            del l[idx]
        self.assertEqual([item for item in l], ['nuggie', 'i1', 'i3'])

    def test_set_random_and_pop(self):
        s = self.db.Set('s_obj')
        s.add('charlie', 'mickey')
        self.assertTrue(s.random() in ['charlie', 'mickey'])
        self.assertTrue(s.pop() in ['charlie', 'mickey'])

    def test_zset_iter(self):
        zs = self.db.ZSet('z_obj').clear()
        zs.add('zaizee', 3, 'mickey', 6, 'charlie', 31, 'huey', 3, 'nuggie', 0)

        items = [item for item in zs]
        self.assertEqual(
            items,
            ['nuggie', 'huey', 'zaizee', 'mickey', 'charlie'])

        self.assertEqual(zs.search('*ie'), ['nuggie', 'charlie'])
        self.assertEqual(zs.search('*u*'), ['nuggie', 'huey'])


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
