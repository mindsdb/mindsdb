import sys
import unittest

from ledis import Ledis
from ledis.client import Token

from walrus import *
from walrus.containers import chainable_method
from walrus.tusks.helpers import TestHelper


class Scannable(object):
    def _scan(self, cmd, match=None, count=None, ordering=None, limit=None):
        parts = [self.key, '']
        if match:
            parts.extend([Token('MATCH'), match])
        if count:
            parts.extend([Token('COUNT'), count])
        if ordering:
            parts.append(Token(ordering.upper()))
        return self._execute_scan(self.database, cmd, parts, limit)

    def _execute_scan(self, database, cmd, parts, limit=None):
        idx = 0
        while True:
            cursor, rows = database.execute_command(cmd, *parts)
            for row in rows:
                idx += 1
                if limit and idx > limit:
                    cursor = None
                    break
                yield row
            if cursor:
                parts[1] = cursor
            else:
                break


class Sortable(object):
    def _sort(self, cmd, pattern=None, limit=None, offset=None,
              get_pattern=None, ordering=None, alpha=True, store=None):
        parts = [self.key]
        def add_kw(kw, param):
            if param is not None:
                parts.extend([Token(kw), param])
        add_kw('BY', pattern)
        if limit or offset:
            offset = offset or 0
            limit = limit or 'Inf'
            parts.extend([Token('LIMIT'), offset, limit])
        add_kw('GET', get_pattern)
        if ordering:
            parts.append(Token(ordering))
        if alpha:
            parts.append(Token('ALPHA'))
        add_kw('STORE', store)
        return self.database.execute_command(cmd, *parts)


class LedisHash(Scannable, Hash):
    @chainable_method
    def clear(self):
        self.database.hclear(self.key)

    @chainable_method
    def expire(self, ttl=None):
        if ttl is not None:
            self.database.hexpire(self.key, ttl)
        else:
            self.database.hpersist(self.key)

    def __iter__(self):
        return self._scan('XHSCAN')

    def scan(self, match=None, count=None, ordering=None, limit=None):
        if limit is not None:
            limit *= 2  # Hashes yield 2 values.
        return self._scan('XHSCAN', match, count, ordering, limit)


class LedisList(Sortable, List):
    @chainable_method
    def clear(self):
        self.database.lclear(self.key)

    def __setitem__(self, idx, value):
        raise TypeError('Ledis does not support setting values by index.')

    @chainable_method
    def expire(self, ttl=None):
        if ttl is not None:
            self.database.lexpire(self.key, ttl)
        else:
            self.database.lpersist(self.key)

    def sort(self, *args, **kwargs):
        return self._sort('XLSORT', *args, **kwargs)


class LedisSet(Scannable, Sortable, Set):
    @chainable_method
    def clear(self):
        self.database.sclear(self.key)

    @chainable_method
    def expire(self, ttl=None):
        if ttl is not None:
            self.database.sexpire(self.key, ttl)
        else:
            self.database.spersist(self.key)

    def __iter__(self):
        return self._scan('XSSCAN')

    def scan(self, match=None, count=None, ordering=None, limit=None):
        return self._scan('XSSCAN', match, count, ordering, limit)

    def sort(self, *args, **kwargs):
        return self._sort('XSSORT', *args, **kwargs)


class LedisZSet(Scannable, Sortable, ZSet):
    @chainable_method
    def clear(self):
        self.database.zclear(self.key)

    @chainable_method
    def expire(self, ttl=None):
        if ttl is not None:
            self.database.zexpire(self.key, ttl)
        else:
            self.database.zpersist(self.key)

    def __iter__(self):
        return self._scan('XZSCAN')

    def scan(self, match=None, count=None, ordering=None, limit=None):
        if limit:
            limit *= 2
        return self._scan('XZSCAN', match, count, ordering, limit)

    def sort(self, *args, **kwargs):
        return self._sort('XZSORT', *args, **kwargs)


class LedisBitSet(Container):
    def clear(self):
        self.database.delete(self.key)

    def __getitem__(self, idx):
        return self.database.execute_command('GETBIT', self.key, idx)

    def __setitem__(self, idx, value):
        return self.database.execute_command('SETBIT', self.key, idx, value)

    def pos(self, bit, start=None, end=None):
        pieces = ['BITPOS', self.key, bit]
        if start or end:
            pieces.append(start or 0)
        if end:
            pieces.append(end)
        return self.database.execute_command(*pieces)

    def __iand__(self, other):
        self.database.execute_command(
            'BITOP',
            'AND',
            self.key,
            self.key,
            other.key)
        return self

    def __ior__(self, other):
        self.database.execute_command(
            'BITOP',
            'OR',
            self.key,
            self.key,
            other.key)
        return self

    def __ixor__(self, other):
        self.database.execute_command(
            'BITOP',
            'XOR',
            self.key,
            self.key,
            other.key)
        return self

    def __str__(self):
        return self.database[self.key]

    __unicode__ = __str__


class WalrusLedis(Ledis, Scannable, Walrus):
    def __init__(self, *args, **kwargs):
        super(WalrusLedis, self).__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        self.set(key, value)

    def setex(self, name, value, time):
        return super(WalrusLedis, self).setex(name, time, value)

    def zadd(self, key, *args, **kwargs):
        if not isinstance(args[0], (int, float)):
            reordered = []
            for idx in range(0, len(args), 2):
                reordered.append(args[idx + 1])
                reordered.append(args[idx])
        else:
            reordered = args
        return super(WalrusLedis, self).zadd(key, *reordered, **kwargs)

    def hash_exists(self, key):
        return self.execute_command('HKEYEXISTS', key)

    def __iter__(self):
        return self.scan()

    def scan(self, *args, **kwargs):
        return self._scan('XSCAN', *args, **kwargs)

    def _scan(self, cmd, match=None, count=None, ordering=None, limit=None):
        parts = ['KV', '']
        if match:
            parts.extend([Token('MATCH'), match])
        if count:
            parts.extend([Token('COUNT'), count])
        if ordering:
            parts.append(Token(ordering.upper()))
        return self._execute_scan(self, cmd, parts, limit)

    def update(self, values):
        return self.mset(values)

    def BitSet(self, key):
        return LedisBitSet(self, key)

    def Hash(self, key):
        return LedisHash(self, key)

    def List(self, key):
        return LedisList(self, key)

    def Set(self, key):
        return LedisSet(self, key)

    def ZSet(self, key):
        return LedisZSet(self, key)


class TestWalrusLedis(TestHelper, unittest.TestCase):
    def setUp(self):
        self.db = WalrusLedis()
        self.db.flushall()

    def test_scan(self):
        values = {
            'k1': 'v1',
            'k2': 'v2',
            'k3': 'v3',
            'charlie': 31,
            'mickey': 7,
            'huey': 5}
        self.db.update(values)
        results = self.db.scan()
        expected = ['charlie', 'huey', 'k1', 'k2', 'k3', 'mickey']
        self.assertEqual(list(results), expected)
        self.assertEqual([item for item in self.db], expected)

    def test_hash_iter(self):
        h = self.db.Hash('h_obj')
        h.clear()
        h.update({'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})

        items = [item for item in h]
        self.assertEqual(items, ['k1', 'v1', 'k2', 'v2', 'k3', 'v3'])
        items = [item for item in h.scan(limit=2)]
        self.assertEqual(items, ['k1', 'v1', 'k2', 'v2'])

    def test_no_setitem_list(self):
        l = self.db.List('l_obj').clear()
        l.append('foo')
        self.assertRaises(TypeError, lambda: l.__setitem__(0, 'xx'))

    def test_set_iter(self):
        s = self.db.Set('s_obj').clear()
        s.add('charlie', 'huey', 'mickey')

        items = [item for item in s]
        self.assertEqual(sorted(items), ['charlie', 'huey', 'mickey'])
        items = [item for item in s.scan(limit=2, ordering='DESC')]
        self.assertEqual(items, ['mickey', 'huey'])

    def test_zset_iter(self):
        zs = self.db.ZSet('z_obj').clear()
        zs.add('zaizee', 3, 'mickey', 6, 'charlie', 31, 'huey', 3, 'nuggie', 0)

        items = [item for item in zs]
        self.assertEqual(items, [
            'charlie', '31',
            'huey', '3',
            'mickey', '6',
            'nuggie', '0',
            'zaizee', '3',
        ])

        items = [item for item in zs.scan(limit=3, ordering='DESC')]
        self.assertEqual(items, [
            'zaizee', '3',
            'nuggie', '0',
            'mickey', '6',
        ])

    def test_bit_set(self):
        b = self.db.BitSet('bitset_obj')
        b.clear()
        b[0] = 1
        b[1] = 1
        b[2] = 0
        b[3] = 1
        self.assertEqual(self.db[b.key], '\xd0')

        b[4] = 1
        self.assertEqual(self.db[b.key], '\xd8')
        self.assertEqual(b[0], 1)
        self.assertEqual(b[2], 0)

        self.db['b1'] = 'foobar'
        self.db['b2'] = 'abcdef'
        b = self.db.BitSet('b1')
        b2 = self.db.BitSet('b2')
        b &= b2
        self.assertEqual(self.db[b.key], '`bc`ab')
        self.assertEqual(str(b), '`bc`ab')

        self.db['b1'] = '\x00\xff\xf0'
        self.assertEqual(b.pos(1, 0), 8)
        self.assertEqual(b.pos(1, 2), 16)

        self.db['b1'] = '\x00\x00\x00'
        self.assertEqual(b.pos(1), -1)

    def test_sorting(self):
        items = ['charlie', 'zaizee', 'mickey', 'huey']
        sorted_items = sorted(items)

        l = self.db.List('l_obj').clear()
        l.extend(items)
        results = l.sort()
        self.assertEqual(results, sorted_items)

        dest = self.db.List('l_dest')
        l.sort(ordering='DESC', limit=3, store=dest.key)
        results = list(dest)
        self.assertEqual(results, ['zaizee', 'mickey', 'huey'])

        s = self.db.Set('s_obj').clear()
        s.add(*items)
        results = s.sort()
        self.assertEqual(results, sorted_items)

        results = s.sort(ordering='DESC', limit=3)
        self.assertEqual(results, ['zaizee', 'mickey', 'huey'])

        z = self.db.ZSet('z_obj').clear()
        z.add('charlie', 10, 'zaizee', 10, 'mickey', 3, 'huey', 4)
        results = z.sort()
        self.assertEqual(results, sorted_items)

        results = z.sort(ordering='DESC', limit=3)
        self.assertEqual(results, ['zaizee', 'mickey', 'huey'])


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
