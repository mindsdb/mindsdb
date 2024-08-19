import unittest

from walrus.containers import *
from walrus.containers import _normalize_stream_keys
from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db
from walrus.tests.base import stream_test
from walrus.tests.base import zpop_test
from walrus.utils import decode
from walrus.utils import decode_dict
from walrus.utils import encode


class TestHash(WalrusTestCase):
    def setUp(self):
        super(TestHash, self).setUp()
        self.hsh = db.Hash('my-hash')

    def test_item_api(self):
        self.hsh['k1'] = 'v1'
        self.assertEqual(self.hsh['k1'], b'v1')
        self.assertTrue(self.hsh['kx'] is None)

        self.hsh['k2'] = 'v2'
        self.hsh['k3'] = 'v3'
        self.assertEqual(self.hsh.as_dict(), {
            b'k1': b'v1',
            b'k2': b'v2',
            b'k3': b'v3'})

        del self.hsh['k2']
        self.assertEqual(self.hsh.as_dict(), {b'k1': b'v1', b'k3': b'v3'})

    def test_dict_apis(self):
        self.hsh.update({'k1': 'v1', 'k2': 'v2'})
        self.hsh.update(k3='v3', k4='v4')
        self.assertEqual(sorted(self.hsh.items()), [
            (b'k1', b'v1'),
            (b'k2', b'v2'),
            (b'k3', b'v3'),
            (b'k4', b'v4')])
        self.assertEqual(sorted(self.hsh.keys()), [b'k1', b'k2', b'k3', b'k4'])
        self.assertEqual(sorted(self.hsh.values()),
                         [b'v1', b'v2', b'v3', b'v4'])

        self.assertEqual(len(self.hsh), 4)
        self.assertTrue('k1' in self.hsh)
        self.assertFalse('kx' in self.hsh)

    def test_search_iter(self):
        self.hsh.update(foo='v1', bar='v2', baz='v3')
        self.assertEqual(sorted(self.hsh), [
            (b'bar', b'v2'),
            (b'baz', b'v3'),
            (b'foo', b'v1')])
        self.assertEqual(sorted(self.hsh.search('b*')), [
            (b'bar', b'v2'),
            (b'baz', b'v3')])

    def test_as_dict(self):
        self.hsh.update(k1='v1', k2='v2')
        self.assertEqual(self.hsh.as_dict(True), {'k1': 'v1', 'k2': 'v2'})
        self.assertEqual(db.Hash('test').as_dict(), {})

    def test_from_dict(self):
        data = dict(zip('abcdefghij', 'klmnopqrst'))
        hsh = Hash.from_dict(db, 'test', data)
        self.assertEqual(hsh.as_dict(True), data)

    def test_setnx(self):
        key, value = "key_setnx", "value_setnx"
        self.assertTrue(self.hsh.setnx(key, value))
        self.assertFalse(self.hsh.setnx(key, value))


class TestSet(WalrusTestCase):
    def setUp(self):
        super(TestSet, self).setUp()
        self.set = db.Set('my-set')

    def test_basic_apis(self):
        self.set.add('i1', 'i2', 'i3', 'i2', 'i1')
        self.assertEqual(sorted(self.set), [b'i1', b'i2', b'i3'])

        self.set.remove('i2')
        self.assertEqual(sorted(self.set), [b'i1', b'i3'])

        self.set.remove('ix')
        self.assertEqual(sorted(self.set), [b'i1', b'i3'])

        # Test __contains__
        self.assertTrue('i1' in self.set)
        self.assertFalse('ix' in self.set)

        # Test __iter__.
        self.assertEqual(sorted(self.set), [b'i1', b'i3'])

        del self.set['i3']
        self.assertEqual(sorted(self.set), [b'i1'])

    def test_combining(self):
        self.set2 = db.Set('my-set2')
        self.set.add(1, 2, 3, 4)
        self.set2.add(3, 4, 5, 6)

        self.assertEqual(
            self.set | self.set2,
            set([b'1', b'2', b'3', b'4', b'5', b'6']))
        self.assertEqual(self.set & self.set2, set([b'3', b'4']))
        self.assertEqual(self.set - self.set2, set([b'1', b'2']))
        self.assertEqual(self.set2 - self.set, set([b'5', b'6']))

    def test_combine_store(self):
        self.set2 = db.Set('my-set2')
        self.set.add(1, 2, 3, 4)
        self.set2.add(3, 4, 5, 6)

        s3 = self.set.unionstore('my-set3', self.set2)
        self.assertEqual(s3.members(),
                         set([b'1', b'2', b'3', b'4', b'5', b'6']))

        s3 = self.set.interstore('my-set3', self.set2)
        self.assertEqual(s3.members(), set([b'3', b'4']))

        s3 = self.set.diffstore('my-set3', self.set2)
        self.assertEqual(s3.members(), set([b'1', b'2']))

        self.set |= self.set2
        self.assertEqual(sorted(self.set),
                         [b'1', b'2', b'3', b'4', b'5', b'6'])

        s4 = db.Set('my-set4')
        s4.add('1', '3')
        s3 &= s4
        self.assertEqual(s3.members(), set([b'1']))

    def test_search(self):
        self.set.add('foo', 'bar', 'baz', 'nug')
        self.assertEqual(sorted(self.set.search('b*')), [b'bar', b'baz'])

    def test_sort(self):
        values = ['charlie', 'zaizee', 'mickey', 'huey']
        self.set.add(*values)
        self.assertEqual(self.set.sort(),
                         [b'charlie', b'huey', b'mickey', b'zaizee'])

        self.set.sort(ordering='DESC', limit=3, store='s_dest')
        self.assertList(db.List('s_dest'), [b'zaizee', b'mickey', b'huey'])

    def test_as_set(self):
        self.set.add('foo', 'bar', 'baz')
        self.assertEqual(self.set.as_set(True), set(('foo', 'bar', 'baz')))
        self.assertEqual(db.Set('test').as_set(), set())

    def test_from_set(self):
        data = set('abcdefghij')
        s = Set.from_set(db, 'test', data)
        self.assertEqual(s.as_set(True), data)


class TestZSet(WalrusTestCase):
    def setUp(self):
        super(TestZSet, self).setUp()
        self.zs = db.ZSet('my-zset')

    def assertZSet(self, expected):
        self.assertEqual(list(self.zs), expected)

    def test_basic_apis(self):
        self.zs.add({'i1': 1, 'i2': 2})
        self.assertZSet([(b'i1', 1), (b'i2', 2)])

        self.zs.add({'i0': 0})
        self.zs.add({'i3': 3})
        self.assertZSet([(b'i0', 0), (b'i1', 1), (b'i2', 2), (b'i3', 3)])

        self.zs.remove('i1')
        self.zs.remove_by_score(3)
        self.zs.add({'i2': -2})
        self.zs.add({'i9': 9})
        self.assertZSet([(b'i2', -2.), (b'i0', 0.), (b'i9', 9.)])

        # __len__
        self.assertEqual(len(self.zs), 3)

        # __contains__
        self.assertTrue('i0' in self.zs)
        self.assertFalse('i1' in self.zs)

        self.assertEqual(self.zs.score('i2'), -2)
        self.assertEqual(self.zs.score('ix'), None)

        self.assertEqual(self.zs.rank('i0'), 1)
        self.assertEqual(self.zs.rank('i1'), None)

        self.assertEqual(self.zs.count(0, 10), 2)
        self.assertEqual(self.zs.count(-3, 11), 3)

        self.zs.incr('i2')
        self.zs.incr('i0', -2)
        self.assertZSet([(b'i0', -2.), (b'i2', -1.), (b'i9', 9.)])

        self.assertEqual(self.zs.range_by_score(0, 9), [b'i9'])
        self.assertEqual(self.zs.range_by_score(-3, 0), [b'i0', b'i2'])

        self.assertEqual(self.zs.popmin_compat(), [(b'i0', -2.)])
        self.assertEqual(len(self.zs), 2)
        self.assertEqual(self.zs.popmax_compat(3),
                              [(b'i9', 9.), (b'i2', -1.)])
        self.assertEqual(self.zs.popmin_compat(), [])
        self.assertEqual(self.zs.popmax_compat(), [])
        self.assertEqual(len(self.zs), 0)

    @zpop_test
    def test_popmin_popmax(self):
        for i in range(10):
            self.zs.add({'i%s' % i: i})

        # a list of item/score tuples is returned.
        self.assertEqual(self.zs.popmin(2), [(b'i0', 0.), (b'i1', 1.)])
        self.assertEqual(self.zs.popmax(2), [(b'i9', 9.), (b'i8', 8.)])

        # when called with no args, a list is still returned.
        self.assertEqual(self.zs.popmin(), [(b'i2', 2.)])
        self.assertEqual(self.zs.popmax(), [(b'i7', 7.)])

        # blocking pop returns single item.
        self.assertEqual(self.zs.bpopmin(), (b'i3', 3.))
        self.assertEqual(self.zs.bpopmax(), (b'i6', 6.))

        # blocking-pop with timeout.
        self.assertEqual(self.zs.bpopmin(2), (b'i4', 4.))
        self.assertEqual(self.zs.bpopmax(2), (b'i5', 5.))

        # empty list is returned when zset is empty.
        self.assertEqual(self.zs.popmin(), [])
        self.assertEqual(self.zs.popmax(), [])

    def test_item_apis(self):
        self.zs['i1'] = 1
        self.zs['i0'] = 0
        self.zs['i3'] = 3
        self.zs['i2'] = 2

        self.assertEqual(self.zs[0, False], [b'i0'])
        self.assertEqual(self.zs[0, True], [(b'i0', 0)])
        self.assertEqual(self.zs[2, False], [b'i2'])
        self.assertEqual(self.zs[2, True], [(b'i2', 2)])
        self.assertEqual(self.zs[-1, True], [(b'i3', 3)])
        self.assertEqual(self.zs[9, True], [])

        self.assertEqual(self.zs[0], [b'i0'])
        self.assertEqual(self.zs[2], [b'i2'])
        self.assertEqual(self.zs[9], [])

        del self.zs['i1']
        del self.zs['i3']
        self.zs['i2'] = -2
        self.zs['i9'] = 9
        self.assertZSet([(b'i2', -2.), (b'i0', 0.), (b'i9', 9.)])

    def test_slicing(self):
        self.zs.add({'i1': 1, 'i2': 2, 'i3': 3, 'i0': 0})
        self.assertEqual(self.zs[:1, True], [(b'i0', 0)])
        self.assertEqual(self.zs[1:3, False], [b'i1', b'i2'])
        self.assertEqual(self.zs[1:-1, True], [(b'i1', 1), (b'i2', 2)])

        self.assertEqual(self.zs['i1':, False], [b'i1', b'i2', b'i3'])
        self.assertEqual(self.zs[:'i2', False], [b'i0', b'i1'])
        self.assertEqual(
            self.zs['i0':'i3', True],
            [(b'i0', 0), (b'i1', 1), (b'i2', 2)])
        self.assertRaises(KeyError, self.zs.__getitem__, (slice('i9'), False))
        self.assertEqual(self.zs[99:, False], [])

        del self.zs[:'i2']
        self.assertZSet([(b'i2', 2.), (b'i3', 3.)])
        del self.zs[1:]
        self.assertZSet([(b'i2', 2.)])

    def test_combine_store(self):
        zs2 = db.ZSet('my-zset2')
        self.zs.add({1: 1, 2: 2, 3: 3})
        zs2.add({3: 3, 4: 4, 5: 5})

        zs3 = self.zs.unionstore('my-zset3', zs2)
        self.assertEqual(
            list(zs3),
            [(b'1', 1.), (b'2', 2.), (b'4', 4.), (b'5', 5.), (b'3', 6.)])

        zs3 = self.zs.interstore('my-zset3', zs2)
        self.assertEqual(list(zs3), [(b'3', 6.)])

        self.zs |= zs2
        self.assertZSet([
            (b'1', 1.), (b'2', 2.), (b'4', 4.), (b'5', 5.), (b'3', 6.)])

        zs3 &= zs2
        self.assertEqual(list(zs3), [(b'3', 9.)])

    def test_search(self):
        self.zs.add({'foo': 1, 'bar': 2, 'baz': 1, 'nug': 3})
        self.assertEqual(
            list(self.zs.search('b*')),
            [(b'baz', 1.), (b'bar', 2.)])

    def test_sort(self):
        values = ['charlie', 3, 'zaizee', 2, 'mickey', 6, 'huey', 3]
        self.zs.add(dict(zip(values[::2], values[1::2])))
        self.assertEqual(
            self.zs.sort(),
            [b'charlie', b'huey', b'mickey', b'zaizee'])

        self.zs.sort(ordering='DESC', limit=3, store='z_dest')
        res = db.List('z_dest')
        self.assertEqual(list(res), [b'zaizee', b'mickey', b'huey'])

    def test_as_items(self):
        self.zs.add({'foo': 3, 'bar': 1, 'baz': 2})
        self.assertEqual(self.zs.as_items(True),
                         [('bar', 1.), ('baz', 2.), ('foo', 3.)])
        self.assertEqual(db.ZSet('test').as_items(), [])

    def test_from_dict(self):
        data = dict(zip('abcdefghij', [float(i) for i in range(10)]))
        zs = ZSet.from_dict(db, 'test', data)
        self.assertEqual(zs.as_items(True), sorted(data.items()))


class TestList(WalrusTestCase):
    def setUp(self):
        super(TestList, self).setUp()
        self.lst = db.List('my-list')

    def test_basic_apis(self):
        self.lst.append('i1')
        self.lst.extend(['i2', 'i3'])
        self.lst.prepend('ix')
        self.assertList(self.lst, [b'ix', b'i1', b'i2', b'i3'])

        self.lst.insert('iy', 'i2', 'before')
        self.lst.insert('iz', 'i2', 'after')
        self.assertList(self.lst, [b'ix', b'i1', b'iy', b'i2', b'iz', b'i3'])

        self.assertEqual(self.lst.pop(), b'i3')
        self.assertEqual(self.lst.popleft(), b'ix')
        self.assertEqual(len(self.lst), 4)

    def test_item_apis(self):
        self.lst.append('i0')
        self.assertEqual(self.lst[0], b'i0')

        self.lst.extend(['i1', 'i2'])
        del self.lst['i1']
        self.assertList(self.lst, [b'i0', b'i2'])

        self.lst[1] = 'i2x'
        self.assertList(self.lst, [b'i0', b'i2x'])

        del self.lst[0]
        self.assertList(self.lst, [b'i2x'])

        del self.lst[99]
        self.assertList(self.lst, [b'i2x'])

        del self.lst['ixxx']
        self.assertList(self.lst, [b'i2x'])

    def test_slicing(self):
        self.lst.extend(['i1', 'i2', 'i3', 'i4'])
        self.assertEqual(self.lst[:1], [b'i1'])
        self.assertEqual(self.lst[:2], [b'i1', b'i2'])
        self.assertEqual(self.lst[:-1], [b'i1', b'i2', b'i3'])
        self.assertEqual(self.lst[1:2], [b'i2'])
        self.assertEqual(self.lst[1:], [b'i2', b'i3', b'i4'])

        l = db.List('l1')
        l.extend(range(10))

        # LTRIM, preserve the 1st to last (removes the 0th element).
        del l[1:-1]
        self.assertEqual([int(decode(i)) for i in l],
                         [1, 2, 3, 4, 5, 6, 7, 8, 9])

        # Trim the list so that it contains only the values within the
        # specified range.
        del l[:3]
        self.assertEqual([int(decode(i)) for i in l], [1, 2, 3])

    def test_sort(self):
        values = ['charlie', 'zaizee', 'mickey', 'huey']
        self.lst.extend(values)
        self.assertEqual(self.lst.sort(),
                         [b'charlie', b'huey', b'mickey', b'zaizee'])

        self.lst.sort(ordering='DESC', limit=3, store='l_dest')
        self.assertList(db.List('l_dest'), [b'zaizee', b'mickey', b'huey'])

    def test_as_list(self):
        self.lst.extend(['foo', 'bar'])
        self.assertEqual(self.lst.as_list(True), ['foo', 'bar'])
        self.assertEqual(db.List('test').as_list(), [])

    def test_from_list(self):
        data = list('abcdefghij')
        lst = List.from_list(db, 'test', data)
        self.assertEqual(lst.as_list(True), data)


class TestArray(WalrusTestCase):
    def setUp(self):
        super(TestArray, self).setUp()
        self.arr = db.Array('my-arr')

    def test_basic_apis(self):
        self.arr.append('i1')
        self.arr.append('i2')
        self.arr.append('i3')
        self.arr.append('i4')
        self.assertEqual(len(self.arr), 4)

        # Indexing works. Invalid indices return None.
        self.assertEqual(self.arr[0], b'i1')
        self.assertEqual(self.arr[3], b'i4')
        self.assertTrue(self.arr[4] is None)

        # Negative indexing works and includes bounds-checking.
        self.assertEqual(self.arr[-1], b'i4')
        self.assertEqual(self.arr[-4], b'i1')
        self.assertTrue(self.arr[-5] is None)

        self.assertEqual(self.arr.pop(1), b'i2')
        self.assertList(self.arr, [b'i1', b'i3', b'i4'])

        self.assertEqual(self.arr.pop(), b'i4')
        self.assertList(self.arr, [b'i1', b'i3'])

        self.arr[-1] = 'iy'
        self.arr[0] = 'ix'
        self.assertList(self.arr, [b'ix', b'iy'])

        self.assertTrue('iy' in self.arr)
        self.assertFalse('i1' in self.arr)

        self.arr.extend(['foo', 'bar', 'baz'])
        self.assertList(self.arr, [b'ix', b'iy', b'foo', b'bar', b'baz'])

    def test_as_list(self):
        self.arr.extend(['foo', 'bar'])
        self.assertEqual(self.arr.as_list(True), ['foo', 'bar'])
        self.assertEqual(db.Array('test').as_list(), [])

    def test_from_list(self):
        data = list('abcdefghij')
        arr = Array.from_list(db, 'test', data)
        self.assertEqual(arr.as_list(True), data)


class TestStream(WalrusTestCase):
    def setUp(self):
        super(TestStream, self).setUp()
        db.delete('my-stream')
        db.delete('sa')
        db.delete('sb')

    def _create_test_data(self):
        return (db.xadd('sa', {'k': 'a1'}, b'1'),
                db.xadd('sb', {'k': 'b1'}, b'2'),
                db.xadd('sa', {'k': 'a2'}, b'3'),
                db.xadd('sb', {'k': 'b2'}, b'4'),
                db.xadd('sb', {'k': 'b3'}, b'5'))

    @stream_test
    def test_stream_group_info(self):
        sa = db.Stream('sa')
        ra1 = sa.add({'k': 'a1'})
        ra2 = sa.add({'k': 'a2'})
        ra3 = sa.add({'k': 'a3'})

        sb = db.Stream('sb')
        rb1 = sb.add({'k': 'b1'})

        sa_info = sa.info()
        self.assertEqual(sa_info['groups'], 0)
        self.assertEqual(sa_info['length'], 3)
        self.assertEqual(sa_info['first-entry'][0], ra1)
        self.assertEqual(sa_info['last-entry'][0], ra3)

        sb_info = sb.info()
        self.assertEqual(sb_info['groups'], 0)
        self.assertEqual(sb_info['length'], 1)
        self.assertEqual(sb_info['first-entry'][0], rb1)
        self.assertEqual(sb_info['last-entry'][0], rb1)

        self.assertEqual(sa.groups_info(), [])
        self.assertEqual(sb.groups_info(), [])

        # Create consumer groups.
        cga = db.consumer_group('cga', ['sa'])
        cga.create()
        cgab = db.consumer_group('cgab', ['sa', 'sb'])
        cgab.create()

        self.assertEqual(sa.info()['groups'], 2)
        self.assertEqual(sb.info()['groups'], 1)

        sa_groups = sa.groups_info()
        self.assertEqual(len(sa_groups), 2)
        self.assertEqual(sorted(g['name'] for g in sa_groups),
                         [b'cga', b'cgab'])

        sb_groups = sb.groups_info()
        self.assertEqual(len(sb_groups), 1)
        self.assertEqual(sb_groups[0]['name'], b'cgab')

        # Verify we can get stream info from the consumer group.
        stream_info = cgab.stream_info()
        self.assertEqual(sorted(stream_info), ['sa', 'sb'])

        # Destroy consumer group?
        cgab.destroy()
        self.assertEqual(len(sa.groups_info()), 1)
        self.assertEqual(len(sb.groups_info()), 0)

    @stream_test
    def test_consumer_group_create(self):
        cg = db.consumer_group('cg', ['sa'])
        self.assertEqual(cg.create(), {'sa': True})

        # Creating the consumer group again will report that it was not created
        # for the given key(s).
        self.assertEqual(cg.create(), {'sa': False})

        # We can register the consumer group with another key.
        cg = db.consumer_group('cg', ['sa', 'sb'])
        self.assertEqual(cg.create(), {'sa': False, 'sb': True})

    @stream_test
    def test_consumer_group_stream_creation(self):
        cg = db.consumer_group('cg1', ['stream-a', 'stream-b'])
        self.assertFalse(db.exists('stream-a'))
        self.assertFalse(db.exists('stream-b'))

        cg.create()

        # The streams were created (by adding and then deleting a message).
        self.assertTrue(db.exists('stream-a'))
        self.assertTrue(db.exists('stream-b'))

        # The streams that were automatically created will not have any data.
        self.assertEqual(db.xlen('stream-a'), 0)
        self.assertEqual(db.xlen('stream-b'), 0)

        # If a stream already exists that's OK.
        db.xadd('stream-c', {'data': 'dummy'}, id=b'1')
        cg = db.consumer_group('cg2', ['stream-c', 'stream-d'])
        self.assertTrue(db.exists('stream-c'))
        self.assertEqual(db.type('stream-c'), b'stream')
        self.assertFalse(db.exists('stream-d'))

        cg.create()
        self.assertTrue(db.exists('stream-d'))
        self.assertEqual(db.type('stream-c'), b'stream')
        self.assertEqual(db.type('stream-d'), b'stream')
        self.assertEqual(db.xlen('stream-c'), 1)
        self.assertEqual(db.xlen('stream-d'), 0)

        # If a stream key already exists and is a different type, fail.
        db.lpush('l1', 'item-1')
        db.hset('h1', 'key', 'data')
        db.sadd('s1', 'item-1')
        db.set('k1', 'v1')
        db.zadd('z1', {'item-1': 1.0})
        for key in ('l1', 'h1', 's1', 'k1', 'z1'):
            cg = db.consumer_group('cg-%s' % key, keys=[key])
            self.assertRaises(ValueError, cg.create)

    @stream_test
    def test_consumer_group_streams(self):
        ra1, rb1, ra2, rb2, rb3 = self._create_test_data()
        cg = db.consumer_group('g1', ['sa', 'sb'])

        self.assertEqual(cg.sa[ra1], (ra1, {b'k': b'a1'}))
        self.assertEqual(cg.sb[rb3], (rb3, {b'k': b'b3'}))

        def assertMessages(resp, expected):
            self.assertEqual([mid for mid, _ in resp], expected)

        assertMessages(cg.sa[ra1:], [ra1, ra2])
        assertMessages(cg.sa[:ra1], [ra1])
        assertMessages(cg.sa[ra2:], [ra2])
        assertMessages(cg.sa[:ra2], [ra1, ra2])
        assertMessages(cg.sa[rb3:], [])
        assertMessages(cg.sa[:b'0-1'], [])
        assertMessages(list(cg.sa), [ra1, ra2])

        assertMessages(cg.sb[rb1:], [rb1, rb2, rb3])
        assertMessages(cg.sb[rb1::2], [rb1, rb2])
        assertMessages(cg.sb[:rb1], [rb1])
        assertMessages(cg.sb[rb3:], [rb3])
        assertMessages(cg.sb[:rb3], [rb1, rb2, rb3])
        assertMessages(list(cg.sb), [rb1, rb2, rb3])

        self.assertEqual(len(cg.sa), 2)
        self.assertEqual(len(cg.sb), 3)

        del cg.sa[ra1]
        del cg.sb[rb1, rb3]
        self.assertEqual(len(cg.sa), 1)
        self.assertEqual(len(cg.sb), 1)
        assertMessages(list(cg.sa), [ra2])
        assertMessages(list(cg.sb), [rb2])

    @stream_test
    def test_consumer_group_container(self):
        ra1, rb1, ra2, rb2, rb3 = self._create_test_data()
        cg1 = db.consumer_group('g1', {'sa': '1', 'sb': '0'})
        cg2 = db.consumer_group('g2', {'sb': '2'})

        self.assertEqual(cg1.create(), {'sa': True, 'sb': True})
        self.assertEqual(cg2.create(), {'sb': True})

        self.assertEqual(dict(cg1.read(count=2)), {
            b'sa': [(ra2, {b'k': b'a2'})],
            b'sb': [(rb1, {b'k': b'b1'}), (rb2, {b'k': b'b2'})]})
        self.assertEqual(cg1.sa.read(), [])
        self.assertEqual(cg1.sb.read(), [(rb3, {b'k': b'b3'})])

        self.assertEqual(cg1.sa.ack(ra2), 1)
        self.assertEqual(cg1.sb.ack(rb1, rb3), 2)
        p1, = cg1.sb.pending()
        self.assertEqual(p1['message_id'], rb2)
        self.assertEqual(p1['consumer'], b'g1.c1')

        self.assertEqual(cg2.read(count=1), [
            [b'sb', [(rb2, {b'k': b'b2'})]]])
        self.assertEqual(cg2.sb.read(), [(rb3, {b'k': b'b3'})])

        self.assertEqual(cg1.destroy(), {'sa': 1, 'sb': 1})
        self.assertEqual(cg2.destroy(), {'sb': 1})

    @stream_test
    def test_consumer_group_consumers(self):
        ra1, rb1, ra2, rb2, rb3 = self._create_test_data()
        cg11 = db.consumer_group('g1', {'sa': '0', 'sb': '0'}, consumer='cg11')
        cg11.create()
        cg12 = cg11.consumer('cg12')

        self.assertEqual(dict(cg11.read(count=1)), {
            b'sa': [(ra1, {b'k': b'a1'})],
            b'sb': [(rb1, {b'k': b'b1'})]})

        self.assertEqual(dict(cg12.read(count=1, block=1)), {
            b'sa': [(ra2, {b'k': b'a2'})],
            b'sb': [(rb2, {b'k': b'b2'})]})

        pa1, pa2 = cg11.sa.pending()
        self.assertEqual(pa1['message_id'], ra1)
        self.assertEqual(pa1['consumer'], b'cg11')
        self.assertEqual(pa2['message_id'], ra2)
        self.assertEqual(pa2['consumer'], b'cg12')

        pb1, pb2 = cg11.sb.pending()
        self.assertEqual(pb1['message_id'], rb1)
        self.assertEqual(pb1['consumer'], b'cg11')
        self.assertEqual(pb2['message_id'], rb2)
        self.assertEqual(pb2['consumer'], b'cg12')

    @stream_test
    def test_read_api(self):
        sa = db.Stream('a')
        sb = db.Stream('b')
        sc = db.Stream('c')
        streams = [sa, sb, sc]
        docids = []
        for i in range(20):
            stream = streams[i % 3]
            docids.append(stream.add({'k': 'v%s' % i}, id=i + 1))

        def assertData(ret, idxs, is_multi=False):
            if is_multi:
                ret = dict(ret)
                accum = {}
                for idx in idxs:
                    sname = encode('abc'[idx % 3])
                    accum.setdefault(sname, [])
                    accum[sname].append((
                        docids[idx], {b'k': encode('v%s' % idx)}))
            else:
                accum = []
                for idx in idxs:
                    accum.append((docids[idx], {b'k': encode('v%s' % idx)}))
            self.assertEqual(ret, accum)

        assertData(sa.read(), [0, 3, 6, 9, 12, 15, 18])
        assertData(sc.read(), [2, 5, 8, 11, 14, 17])

        # We can specify a maximum number of records via "count".
        assertData(sa.read(3), [0, 3, 6])
        assertData(sb.read(2), [1, 4])
        assertData(sc.read(4), [2, 5, 8, 11])

        # We get the same values we read earlier.
        assertData(sa.read(2), [0, 3])

        # We can pass a minimum ID and will get newer data -- even if the ID
        # does not exist in the stream. We can also pass an exact ID and unlike
        # the range function, it is not inclusive.
        assertData(sa.read(2, last_id=docids[3]), [6, 9])
        assertData(sa.read(2, last_id=docids[4]), [6, 9])

        # If the last ID exceeds the highest ID (indicating no data), None is
        # returned. This is the same whether or not "count" is specified.
        self.assertEqual(sa.read(last_id=docids[18]), [])
        self.assertEqual(sa.read(2, last_id=docids[18]), [])

        # The count is a maximum, so up-to 2 items are return -- but since only
        # one item in the stream exceeds the given ID, we only get one result.
        assertData(sa.read(2, last_id=docids[17]), [18])

        # If a timeout is set and any stream can return a value, then that
        # value is returned immediately.
        assertData(sa.read(2, block=1, last_id=docids[17]), [18])
        assertData(sb.read(2, block=1, last_id=docids[18]), [19])

        # If no items are available and we timed-out, None is returned.
        self.assertEqual(sc.read(block=1, last_id=docids[19]), [])
        self.assertEqual(sc.read(2, block=1, last_id=docids[19]), [])

        # When multiple keys are given, up-to "count" items per stream
        # are returned.
        normalized = _normalize_stream_keys(['a', 'b', 'c'])
        res = db.xread(normalized, count=2)
        assertData(res, [0, 1, 2, 3, 4, 5], True)

        # Specify max-ids for each stream. The max value in "c" is 17, so
        # nothing will be returned for "c".
        uids = [decode(docid) for docid in docids]
        res = db.xread({'a': uids[15], 'b': uids[16], 'c': uids[17]},
                       count=3)
        assertData(res, [18, 19], True)

        # Now we limit ourselves to being able to pull only a single item from
        # stream "c".
        res = db.xread({'a': uids[18], 'b': uids[19], 'c': uids[16]})
        assertData(res, [17], True)

        # None is returned when no results are present and timeout is None or
        # if we reach the timeout.
        res = db.xread({'a': uids[18], 'b': uids[19], 'c': uids[17]})
        self.assertEqual(res, [])

        res = db.xread({'a': uids[18], 'b': uids[19], 'c': uids[17]},
                       count=1, block=1)
        self.assertEqual(res, [])

    @stream_test
    def test_set_id_stream(self):
        stream = db.Stream('my-stream')
        stream.add({'k': 'v1'}, id='3')
        self.assertTrue(stream.set_id('5'))
        self.assertRaises(Exception, stream.add, {'k': 'v2'}, id='4')
        stream.add({'k': 'v3'}, id='6')
        self.assertEqual(stream.read(), [
            (b'3-0', {b'k': b'v1'}),
            (b'6-0', {b'k': b'v3'})])

    @stream_test
    def test_basic_apis(self):
        stream = db.Stream('my-stream')

        # Item ids will be 1-0, 11-0, ...91-0.
        item_ids = [stream.add({'k': 'v%s' % i}, id='%s1' % i)
                    for i in range(10)]
        self.assertEqual(len(stream), 10)

        # Redis automatically adds the sequence number.
        self.assertEqual(item_ids[:3], [b'1-0', b'11-0', b'21-0'])
        self.assertEqual(item_ids[7:], [b'71-0', b'81-0', b'91-0'])

        def assertData(items, expected):
            self.assertEqual(items, [(item_ids[e], {b'k': encode('v%s' % e)})
                                     for e in expected])

        # The sequence number is optional if it's zero.
        assertData(stream[:'1'], [0])
        assertData(stream[:'1-0'], [0])
        assertData(stream['91':], [9])
        assertData(stream['91-0':], [9])
        assertData(stream['91-1':], [])

        # We can slice up to a value. If the sequence number is omitted it will
        # be treated as zero.
        assertData(stream[:'31'], [0, 1, 2, 3])
        assertData(stream[:'31-0'], [0, 1, 2, 3])
        assertData(stream[:'31-1'], [0, 1, 2, 3])

        # We can slice up from a value as well.
        assertData(stream['71':], [7, 8, 9])
        assertData(stream['71-0':], [7, 8, 9])
        assertData(stream['71-1':], [8, 9])

        # We can also slice between values.
        assertData(stream['21':'41'], [2, 3, 4])
        assertData(stream['21-0':'41'], [2, 3, 4])
        assertData(stream['21':'41-0'], [2, 3, 4])
        assertData(stream['21-1':'41'], [3, 4])
        assertData(stream['21-1':'41-1'], [3, 4])

        # The "step" parameter, the third part of the slice, indicates count.
        assertData(stream['41'::3], [4, 5, 6])
        assertData(stream[:'41':3], [0, 1, 2])
        assertData(stream['81'::3], [8, 9])

        # Test using in-between values. The endpoints of the slice are
        # inclusive.
        assertData(stream[:'5'], [0])
        assertData(stream[:'5-1'], [0])
        assertData(stream[:'25'], [0, 1, 2])
        assertData(stream[:'25-1'], [0, 1, 2])
        assertData(stream['25':'55'], [3, 4, 5])
        assertData(stream['55':'92'], [6, 7, 8, 9])
        assertData(stream['91':'92'], [9])

        # If we go above or below, it returns an empty list.
        assertData(stream['92':], [])
        assertData(stream[:'0'], [])

        # We can also provide a count when indexing in-between.
        assertData(stream['25':'55':2], [3, 4])
        assertData(stream['55':'92':1], [6])

        # Use "del" to remove items by ID. The sequence number will be treated
        # as zero if not provided.
        del stream['21', '41-0', '61']
        del stream['51-1']  # Has no effect since we only have 51-0.
        assertData(stream['5':'65'], [1, 3, 5])
        self.assertEqual(len(stream), 7)

        del stream['21']  # Can delete non-existent items.

        # Cannot add lower than maximum ID.
        self.assertRaises(Exception, stream.add, {'k': 'v2'}, id='90-1')
        self.assertRaises(Exception, stream.add, {'k': 'v2'}, id='91-0')

        # Adding a "1" to the sequence works:
        new_id = stream.add({'k': 'v10'}, id='91-1')
        self.assertEqual(new_id, b'91-1')

        # Length reflects the latest addition.
        self.assertEqual(len(stream), 8)

        # Range starting at 91-0 yields 91-0 and 91-1.
        data = stream['91-0':]
        self.assertEqual(len(data), 2)
        self.assertEqual([obj_id for obj_id, _ in data], [b'91-0', b'91-1'])

        # Remove the two 91-x items.
        del stream['91', '91-1']

        # Sanity check that the data was really remove.
        self.assertEqual(len(stream), 6)
        assertData(stream['61':], [7, 8])

        # Can we add an item with an id lower than 91? We've deleted it so the
        # last value is 81, but this still doesn't work (?).
        for docid in ('90', '91', '91-1'):
            self.assertRaises(Exception, stream.add, {'k': 'v9'}, id='90')

        new_id = stream.add({'k': 'v9'}, id='91-2')
        self.assertEqual(new_id, b'91-2')
        self.assertEqual(stream['91':], [(b'91-2', {b'k': b'v9'})])
        del stream['91-2']

        nremoved = stream.trim(4, approximate=False)
        self.assertEqual(nremoved, 2)
        assertData(stream[:], [3, 5, 7, 8])

        # Trimming again returns 0, no items removed.
        self.assertEqual(stream.trim(4, approximate=False), 0)

        # Verify we can iterate over the stream.
        assertData(list(stream), [3, 5, 7, 8])

        # Verify we can get items by id.
        d5 = stream.get('51-0')
        self.assertEqual(d5, (b'51-0', {b'k': b'v5'}))

        # Nonexistant values return None.
        self.assertTrue(stream.get('61-0') is None)


class TestBitField(WalrusTestCase):
    def setUp(self):
        super(TestBitField, self).setUp()
        self.bf = db.bit_field('bf')

    def test_simple_operations(self):
        resp = (self.bf
                .set('u8', 8, 255)
                .get('u8', 0)
                .get('u4', 8)  # 1111
                .get('u4', 12)  # 1111
                .get('u4', 13)  # 1110
                .execute())
        self.assertEqual(resp, [0, 0, 15, 15, 14])

        resp = (self.bf
                .set('u8', 4, 1)  # 00ff -> 001f (returns old val, 0x0f)
                .get('u16', 0)  # 001f (00011111)
                .set('u16', 0, 0))  # 001f -> 0000
        self.assertEqual(list(resp), [15, 31, 31])

        resp = (self.bf
                .incrby('u8', 8, 254)
                .get('u16', 0))
        self.assertEqual(list(resp), [254, 254])

        # Verify overflow protection works:
        resp = (self.bf
                .incrby('u8', 8, 2, 'FAIL')
                .incrby('u8', 8, 1)
                .incrby('u8', 8, 1)  # Still "FAIL".
                .get('u16', 0))
        self.assertEqual(list(resp), [None, 255, None, 255])

        self.assertEqual(self.bf.get_raw(), b'\x00\xff')

    def test_slicing(self):
        resp = self.bf.set('u8', 0, 166).execute()  # 10100110

        self.assertEqual(self.bf[:8], 166)
        self.assertEqual(self.bf[:4], 10)  # 1010
        self.assertEqual(self.bf[4:8], 6)   # 0110
        self.assertEqual(self.bf[2:6], 9) # 1001
        self.assertEqual(self.bf[6:10], 8) # 10?? -> 1000
        self.assertEqual(self.bf[8:16], 0)  # Undefined, defaults to zero.

        self.assertRaises(ValueError, lambda: self.bf[1])
        self.assertRaises(ValueError, lambda: self.bf[1:])
        self.assertRaises(ValueError, lambda: self.bf[4:1])

        self.bf[:8] = 89  # 01011001
        self.assertEqual(self.bf[:8], 89)
        def overflow():
            self.bf[:8] = 256
        self.assertRaises(ValueError, overflow)
        self.bf[:8] = 255
        self.assertEqual(self.bf[:8], 255)

        del self.bf[2:6]
        self.assertEqual(self.bf[:8], 195)  # 11000011


class TestBloomFilter(WalrusTestCase):
    def setUp(self):
        super(TestBloomFilter, self).setUp()
        self.bf = db.bloom_filter('bf')

    def test_bloom_filter(self):
        data = ('foo', 'bar', 'baz', 'nugget', 'this is a test', 'testing',
                'alpha', 'beta', 'delta', 'gamma')

        # Verify that the bloom-filter does not contain any of our items.
        for item in data:
            self.assertFalse(item in self.bf)

        # Add all the items to the bloom filter.
        for item in data:
            self.bf.add(item)

        # Verify that all of our items are now present.
        for item in data:
            self.assertTrue(item in self.bf)

        # Making some small modifications we can verify that all these other
        # items are not present, however.
        for item in data:
            self.assertFalse(item.upper() in self.bf)
            self.assertFalse(item.title() in self.bf)
