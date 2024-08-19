import datetime

from walrus import *


class TestHelper(object):
    def test_simple_string_ops(self):
        self.assertTrue(self.db.set('name', 'charlie'))
        self.assertEqual(self.db.get('name'), 'charlie')
        self.assertIsNone(self.db.get('not-exist'))

        self.assertFalse(self.db.setnx('name', 'huey'))
        self.db.setnx('friend', 'zaizee')
        self.assertEqual(self.db['name'], 'charlie')
        self.assertEqual(self.db['friend'], 'zaizee')

        self.assertTrue(self.db.mset({'k1': 'v1', 'k2': 'v2'}))
        res = self.db.mget('k1', 'k2')
        self.assertEqual(res, ['v1', 'v2'])

        self.db.append('k1', 'xx')
        self.assertEqual(self.db['k1'], 'v1xx')

        del self.db['counter']
        self.assertEqual(self.db.incr('counter'), 1)
        self.assertEqual(self.db.incr('counter', 5), 6)
        self.assertEqual(self.db.decr('counter', 2), 4)

        self.assertEqual(self.db.getrange('name', 3, 5), 'rli')
        self.assertEqual(self.db.getset('k2', 'baze'), 'v2')
        self.assertEqual(self.db['k2'], 'baze')
        self.assertEqual(self.db.strlen('name'), 7)

        self.db['data'] = '\x07'
        self.assertEqual(self.db.bitcount('data'), 3)

        del self.db['name']
        self.assertIsNone(self.db.get('name'))
        self.assertRaises(KeyError, lambda: self.db['name'])

        self.assertFalse('name' in self.db)
        self.assertTrue('k1' in self.db)

    def test_simple_hash(self):
        h = self.db.Hash('hash_obj')
        h.clear()

        h['k1'] = 'v1'
        h.update({'k2': 'v2', 'k3': 'v3'})
        self.assertEqual(h.as_dict(), {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})

        self.assertEqual(h['k2'], 'v2')
        self.assertIsNone(h['k4'])
        self.assertTrue('k2' in h)
        self.assertFalse('k4' in h)

        del h['k2']
        del h['k4']
        self.assertEqual(sorted(h.keys()), ['k1', 'k3'])
        self.assertEqual(sorted(h.values()), ['v1', 'v3'])
        self.assertEqual(len(h), 2)

        self.assertEqual(h['k1', 'k2', 'k3'], ['v1', None, 'v3'])

        self.assertEqual(h.incr('counter'), 1)
        self.assertEqual(h.incr('counter', 3), 4)

    def test_simple_list(self):
        l = self.db.List('list_obj')
        l.clear()

        l.append('charlie')
        l.extend(['mickey', 'huey', 'zaizee'])
        self.assertEqual(l[1], 'mickey')
        self.assertEqual(l[-1], 'zaizee')

        self.assertEqual(l[:], ['charlie', 'mickey', 'huey', 'zaizee'])
        self.assertEqual(l[1:-1], ['mickey', 'huey'])
        self.assertEqual(l[2:], ['huey', 'zaizee'])
        self.assertEqual(len(l), 4)

        l.prepend('nuggie')
        l.popright()
        l.popright()
        self.assertEqual([item for item in l], ['nuggie', 'charlie', 'mickey'])

        self.assertEqual(l.popleft(), 'nuggie')
        self.assertEqual(l.popright(), 'mickey')

        l.clear()
        self.assertEqual(list(l), [])
        self.assertIsNone(l.popleft())

    def test_simple_set(self):
        s = self.db.Set('set_obj')
        s.clear()

        self.assertTrue(s.add('charlie'))
        self.assertFalse(s.add('charlie'))
        s.add('huey', 'mickey')
        self.assertEqual(len(s), 3)
        self.assertTrue('huey' in s)
        self.assertFalse('xx' in s)
        self.assertEqual(s.members(), set(['charlie', 'huey', 'mickey']))

        del s['huey']
        del s['xx']
        self.assertEqual(s.members(), set(['charlie', 'mickey']))

        n1 = self.db.Set('n1')
        n2 = self.db.Set('n2')
        n1.add(*range(5))
        n2.add(*range(3, 7))

        self.assertEqual(n1 - n2, set(['0', '1', '2']))
        self.assertEqual(n2 - n1, set(['5', '6']))
        self.assertEqual(n1 | n2, set(map(str, range(7))))
        self.assertEqual(n1 & n2, set(['3', '4']))

        n1.diffstore('ndiff', n2)
        ndiff = self.db.Set('ndiff')
        self.assertEqual(ndiff.members(), set(['0', '1', '2']))

        n1.interstore('ninter', n2)
        ninter = self.db.Set('ninter')
        self.assertEqual(ninter.members(), set(['3', '4']))

    def test_zset(self):
        zs = self.db.ZSet('zset_obj')
        zs.clear()

        zs.add('charlie', 31, 'huey', 3, 'mickey', 6, 'zaizee', 3, 'nuggie', 0)
        self.assertEqual(zs[1], ['huey'])
        self.assertEqual(zs[1, True], [('huey', 3)])

        self.assertEqual(
            zs[:],
            ['nuggie', 'huey', 'zaizee', 'mickey', 'charlie'])
        self.assertEqual(zs[:2], ['nuggie', 'huey'])
        self.assertEqual(zs[1:3, True], [('huey', 3), ('zaizee', 3)])
        self.assertEqual(zs['huey':'charlie'], ['huey', 'zaizee', 'mickey'])

        self.assertEqual(len(zs), 5)
        self.assertTrue('charlie' in zs)
        self.assertFalse('xx' in zs)

        self.assertEqual(zs.score('charlie'), 31.)
        self.assertIsNone(zs.score('xx'))

        self.assertEqual(zs.rank('mickey'), 3)
        self.assertIsNone(zs.rank('xx'))

        self.assertEqual(zs.count(0, 5), 3)
        self.assertEqual(zs.count(6, 31), 2)
        self.assertEqual(zs.count(6, 30), 1)

        zs.incr('mickey')
        self.assertEqual(zs.score('mickey'), 7.)

        self.assertEqual(zs.range_by_score(0, 5), ['nuggie', 'huey', 'zaizee'])

        zs.remove('nuggie')
        self.assertEqual(zs[:2], ['huey', 'zaizee'])

        del zs['mickey']
        self.assertEqual(zs[:], ['huey', 'zaizee', 'charlie'])
        self.assertEqual(len(zs), 3)

        zs.remove_by_score(2, 4)
        self.assertEqual(zs[:], ['charlie'])

        zs.add('huey', 4, 'zaizee', 3, 'beanie', 8)
        zs.remove_by_rank(2)
        self.assertEqual(zs[:], ['zaizee', 'huey', 'charlie'])

        self.assertRaises(KeyError, lambda: zs['xx':])

        z1 = self.db.ZSet('z1')
        z2 = self.db.ZSet('z2')
        z1.add(1, 1, 2, 2, 3, 3)
        z2.add(3, 3, 4, 4, 5, 5)
        z3 = z1.unionstore('z3', z2)
        self.assertEqual(z3[:], ['1', '2', '4', '5', '3'])

        z3 = z1.interstore('z3', z2)
        self.assertEqual(z3[:], ['3'])

    def test_models(self):
        class User(Model):
            __database__ = self.db
            username = TextField(primary_key=True)
            value = IntegerField(index=True)

        for i, username in enumerate(('charlie', 'huey', 'zaizee', 'mickey')):
            User.create(username=username, value=i)

        charlie = User.load('charlie')
        self.assertEqual(charlie.username, 'charlie')
        self.assertEqual(charlie.value, 0)

        query = User.query(
            (User.username == 'charlie') |
            (User.username == 'huey'))
        users = [user.username for user in query]
        self.assertEqual(sorted(users), ['charlie', 'huey'])

    def test_cache(self):
        cache = self.db.cache(name='test-cache')

        @cache.cached(timeout=10)
        def now(seed=None):
            return datetime.datetime.now()

        dt1 = now()
        self.assertEqual(now(), dt1)
        self.assertNotEqual(now(1), dt1)
        self.assertEqual(now(1), now(1))
