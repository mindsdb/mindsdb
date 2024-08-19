from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db


class TestGraph(WalrusTestCase):
    def setUp(self):
        super(TestGraph, self).setUp()
        # Limit to 5 events per second.
        self.g = db.graph('test-graph')

    def create_graph_data(self):
        data = (
            ('charlie', 'likes', 'huey'),
            ('charlie', 'likes', 'mickey'),
            ('charlie', 'likes', 'zaizee'),
            ('charlie', 'is', 'human'),
            ('connor', 'likes', 'huey'),
            ('connor', 'likes', 'mickey'),
            ('huey', 'eats', 'catfood'),
            ('huey', 'is', 'cat'),
            ('mickey', 'eats', 'anything'),
            ('mickey', 'is', 'dog'),
            ('zaizee', 'eats', 'catfood'),
            ('zaizee', 'is', 'cat'),
        )
        self.g.store_many(data)

    def create_friends(self):
        data = (
            ('charlie', 'friend', 'huey'),
            ('huey', 'friend', 'charlie'),
            ('huey', 'friend', 'mickey'),
            ('zaizee', 'friend', 'charlie'),
            ('zaizee', 'friend', 'mickey'),
            ('mickey', 'friend', 'nuggie'),
        )
        for item in data:
            self.g.store(*item)

    def test_search_extended(self):
        self.create_graph_data()
        X = self.g.v.x
        Y = self.g.v.y
        Z = self.g.v.z
        result = self.g.search(
            (X, 'likes', Y),
            (Y, 'is', 'cat'),
            (Z, 'likes', Y))
        self.assertEqual(result['x'], set(['charlie', 'connor']))
        self.assertEqual(result['y'], set(['huey', 'zaizee']))
        self.assertEqual(result['z'], set(['charlie', 'connor']))

        self.g.store_many((
            ('charlie', 'likes', 'connor'),
            ('connor', 'likes', 'charlie'),
            ('connor', 'is', 'baby'),
            ('connor', 'is', 'human'),
            ('nash', 'is', 'baby'),
            ('nash', 'is', 'human'),
            ('connor', 'lives', 'ks'),
            ('nash', 'lives', 'nv'),
            ('charlie', 'lives', 'ks')))

        result = self.g.search(
            ('charlie', 'likes', X),
            (X, 'is', 'baby'),
            (X, 'lives', 'ks'))
        self.assertEqual(result, {'x': set(['connor'])})

        result = self.g.search(
            (X, 'is', 'baby'),
            (X, 'likes', Y),
            (Y, 'lives', 'ks'))
        self.assertEqual(result, {
            'x': set(['connor']),
            'y': set(['charlie']),
        })

    def assertTriples(self, result, expected):
        result = list(result)
        self.assertEqual(len(result), len(expected))
        for i1, i2 in zip(result, expected):
            self.assertEqual(
                (i1['s'], i1['p'], i1['o']), i2)

    def test_query(self):
        self.create_graph_data()
        res = self.g.query()
        self.assertTriples(res, (
            ('charlie', 'is', 'human'),
            ('charlie', 'likes', 'huey'),
            ('charlie', 'likes', 'mickey'),
            ('charlie', 'likes', 'zaizee'),
            ('connor', 'likes', 'huey'),
            ('connor', 'likes', 'mickey'),
            ('huey', 'eats', 'catfood'),
            ('huey', 'is', 'cat'),
            ('mickey', 'eats', 'anything'),
            ('mickey', 'is', 'dog'),
            ('zaizee', 'eats', 'catfood'),
            ('zaizee', 'is', 'cat'),
        ))

        res = self.g.query('charlie', 'likes')
        self.assertTriples(res, (
            ('charlie', 'likes', 'huey'),
            ('charlie', 'likes', 'mickey'),
            ('charlie', 'likes', 'zaizee'),
        ))

        res = self.g.query(p='is', o='cat')
        self.assertTriples(res, (
            ('huey', 'is', 'cat'),
            ('zaizee', 'is', 'cat'),
        ))

        res = self.g.query(s='huey')
        self.assertTriples(res, (
            ('huey', 'eats', 'catfood'),
            ('huey', 'is', 'cat'),
        ))

        res = self.g.query(o='huey')
        self.assertTriples(res, (
            ('charlie', 'likes', 'huey'),
            ('connor', 'likes', 'huey'),
        ))

        res = self.g.query(s='does-not-exist')
        self.assertTriples(res, [])

        res = self.g.query(s='huey', p='is', o='x')
        self.assertTriples(res, [])

    def test_search(self):
        self.create_graph_data()
        X = self.g.v('x')
        result = self.g.search(
            {'s': 'charlie', 'p': 'likes', 'o': X},
            {'s': X, 'p': 'eats', 'o': 'catfood'},
            {'s': X, 'p': 'is', 'o': 'cat'})
        self.assertEqual(result, {'x': set(['huey', 'zaizee'])})

    def test_search_simple(self):
        self.create_friends()
        X = self.g.v('x')
        result = self.g.search({'s': X, 'p': 'friend', 'o': 'charlie'})
        self.assertEqual(result, {'x': set(['huey', 'zaizee'])})

    def test_search_2var(self):
        self.create_friends()
        X = self.g.v('x')
        Y = self.g.v('y')

        result = self.g.search(
            {'s': X, 'p': 'friend', 'o': 'charlie'},
            {'s': Y, 'p': 'friend', 'o': X})
        self.assertEqual(result, {
            'x': set(['huey']),
            'y': set(['charlie']),
        })

        result = self.g.search(
            ('charlie', 'friend', X),
            (X, 'friend', Y),
            (Y, 'friend', 'nuggie'))
        self.assertEqual(result, {
            'x': set(['huey']),
            'y': set(['mickey']),
        })

        result = self.g.search(
            ('huey', 'friend', X),
            (X, 'friend', Y))
        self.assertEqual(result['y'], set(['huey', 'nuggie']))

    def test_search_mutual(self):
        self.create_friends()
        X = self.g.v('x')
        Y = self.g.v('y')

        result = self.g.search(
            {'s': X, 'p': 'friend', 'o': Y},
            {'s': Y, 'p': 'friend', 'o': X})
        self.assertEqual(result['y'], set(['charlie', 'huey']))
