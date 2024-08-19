import random

from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db


class TestAutocomplete(WalrusTestCase):
    test_data = (
        (1, 'testing python'),
        (2, 'testing python code'),
        (3, 'web testing python code'),
        (4, 'unit tests with python'))

    def setUp(self):
        super(TestAutocomplete, self).setUp()
        self.ac = db.autocomplete()

    def store_test_data(self, id_to_store=None):
        for obj_id, title in self.test_data:
            if id_to_store is None or obj_id == id_to_store:
                self.ac.store(obj_id, title, {
                    'obj_id': obj_id,
                    'title': title,
                    'value': obj_id % 2 == 0 and 'even' or 'odd'})

    def sort_results(self, results):
        return sorted(results, key=lambda item: item['obj_id'])

    def assertResults(self, results, expected):
        self.assertEqual([result['obj_id'] for result in results], expected)

    def test_search(self):
        self.store_test_data()

        results = self.ac.search('testing python')
        self.assertList(results, [
            {'obj_id': 1, 'title': 'testing python', 'value': 'odd'},
            {'obj_id': 2, 'title': 'testing python code', 'value': 'even'},
            {'obj_id': 3, 'title': 'web testing python code', 'value': 'odd'},
        ])

        results = self.ac.search('test')
        self.assertResults(results, [1, 2, 4, 3])

        results = self.ac.search('uni')
        self.assertResults(results, [4])

        self.assertList(self.ac.search(''), [])
        self.assertList(self.ac.search('missing'), [])
        self.assertList(self.ac.search('with'), [])

    def test_boosting(self):
        letters = ('alpha', 'beta', 'gamma', 'delta')
        n = len(letters)
        test_data = []
        for i in range(n * 3):
            obj_id = i + 1
            obj_type = 't%d' % ((i / n) + 1)
            title = 'test %s' % letters[i % n]
            self.ac.store(
                obj_id,
                title,
                {'obj_id': obj_id, 'title': title},
                obj_type)

        def assertBoosts(query, boosts, expected):
            results = self.ac.search(query, boosts=boosts)
            self.assertResults(results, expected)

        assertBoosts('alp', None, [1, 5, 9])
        assertBoosts('alp', {'t2': 1.1}, [5, 1, 9])
        assertBoosts('test', {'t3': 1.5, 't2': 1.1}, [
            9, 10, 12, 11, 5, 6, 8, 7, 1, 2, 4, 3])
        assertBoosts('alp', {'t1': 0.5}, [5, 9, 1])
        assertBoosts('alp', {'t1': 1.5, 't3': 1.6}, [9, 1, 5])
        assertBoosts('alp', {'t3': 1.5, '5': 1.6}, [5, 9, 1])

    def test_stored_boosts(self):
        id_to_type = {
            'aaa': 1,
            'aab': 2,
            'aac': 3,
            'aaab': 4,
            'bbbb': 4}
        for obj_id, obj_type in id_to_type.items():
            self.ac.store(obj_id, obj_type=obj_type)

        self.assertList(self.ac.search('aa'), ['aaa', 'aaab', 'aab', 'aac'])

        self.ac.boost_object(obj_type=2, multiplier=2)
        self.assertList(self.ac.search('aa'), ['aab', 'aaa', 'aaab', 'aac'])

        self.ac.boost_object('aac', multiplier=3)
        self.assertList(self.ac.search('aa'), ['aac', 'aab', 'aaa', 'aaab'])

        results = self.ac.search('aa', boosts={'aac': 1.5})
        self.assertList(results, ['aab', 'aac', 'aaa', 'aaab'])

    def test_limit(self):
        self.store_test_data()
        results = self.ac.search('testing', limit=1)
        self.assertResults(results, [1])

        results = self.ac.search('testing', limit=2)
        self.assertResults(results, [1, 2])

    def test_search_empty(self):
        self.assertList(self.ac.search(''), [])

    def test_chunked(self):
        for i in range(25):
            self.ac.store('foo %s' % (chr(i + ord('a')) * 2))

        ge = self.ac.search('foo', limit=21, chunk_size=5)
        results = list(ge)
        self.assertEqual(len(results), 21)
        self.assertEqual(results[0], 'foo aa')
        self.assertEqual(results[-1], 'foo uu')

    def test_scoring_proximity_to_front(self):
        self.ac.store('aa bb cc')
        self.ac.store('tt cc')

        self.assertList(self.ac.search('cc'), ['tt cc', 'aa bb cc'])

        self.ac.store('aa b cc')
        self.assertList(self.ac.search('cc'), ['tt cc', 'aa b cc', 'aa bb cc'])

    def test_simple(self):
        for _, title in self.test_data:
            self.ac.store(title)

        self.assertList(self.ac.search('testing'), [
            'testing python',
            'testing python code',
            'web testing python code'])
        self.assertList(self.ac.search('code'), [
            'testing python code',
            'web testing python code'])

        self.ac.store('z python code')
        self.assertList(self.ac.search('cod'), [
            'testing python code',
            'z python code',
            'web testing python code'])

    def test_sorting(self):
        strings = []
        for i in range(26):
            strings.append('aaaa%s' % chr(i + ord('a')))
            if i > 0:
                strings.append('aaa%sa' % chr(i + ord('a')))

        random.shuffle(strings)
        for s in strings:
            self.ac.store(s)

        self.assertList(self.ac.search('aaa'), sorted(strings))
        self.assertList(self.ac.search('aaa', limit=30), sorted(strings)[:30])

    def test_removing_objects(self):
        self.store_test_data()
        self.ac.remove(1)

        self.assertResults(self.ac.search('testing'), [2, 3])

        # Restore item 1 and remove item 2.
        self.store_test_data(1)
        self.ac.remove(2)

        self.assertResults(self.ac.search('testing'), [1, 3])

        # Item with obj_id=2 has already been removed.
        with self.assertRaises(KeyError):
            self.ac.remove(2)

    def test_tokenize_title(self):
        self.assertEqual(
            self.ac.tokenize_title('abc def ghi'),
            ['abc', 'def', 'ghi'])

        # Stop-words are removed automatically.
        self.assertEqual(self.ac.tokenize_title('a A tHe an a'), [])

        # Empty string yields an empty list.
        self.assertEqual(self.ac.tokenize_title(''), [])

        # Stop-words, punctuation, capitalization, etc.
        self.assertEqual(self.ac.tokenize_title(
            'The Best of times, the blurst of times'),
            ['times', 'blurst', 'times'])

    def test_exists(self):
        self.assertFalse(self.ac.exists('test'))
        self.ac.store('test')
        self.assertTrue(self.ac.exists('test'))

    def test_key_leaks(self):
        initial_key_count = len(db.keys())

        # Store a single item.
        self.store_test_data(1)

        # See how many keys we have in the db - check again in a bit.
        key_len = len(db.keys())

        # Store a second item.
        self.store_test_data(2)
        key_len2 = len(db.keys())

        self.assertTrue(key_len != key_len2)
        self.ac.remove(2)

        # Back to the original amount of keys we had after one item.
        self.assertEqual(len(db.keys()), key_len)

        # Remove the first item, back to original count at start.
        self.ac.remove(1)
        self.assertEqual(len(db.keys()), initial_key_count)

    def test_updating(self):
        # store(obj_id, title=None, data=None, obj_type=None).
        self.ac.store('id1', 'title baze', 'd1', 't1')
        self.ac.store('id2', 'title nugget', 'd2', 't2')
        self.ac.store('id3', 'title foo', 'd3', 't3')

        self.assertList(self.ac.search('tit'), ['d1', 'd3', 'd2'])

        # Overwrite the data for id1.
        self.ac.store('id1', 'title foo', 'D1', 't1')
        self.assertList(self.ac.search('tit'), ['D1', 'd3', 'd2'])

        # Overwrite the data with a new title, will remove the title one refs.
        self.ac.store('id1', 'Herple', 'done', 't1')
        self.assertList(self.ac.search('tit'), ['d3', 'd2'])
        self.assertList(self.ac.search('herp'), ['done'])

        # Overwrite again, capitalizing the data and changing the title.
        self.ac.store('id1', 'title baze', 'Done', 't1')
        self.assertList(self.ac.search('tit'), ['Done', 'd3', 'd2'])

        # Verify that we clean up any crap when updating.
        self.assertList(self.ac.search('herp'), [])

    def test_word_position_ordering(self):
        self.ac.store('aaaa bbbb')
        self.ac.store('bbbb cccc')
        self.ac.store('bbbb aaaa')
        self.ac.store('aaaa bbbb')

        results = self.ac.search('bb')
        self.assertList(results, ['bbbb aaaa', 'bbbb cccc', 'aaaa bbbb'])
        self.assertList(self.ac.search('aa'), ['aaaa bbbb', 'bbbb aaaa'])

        self.ac.store('aabb bbbb')
        self.assertList(self.ac.search('bb'), [
            'bbbb aaaa',
            'bbbb cccc',
            'aaaa bbbb',
            'aabb bbbb'])
        self.assertList(self.ac.search('aa'), [
            'aaaa bbbb',
            'aabb bbbb',
            'bbbb aaaa'])

        # Verify issue 9 is fixed.
        self.ac.store('foo one')
        self.ac.store('bar foo one')
        self.assertList(self.ac.search('foo'), ['foo one', 'bar foo one'])

    def test_return_all_results(self):
        phrases = ('aa bb', 'aa cc', 'bb aa cc', 'bb cc', 'cc aa bb')
        for phrase in phrases:
            self.ac.store(phrase)

        self.assertList(sorted(self.ac.list_data()), list(phrases))
        self.assertEqual(sorted(self.ac.list_titles()), list(phrases))

    def test_multiword_phrases(self):
        self.ac.store('p1', 'alpha beta gamma delta')
        self.ac.store('p2', 'beta delta zeta')
        self.ac.store('p3', 'gamma zeta iota')

        self.assertList(self.ac.search('ga del'), ['alpha beta gamma delta'])
        self.assertList(self.ac.search('be de'), [
            'beta delta zeta',
            'alpha beta gamma delta'])
        self.assertList(self.ac.search('de be'), [
            'beta delta zeta',
            'alpha beta gamma delta'])
        self.assertList(self.ac.search('bet delt'), [
            'beta delta zeta',
            'alpha beta gamma delta'])
        self.assertList(self.ac.search('delt bet'), [
            'beta delta zeta',
            'alpha beta gamma delta'])

        self.assertList(self.ac.search('delt bet alpha'),
                        ['alpha beta gamma delta'])

    def test_multiword_stopword_handling(self):
        self.ac.store('p1', 'alpha beta delta')
        self.ac.store('p2', 'alpha delta gamma')
        self.ac.store('p3', 'beta gamma')

        self.assertList(self.ac.search('a'), [
            'alpha beta delta',
            'alpha delta gamma'])
        self.assertList(self.ac.search('be'), [
            'beta gamma',
            'alpha beta delta'])
        # Here since "a" is a stopword and is not the last token, we strip it.
        self.assertList(self.ac.search('a be'), [
            'beta gamma',
            'alpha beta delta'])
        # a & be are stripped, since they are stopwords and not last token.
        self.assertList(self.ac.search('a be de'), [
            'alpha delta gamma',
            'alpha beta delta'])

        self.assertList(self.ac.search('al bet de'), [
            'alpha beta delta'])
