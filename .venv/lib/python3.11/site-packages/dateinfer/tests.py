import unittest
from dateinfer.date_elements import *
from dateinfer.infer import infer, _mode, _most_restrictive, _tag_most_likely, _percent_match, _tokenize_by_character_class
import dateinfer.ruleproc as ruleproc
import yaml


def load_tests(loader, standard_tests, ignored):
    """
    Return a TestSuite containing standard_tests plus generated test cases
    """
    suite = unittest.TestSuite()
    suite.addTests(standard_tests)

    with open('examples.yaml', 'r') as f:
        examples = yaml.safe_load_all(f)
        for example in examples:
            suite.addTest(test_case_for_example(example))

    return suite


def test_case_for_example(test_data):
    """
    Return an instance of TestCase containing a test for a date-format example
    """

    # This class definition placed inside method to prevent discovery by test loader
    class TestExampleDate(unittest.TestCase):
        def testFormat(self):
            # verify initial conditions
            self.assertTrue(hasattr(self, 'test_data'), 'testdata field not set on test object')

            expected = self.test_data['format']
            actual = infer(self.test_data['examples'])

            self.assertEqual(expected,
                             actual,
                             '{0}: Inferred `{1}`!=`{2}`'.format(self.test_data['name'], actual, expected))

    test_case = TestExampleDate(methodName='testFormat')
    test_case.test_data = test_data
    return test_case


class TestAmbiguousDateCases(unittest.TestCase):
    """
    TestCase for tests which results are ambiguous but can be assumed to fall in a small set of possibilities.
    """
    def testAmbg1(self):
        self.assertIn(infer(['1/1/2012']), ['%m/%d/%Y', '%d/%m/%Y'])

    def testAmbg2(self):
        # Note: as described in Issue #5 (https://github.com/jeffreystarr/dateinfer/issues/5), the result
        # should be %d/%m/%Y as the more likely choice. However, at this point, we will allow %m/%d/%Y.
        self.assertIn(infer(['04/12/2012', '05/12/2012', '06/12/2012', '07/12/2012']),
                      ['%d/%m/%Y', '%m/%d/%Y'])


class TestMode(unittest.TestCase):
    def testMode(self):
        self.assertEqual(5, _mode([1, 3, 4, 5, 6, 5, 2, 5, 3]))
        self.assertEqual(2, _mode([1, 2, 2, 3, 3]))  # with ties, pick least value


class TestMostRestrictive(unittest.TestCase):
    def testMostRestrictive(self):
        t = _most_restrictive

        self.assertEqual(MonthNum(), t([DayOfMonth(), MonthNum, Year4()]))
        self.assertEqual(Year2(), t([Year4(), Year2()]))


class TestPercentMatch(unittest.TestCase):
    def testPercentMatch(self):
        t = _percent_match
        patterns = (DayOfMonth, MonthNum, Filler)
        examples = ['1', '2', '24', 'b', 'c']

        percentages = t(patterns, examples)

        self.assertAlmostEqual(percentages[0], 0.6)  # DayOfMonth 1..31
        self.assertAlmostEqual(percentages[1], 0.4)  # Month 1..12
        self.assertAlmostEqual(percentages[2], 1.0)  # Filler any


class TestRuleElements(unittest.TestCase):
    def testFind(self):
        elem_list = [Filler(' '), DayOfMonth(), Filler('/'), MonthNum(), Hour24(), Year4()]
        t = ruleproc.Sequence.find

        self.assertEqual(0, t([Filler(' ')], elem_list))
        self.assertEqual(3, t([MonthNum], elem_list))
        self.assertEqual(2, t([Filler('/'), MonthNum()], elem_list))
        self.assertEqual(4, t([Hour24, Year4()], elem_list))

        elem_list = [WeekdayShort, MonthTextShort, Filler(' '), Hour24, Filler(':'), Minute, Filler(':'), Second,
                     Filler(' '), Timezone, Filler(' '), Year4]
        self.assertEqual(3, t([Hour24, Filler(':')], elem_list))

    def testMatch(self):
        t = ruleproc.Sequence.match

        self.assertTrue(t(Hour12, Hour12))
        self.assertTrue(t(Hour12(), Hour12))
        self.assertTrue(t(Hour12, Hour12()))
        self.assertTrue(t(Hour12(), Hour12()))
        self.assertFalse(t(Hour12, Hour24))
        self.assertFalse(t(Hour12(), Hour24))
        self.assertFalse(t(Hour12, Hour24()))
        self.assertFalse(t(Hour12(), Hour24()))

    def testNext(self):
        elem_list = [Filler(' '), DayOfMonth(), Filler('/'), MonthNum(), Hour24(), Year4()]

        next1 = ruleproc.Next(DayOfMonth, MonthNum)
        self.assertTrue(next1.is_true(elem_list))

        next2 = ruleproc.Next(MonthNum, Hour24)
        self.assertTrue(next2.is_true(elem_list))

        next3 = ruleproc.Next(Filler, Year4)
        self.assertFalse(next3.is_true(elem_list))


class TestTagMostLikely(unittest.TestCase):
    def testTagMostLikely(self):
        examples = ['8/12/2004', '8/14/2004', '8/16/2004', '8/25/2004']
        t = _tag_most_likely

        actual = t(examples)
        expected = [MonthNum(), Filler('/'), DayOfMonth(), Filler('/'), Year4()]

        self.assertListEqual(actual, expected)


class TestTokenizeByCharacterClass(unittest.TestCase):
    def testTokenize(self):
        t = _tokenize_by_character_class

        self.assertListEqual([], t(''))
        self.assertListEqual(['2013', '-', '08', '-', '14'], t('2013-08-14'))
        self.assertListEqual(['Sat', ' ', 'Jan', ' ', '11', ' ', '19', ':', '54', ':', '52', ' ', 'MST', ' ', '2014'],
                             t('Sat Jan 11 19:54:52 MST 2014'))
        self.assertListEqual(['4', '/', '30', '/', '1998', ' ', '4', ':', '52', ' ', 'am'], t('4/30/1998 4:52 am'))
