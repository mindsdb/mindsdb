from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db


class TestCounter(WalrusTestCase):
    def test_counter(self):
        counter_a = db.counter('counter-a')
        counter_b = db.counter('counter-b')

        self.assertEqual(counter_a.value(), 0)
        self.assertEqual(counter_a.incr(), 1)
        self.assertEqual(counter_a.incr(3), 4)
        self.assertEqual(counter_a.value(), 4)

        self.assertEqual(counter_b.value(), 0)
        counter_b += 3
        self.assertEqual(counter_b.value(), 3)
        counter_b = counter_b + counter_a
        self.assertEqual(counter_b.value(), 7)
        counter_b = counter_b - 5
        self.assertEqual(counter_b.value(), 2)
