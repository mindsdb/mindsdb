import unittest
import random


class MLflowTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        # OPT model logic
        self.model = lambda x: random.random()

        # OPT store it

        # 1. serve it (mlflow models serve --URI alkjdsfhlakjhdflkjadslfjkhasdf)

        # 2. use it

    def test_1(self):
        pass