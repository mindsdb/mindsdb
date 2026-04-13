import unittest

from numpy import int64
import pandas as pd

from mindsdb.integrations.libs.llm.utils import get_completed_prompts


class TestLLM(unittest.TestCase):
    def test_get_completed_prompts(self):
        placeholder = "{{text}}"
        prefix = "You are a helpful assistant. Here is the user's input:"
        user_inputs = ["Hi! I would love some help.", "Hello! Are you sentient?", None]

        # send all rows at once
        base_template = prefix + placeholder
        df = pd.DataFrame({"text": user_inputs})
        prompts, empties = get_completed_prompts(base_template, df)

        # should detect a single missing value in the relevant column (last row)
        assert empties.shape == (1,)
        assert empties.dtype in [int, int64]
        assert empties[0] == 2

        # check results
        for i in range(len(empties)):
            # in-fill
            assert prompts[0] == prefix + user_inputs[i]

        # edge case - invalid template
        placeholder = ""
        base_template = prefix + placeholder
        df = pd.DataFrame({"text": user_inputs})
        with self.assertRaises(Exception):
            get_completed_prompts(base_template, df)
