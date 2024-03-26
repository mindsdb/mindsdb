import unittest
from textwrap import dedent, indent

import pandas as pd

from mindsdb.integrations.libs.llm.utils import ft_chat_formatter, ft_code_formatter, ft_cqa_formatter
from mindsdb.integrations.libs.llm.utils import ft_jsonl_validation, ft_chat_format_validation
from mindsdb.integrations.libs.llm.utils import get_completed_prompts


class TestLLM(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        # used in `test_ft_chat_format_validation`
        cls.valid_chats = [
            # u/a pattern
            [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
                {"role": "user", "content": "how are you?"},
                {"role": "assistant", "content": "I'm good, thanks"},
            ],

            # u/a pattern
            [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
                {"role": "user", "content": "how are you?"},
            ],

            # s/u/a pattern
            [
                {"role": "system", "content": "you are a useful assistant."},
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "how are you?"},
            ],

            # s/u/a pattern
            [
                {"role": "system", "content": "you are a useful assistant."},
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "how are you?"},
                {"role": "user", "content": "I'm good, thanks"},
            ],
        ]

        # used in `test_ft_chat_format_validation`
        cls.invalid_chats = [
            # invalid - repeated user
            [
                {"role": "user", "content": "hi"},
                {"role": "user", "content": "hello"},  # this is invalid
                {"role": "assistant", "content": "how are you?"},
                {"role": "user", "content": "I'm good, thanks"},
            ],

            # invalid - repeated assistant
            [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
                {"role": "assistant", "content": "how are you?"},  # this is invalid
                {"role": "user", "content": "I'm good, thanks"},
            ],

            # invalid - incorrect system prompt order
            [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
                {"role": "system", "content": "you are a useful assistant."},  # this is invalid
                {"role": "user", "content": "I'm good, thanks"},
            ],

            # invalid roles
            [
                {"role": "user", "content": "hi"},
                {"role": "invalid", "content": "this is an invalid role"},
            ],

            # invalid content
            [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": None},  # should always be a string
            ],

            # invalid - no assistant in the chat
            [
                {"role": "user", "content": "hi"},
            ],
        ]

    def test_get_completed_prompts(self):
        placeholder = "{{text}}"
        prefix = "You are a helpful assistant. Here is the user's input:"
        user_inputs = ["Hi! I would love some help.", "Hello! Are you sentient?", None]

        # send all rows at once
        base_template = prefix + placeholder
        df = pd.DataFrame({'text': user_inputs})
        prompts, empties = get_completed_prompts(base_template, df)

        # should detect a single missing value in the relevant column (last row)
        assert empties.shape == (1,)
        assert empties.dtype == int
        assert empties[0] == 2

        # check results
        for i in range(len(empties)):
            # in-fill
            assert prompts[0] == prefix + user_inputs[i]

        # edge case - invalid template
        placeholder = ""
        base_template = prefix + placeholder
        df = pd.DataFrame({'text': user_inputs})
        with self.assertRaises(Exception):
            get_completed_prompts(base_template, df)

    def test_ft_chat_format_validation(self):
        for chat in self.valid_chats:
            ft_chat_format_validation(chat)  # if chat is valid, returns `None`

        for chat in self.invalid_chats:
            with self.assertRaises(Exception):
                ft_chat_format_validation(chat)  # all of these should raise an Exception

    def test_ft_chat_formatter(self):
        # 1a. long DF with required columns (`role` and `content`)
        df = pd.DataFrame({
            'role': ['system', 'user', 'assistant', 'user'],
            'content': ['you are a helpful assistant', 'hello', 'hi, how can I help?', "I'm good, thanks"],
        })
        chats = ft_chat_formatter(df)
        assert list(chats[0].keys()) == ['messages']
        ft_chat_format_validation(chats[0]['messages'])  # valid, returns None

        # 1b. add `chat_id` to df
        df = pd.DataFrame({
            'chat_id': [1, 1, 1, 2, 2, 2],
            'role': ['system', 'user', 'assistant'] * 2,
            'content': ['you are a helpful assistant', 'hello', 'hi, how can I help?'] * 2,
        })
        # add extra row at the end, belonging to first chat. This checks sorting.
        df = pd.concat([df, pd.DataFrame({'chat_id': [1], 'role': ['user'], 'content': ["I'm good, thanks"]})])
        chats = ft_chat_formatter(df)
        for chat in chats:
            assert list(chat.keys()) == ['messages']
            ft_chat_format_validation(chat['messages'])  # valid, returns None

        # 1c. add `message_id` to df (scrambled to check sorting)
        df = pd.DataFrame({
            'chat_id': [1, 2, 1, 2, 1, 2],
            'message_id': [1, 1, 2, 2, 3, 3],
            'role': ['system', 'system', 'user', 'user', 'assistant', 'assistant'],
            'content': ['you are a helpful assistant'] * 2 + ['hello'] * 2 + ['hi, how can I help?'] * 2,
        })
        chats = ft_chat_formatter(df)
        for chat in chats:
            assert list(chat.keys()) == ['messages']
            ft_chat_format_validation(chat['messages'])  # valid, returns None

        # 2a. json format - df contains single column `chat_json`
        df = pd.DataFrame({
            'chat_json': [
                '{"messages": [{"role": "user", "content": "hi"}, {"role": "assistant", "content": "hello"}]}'
            ]})
        chats = ft_chat_formatter(df)
        assert list(chats[0].keys()) == ['messages']
        ft_chat_format_validation(chats[0]['messages'])  # valid, returns None

    def test_ft_jsonl_validation(self):
        df = pd.DataFrame({
            'role': ['system', 'user', 'assistant', 'user'],
            'content': ['you are a helpful assistant', 'hello', 'hi, how can I help?', "I'm good, thanks"],
        })
        chats = ft_chat_formatter(df)

        # when validated, this method won't return anything
        assert ft_jsonl_validation([line for line in chats]) is None

        # otherwise, it raises an Exception
        chats = ft_chat_formatter(df)
        chats[0]['messages'][1]['role'] = 'invalid'
        with self.assertRaises(Exception):
            ft_jsonl_validation([line for line in chats])

    def test_ft_code_formatter(self):
        df = pd.DataFrame({'code': ["".join(
            [
                indent(dedent(
                    """
                    # format chunks into prompts
                    roles = []
                    contents = []

                    for idx in range(0, len(chunks), 3):
                    """),
                    " " * 4 * 2),  # mind the base indent level
                indent(dedent(
                    """pre, mid, suf = chunks[idx:idx+3]

                    interleaved = list(itertools.chain(*zip(templates, (pre, mid, suf))))
                    """),
                    " " * 4 * 3)  # mind the base indent level
            ])
        ]})
        df2 = ft_code_formatter(df, chunk_size=110)

        assert list(df2['role']) == ['system', 'user', 'assistant']
        assert df2['content'].iloc[0] == 'You are a powerful text to code model. Your job is to provide great code completions. As context, you are given code that is found immediately before and after the code you must generate.\n\nYou must output the code that should go in between the prefix and suffix.\n\n'  # noqa
        assert df2['content'].iloc[1] == '### Code prefix:\n# format chunks into prompts\n        roles = []\n        contents = []\n### Code suffix:\ninterleaved = list(itertools.chain(*zip(templates, (pre, mid, suf))))\n### Completion:'  # noqa
        assert df2['content'].iloc[2] == 'for idx in range(0, len(chunks), 3):\n            pre, mid, suf = chunks[idx:idx+3]'  # noqa

        df2 = ft_code_formatter(df, format='fim', chunk_size=110)
        assert list(df2['role']) == ['system', 'user', 'assistant']
        assert df2['content'].iloc[0] == 'You are a powerful text to code model. Your job is to provide great code completions. As context, you are given code that is found immediately before and after the code you must generate.\n\nYou must output the code that should go in between the prefix and suffix.\n\n'  # noqa
        assert df2['content'].iloc[1] == '<PRE>\n# format chunks into prompts\n        roles = []\n        contents = []\n<SUF>\ninterleaved = list(itertools.chain(*zip(templates, (pre, mid, suf))))\n<MID>'  # noqa
        assert df2['content'].iloc[2] == 'for idx in range(0, len(chunks), 3):\n            pre, mid, suf = chunks[idx:idx+3]'  # noqa

    def test_ft_cqa_formatter(self):
        df = pd.DataFrame({
            'instruction': ['Answer accurately.'],
            'context': ['You are a helpful assistant.'],
            'question': ['What is the capital of France?'],
            'answer': ['Paris'],
        })

        df2 = ft_cqa_formatter(df)

        assert list(df2['role']) == ['system', 'user', 'assistant']
        assert df2['content'].iloc[0] == 'Answer accurately.\nYou are a helpful assistant.'
        assert df2['content'].iloc[1] == 'What is the capital of France?'
        assert df2['content'].iloc[2] == 'Paris'
