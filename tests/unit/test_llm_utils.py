import unittest

import pandas as pd

from mindsdb.integrations.libs.llm_utils import ft_jsonl_validation, ft_chat_formatter, ft_chat_format_validation


class TestLLM(unittest.TestCase):
    def test_ft_chat_format_validation(self):
        valid_chats = [
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

        invalid_chats = [
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

        for chat in valid_chats:
            # should return True, chat is valid
            assert ft_chat_format_validation(chat)

        for chat in invalid_chats:
            # all of these should return False
            assert not ft_chat_format_validation(chat)

    def test_ft_chat_formatter(self):
        # 1a. long DF with required columns (`role` and `content`)
        df = pd.DataFrame({
            'role': ['system', 'user', 'assistant', 'user'],
            'content': ['you are a helpful assistant', 'hello', 'hi, how can I help?', "I'm good, thanks"],
        })
        chats = ft_chat_formatter(df)
        assert list(chats[0].keys()) == ['messages']
        assert ft_chat_format_validation(chats[0]['messages'])

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
            assert ft_chat_format_validation(chat['messages'])

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
            assert ft_chat_format_validation(chat['messages'])

        # 2a. json format - df contains single column `chat_json`
        df = pd.DataFrame({
            'chat_json': [
                '{"messages": [{"role": "user", "content": "hi"}, {"role": "assistant", "content": "hello"}]}'
            ]})
        chats = ft_chat_formatter(df)
        assert list(chats[0].keys()) == ['messages']
        assert ft_chat_format_validation(chats[0]['messages'])


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
            self.assertRaises(ft_jsonl_validation([line for line in chats]))