"""Tests for the AG2 handler."""

import json
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from mindsdb.integrations.handlers.ag2_handler.ag2_handler import AG2Handler


class TestAG2HandlerValidation(unittest.TestCase):
    """Test AG2Handler validation methods."""

    def test_create_validation_valid_agents(self):
        args = {
            "using": {
                "agents": json.dumps(
                    [
                        {"name": "Agent1", "system_message": "You are agent 1."},
                        {"name": "Agent2", "system_message": "You are agent 2."},
                    ]
                ),
            }
        }
        # Should not raise
        AG2Handler.create_validation("answer", args)

    def test_create_validation_no_agents(self):
        args = {"using": {}}
        # Should not raise — agents are optional
        AG2Handler.create_validation("answer", args)

    def test_create_validation_invalid_agents_json(self):
        args = {"using": {"agents": "not-valid-json"}}
        with self.assertRaises(ValueError):
            AG2Handler.create_validation("answer", args)

    def test_create_validation_empty_agents_list(self):
        args = {"using": {"agents": "[]"}}
        with self.assertRaises(ValueError):
            AG2Handler.create_validation("answer", args)

    def test_create_validation_missing_agent_name(self):
        args = {
            "using": {
                "agents": json.dumps([{"system_message": "No name here."}]),
            }
        }
        with self.assertRaises(ValueError):
            AG2Handler.create_validation("answer", args)

    def test_create_validation_invalid_mode(self):
        args = {"using": {"mode": "invalid"}}
        with self.assertRaises(ValueError):
            AG2Handler.create_validation("answer", args)

    def test_create_validation_valid_modes(self):
        for mode in ("single", "groupchat"):
            args = {"using": {"mode": mode}}
            AG2Handler.create_validation("answer", args)

    def test_create_validation_invalid_speaker_selection(self):
        args = {"using": {"speaker_selection": "invalid"}}
        with self.assertRaises(ValueError):
            AG2Handler.create_validation("answer", args)

    def test_create_validation_valid_speaker_selections(self):
        for sel in ("auto", "round_robin", "random"):
            args = {"using": {"speaker_selection": sel}}
            AG2Handler.create_validation("answer", args)


class TestAG2HandlerCreate(unittest.TestCase):
    """Test AG2Handler create method."""

    def _make_handler(self):
        handler = AG2Handler.__new__(AG2Handler)
        handler.model_storage = MagicMock()
        handler.engine_storage = MagicMock()
        handler.engine_storage.get_connection_args.return_value = {
            "openai_api_key": "test-key",
            "model": "gpt-4o-mini",
            "api_type": "openai",
        }
        return handler

    def test_create_stores_args(self):
        handler = self._make_handler()
        args = {
            "using": {
                "agents": json.dumps([{"name": "Agent1"}]),
                "max_rounds": 5,
            }
        }
        handler.create("answer", args=args)
        stored = handler.model_storage.json_set.call_args[0]
        self.assertEqual(stored[0], "args")
        self.assertEqual(stored[1]["target"], "answer")
        self.assertEqual(stored[1]["max_rounds"], 5)

    def test_create_stores_target(self):
        handler = self._make_handler()
        handler.create("my_output", args={"using": {}})
        stored = handler.model_storage.json_set.call_args[0][1]
        self.assertEqual(stored["target"], "my_output")


class TestAG2HandlerPredict(unittest.TestCase):
    """Test AG2Handler predict method."""

    def _make_handler(self):
        handler = AG2Handler.__new__(AG2Handler)
        handler.model_storage = MagicMock()
        handler.engine_storage = MagicMock()
        handler.engine_storage.get_connection_args.return_value = {
            "openai_api_key": "test-key",
            "model": "gpt-4o-mini",
            "api_type": "openai",
        }
        return handler

    @patch.object(AG2Handler, "_run_agents")
    def test_predict_calls_agents_per_row(self, mock_run):
        mock_run.return_value = "Test answer"
        handler = self._make_handler()
        handler.model_storage.json_get.return_value = {
            "agents": json.dumps([{"name": "Agent1"}]),
            "max_rounds": 8,
            "target": "answer",
        }

        df = pd.DataFrame({"question": ["Q1", "Q2"]})
        result = handler.predict(df, args={})

        self.assertEqual(len(result), 2)
        self.assertEqual(result["answer"][0], "Test answer")
        self.assertEqual(result["answer"][1], "Test answer")
        self.assertEqual(mock_run.call_count, 2)

    @patch.object(AG2Handler, "_run_agents")
    def test_predict_uses_target_column(self, mock_run):
        mock_run.return_value = "Result"
        handler = self._make_handler()
        handler.model_storage.json_get.return_value = {
            "target": "my_output",
        }

        df = pd.DataFrame({"question": ["Q1"]})
        result = handler.predict(df, args={})

        self.assertIn("my_output", result.columns)
        self.assertEqual(result["my_output"][0], "Result")

    @patch.object(AG2Handler, "_run_agents")
    def test_predict_falls_back_to_first_column(self, mock_run):
        mock_run.return_value = "Answer"
        handler = self._make_handler()
        handler.model_storage.json_get.return_value = {
            "target": "answer",
        }

        df = pd.DataFrame({"prompt": ["Hello"]})
        handler.predict(df, args={})

        # Should use first column when 'question' is not present
        call_kwargs = mock_run.call_args
        self.assertEqual(call_kwargs[1]["question"], "Hello")

    @patch.object(AG2Handler, "_run_agents")
    def test_predict_handles_errors(self, mock_run):
        mock_run.side_effect = RuntimeError("LLM failed")
        handler = self._make_handler()
        handler.model_storage.json_get.return_value = {
            "target": "answer",
        }

        df = pd.DataFrame({"question": ["Q1"]})
        result = handler.predict(df, args={})

        self.assertIn("Error:", result["answer"][0])


class TestAG2HandlerDescribe(unittest.TestCase):
    """Test AG2Handler describe method."""

    def _make_handler(self):
        handler = AG2Handler.__new__(AG2Handler)
        handler.model_storage = MagicMock()
        handler.engine_storage = MagicMock()
        return handler

    def test_describe_default(self):
        handler = self._make_handler()
        handler.model_storage.json_get.return_value = {
            "mode": "groupchat",
            "max_rounds": 8,
            "speaker_selection": "auto",
            "agents": json.dumps([{"name": "A"}, {"name": "B"}]),
        }
        result = handler.describe()
        self.assertEqual(result["num_agents"][0], 2)
        self.assertEqual(result["mode"][0], "groupchat")

    def test_describe_args(self):
        handler = self._make_handler()
        stored = {"mode": "single", "max_rounds": 5}
        handler.model_storage.json_get.return_value = stored
        result = handler.describe(attribute="args")
        self.assertEqual(result["mode"][0], "single")

    def test_describe_agents(self):
        handler = self._make_handler()
        agents = [{"name": "Researcher"}, {"name": "Writer"}]
        handler.model_storage.json_get.return_value = {
            "agents": json.dumps(agents),
        }
        result = handler.describe(attribute="agents")
        self.assertEqual(len(result), 2)
        self.assertEqual(result["name"][0], "Researcher")

    def test_describe_no_agents(self):
        handler = self._make_handler()
        handler.model_storage.json_get.return_value = {}
        result = handler.describe(attribute="agents")
        self.assertTrue(result.empty)


if __name__ == "__main__":
    unittest.main()
