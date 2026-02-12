from contextlib import contextmanager
from unittest.mock import Mock, patch
import pandas as pd
from pydantic_ai.messages import ModelRequest, ModelResponse

from mindsdb.interfaces.agents.pydantic_ai_agent import PydanticAIAgent
from mindsdb.interfaces.agents.utils.constants import ASSISTANT_COLUMN, CONTEXT_COLUMN, TRACE_ID_COLUMN


class TestPydanticAIAgent:
    @staticmethod
    def _new_agent():
        return PydanticAIAgent.__new__(PydanticAIAgent)

    @staticmethod
    def _history_to_text(history):
        if not history:
            return ""
        content = []
        for msg in history:
            if not hasattr(msg, "parts"):
                continue
            for part in msg.parts:
                if hasattr(part, "content"):
                    content.append(str(part.content))
        return "".join(content)

    @staticmethod
    def _make_planning_agent(plan="Plan", estimated_steps=1):
        planning_agent = Mock()
        plan_output = Mock()
        plan_output.plan = plan
        plan_output.estimated_steps = estimated_steps
        planning_agent.run_sync.return_value = Mock(output=plan_output)
        return planning_agent

    @staticmethod
    def _make_sql_agent(trace_id="trace-1", system_prompt="Test"):
        agent = PydanticAIAgent.__new__(PydanticAIAgent)
        agent.llm_params = {"model_name": "test-model"}
        langfuse_client = Mock()
        langfuse_client.get_trace_id.return_value = trace_id
        langfuse_client.setup_trace = Mock()
        langfuse_client.start_span = Mock(return_value=Mock())
        langfuse_client.end_span = Mock()
        agent.langfuse_client_wrapper = langfuse_client
        agent.model_instance = Mock()
        agent.system_prompt = system_prompt
        agent.agent_mode = "sql"
        agent.select_targets = None
        agent.sql_toolkit = Mock()
        agent.sql_toolkit.knowledge_bases = []
        return agent

    @contextmanager
    def _patched_agents(self, main_agent, catalog="Catalog"):
        with patch("mindsdb.interfaces.agents.pydantic_ai_agent.Agent") as MockAgent, \
             patch("mindsdb.interfaces.agents.pydantic_ai_agent.DataCatalogBuilder") as MockDataCatalog:
            MockAgent.side_effect = [self._make_planning_agent(), main_agent]
            mock_catalog = Mock()
            mock_catalog.build_data_catalog.return_value = catalog
            MockDataCatalog.return_value = mock_catalog
            yield

    def test_extracts_prompt_and_history_from_role_content_dataframe(self):
        agent = self._new_agent()
        df = pd.DataFrame(
            [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi there"},
                {"role": "user", "content": "What's the status?"},
            ]
        )

        current_prompt, history = agent._extract_current_prompt_and_history(df, {})

        assert current_prompt == "What's the status?"
        assert len(history) == 2

    def test_extracts_prompt_when_last_message_is_assistant(self):
        agent = self._new_agent()
        df = pd.DataFrame(
            [
                {"role": "user", "content": "Ping"},
                {"role": "assistant", "content": "Pong"},
            ]
        )

        current_prompt, history = agent._extract_current_prompt_and_history(df, {})

        assert current_prompt == "Ping"
        assert len(history) == 1

    def test_extracts_prompt_and_history_from_role_content_list(self):
        agent = self._new_agent()
        messages = [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi"},
            {"role": "user", "content": "Next step?"},
        ]

        current_prompt, history = agent._extract_current_prompt_and_history(messages, {})

        assert current_prompt == "Next step?"
        assert len(history) == 2
        assert isinstance(history[0], ModelRequest)
        assert isinstance(history[1], ModelResponse)

    def test_extracts_prompt_from_role_content_list_when_last_is_assistant(self):
        agent = self._new_agent()
        messages = [
            {"role": "user", "content": "Ping"},
            {"role": "assistant", "content": "Pong"},
        ]

        current_prompt, history = agent._extract_current_prompt_and_history(messages, {})

        assert current_prompt == "Ping"
        assert len(history) == 1
        assert isinstance(history[0], ModelRequest)

    def test_extracts_prompt_and_history_from_qa_list(self):
        agent = self._new_agent()
        messages = [
            {"question": "What time?", "answer": "Noon"},
            {"question": "Next?", "answer": ""},
        ]

        current_prompt, history = agent._extract_current_prompt_and_history(messages, {})

        assert current_prompt == "Next?"
        assert len(history) == 2
        assert isinstance(history[0], ModelRequest)
        assert isinstance(history[1], ModelResponse)

    