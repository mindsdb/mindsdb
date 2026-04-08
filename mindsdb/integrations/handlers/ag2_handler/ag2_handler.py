"""AG2 multi-agent handler for MindsDB.

Enables creating and querying AG2 multi-agent GroupChats via SQL.

Usage:
    -- Create engine
    CREATE ML_ENGINE ag2_engine
    FROM ag2
    USING openai_api_key = 'sk-...';

    -- Create model (agent team)
    CREATE MODEL my_agent_team
    PREDICT answer
    USING
        engine = 'ag2_engine',
        agents = '[
            {"name": "Researcher", "system_message": "You research topics thoroughly."},
            {"name": "Writer", "system_message": "You write clear summaries."},
            {"name": "Critic", "system_message": "You review for accuracy. Say TERMINATE when done."}
        ]',
        max_rounds = 8;

    -- Query the agent team
    SELECT answer
    FROM my_agent_team
    WHERE question = 'Explain how transformers work';
"""

import json
import os
from typing import Any, Dict, Optional

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class AG2Handler(BaseMLEngine):
    """Handler for AG2 multi-agent framework."""

    name = "ag2"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True

    def create_engine(self, connection_args: Dict) -> None:
        """Validate engine connection args (API key)."""
        api_key = connection_args.get("openai_api_key") or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("openai_api_key is required. Pass it in USING clause or set OPENAI_API_KEY env var.")

        try:
            from autogen import LLMConfig

            model = connection_args.get("model", "gpt-4o-mini")
            api_type = connection_args.get("api_type", "openai")

            config = {"model": model, "api_key": api_key, "api_type": api_type}
            if connection_args.get("api_base"):
                config["base_url"] = connection_args["api_base"]

            LLMConfig(config)
        except ImportError:
            raise ImportError('AG2 is not installed. Run: pip install "ag2[openai]>=0.11.4,<1.0"')
        except Exception as e:
            raise ValueError(f"Failed to validate AG2 configuration: {e}")

    @staticmethod
    def create_validation(target: str, args: Optional[Dict] = None, **kwargs: Any) -> None:
        """Validate model creation args."""
        using_args = args.get("using", {})

        agents_json = using_args.get("agents")
        if agents_json:
            try:
                agents = json.loads(agents_json) if isinstance(agents_json, str) else agents_json
                if not isinstance(agents, list) or len(agents) == 0:
                    raise ValueError("'agents' must be a non-empty JSON list.")
                for agent in agents:
                    if "name" not in agent:
                        raise ValueError("Each agent must have a 'name' field.")
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid 'agents' JSON: {e}")

        mode = using_args.get("mode", "groupchat")
        if mode not in ("single", "groupchat"):
            raise ValueError(f"Invalid mode '{mode}'. Must be 'single' or 'groupchat'.")

        selection = using_args.get("speaker_selection", "auto")
        if selection not in ("auto", "round_robin", "random"):
            raise ValueError(f"Invalid speaker_selection '{selection}'. Must be 'auto', 'round_robin', or 'random'.")

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """Store model configuration."""
        using_args = args.get("using", {})
        using_args["target"] = target
        self.model_storage.json_set("args", using_args)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """Run AG2 agents for each row in the input DataFrame.

        Expects a 'question' column. Returns a DataFrame with the target column.
        """
        from autogen import LLMConfig

        stored_args = self.model_storage.json_get("args")
        predict_args = args.get("predict_params", {}) if args else {}
        merged_args = {**stored_args, **predict_args}

        # Build LLM config from engine args
        engine_args = self.engine_storage.get_connection_args()
        model = merged_args.get("model", engine_args.get("model", "gpt-4o-mini"))
        api_type = merged_args.get("api_type", engine_args.get("api_type", "openai"))

        api_key = engine_args.get("openai_api_key") or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("openai_api_key not found. Pass it in USING clause or set OPENAI_API_KEY env var.")

        config = {
            "model": model,
            "api_key": api_key,
            "api_type": api_type,
        }
        if engine_args.get("api_base"):
            config["base_url"] = engine_args["api_base"]

        llm_config = LLMConfig(config)

        # Parse agent definitions
        agents_json = merged_args.get("agents")
        if agents_json:
            agent_defs = json.loads(agents_json) if isinstance(agents_json, str) else agents_json
        else:
            agent_defs = [
                {
                    "name": "Assistant",
                    "system_message": (
                        "You are a helpful AI assistant. Provide clear, comprehensive "
                        "answers. Reply TERMINATE when the task is complete."
                    ),
                },
            ]

        mode = merged_args.get("mode", "groupchat" if len(agent_defs) > 1 else "single")
        max_rounds = int(merged_args.get("max_rounds", 8))
        speaker_selection = merged_args.get("speaker_selection", "auto")

        # Determine question column
        question_col = "question"
        if question_col not in df.columns:
            question_col = df.columns[0]

        target = merged_args.get("target", "answer")

        results = []
        for _, row in df.iterrows():
            question = str(row[question_col])

            try:
                answer = self._run_agents(
                    llm_config=llm_config,
                    agent_defs=agent_defs,
                    question=question,
                    mode=mode,
                    max_rounds=max_rounds,
                    speaker_selection=speaker_selection,
                )
                results.append({target: answer})
            except Exception as e:
                logger.error(f"AG2 prediction error: {e}")
                results.append({target: f"Error: {e}"})

        return pd.DataFrame(results)

    def _run_agents(
        self,
        llm_config,
        agent_defs: list,
        question: str,
        mode: str,
        max_rounds: int,
        speaker_selection: str,
    ) -> str:
        """Execute AG2 agent conversation and return the final answer."""
        from autogen import AssistantAgent, GroupChat, GroupChatManager, UserProxyAgent

        agents = []
        for agent_def in agent_defs:
            agent = AssistantAgent(
                name=agent_def["name"],
                system_message=agent_def.get(
                    "system_message",
                    f"You are {agent_def['name']}. Be helpful and concise.",
                ),
                llm_config=llm_config,
            )
            agents.append(agent)

        user_proxy = UserProxyAgent(
            name="User",
            human_input_mode="NEVER",
            max_consecutive_auto_reply=0,
            code_execution_config=False,
        )

        if mode == "single":
            user_proxy.run(agents[0], message=question).process()
            messages = agents[0].chat_messages.get(user_proxy, [])
        else:
            group_chat = GroupChat(
                agents=[user_proxy] + agents,
                messages=[],
                max_round=max_rounds,
                speaker_selection_method=speaker_selection,
            )
            manager = GroupChatManager(
                groupchat=group_chat,
                llm_config=llm_config,
            )
            user_proxy.run(manager, message=question).process()
            messages = group_chat.messages

        # Extract last non-user, non-empty message as the answer
        answer = ""
        for msg in reversed(messages):
            content = msg.get("content", "").strip()
            name = msg.get("name", "")
            if content and name != "User":
                answer = content.replace("TERMINATE", "").strip()
                if answer:
                    break

        return answer or "No answer generated."

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        """Describe the model configuration."""
        stored_args = self.model_storage.json_get("args")

        if attribute == "args":
            return pd.DataFrame([stored_args])
        elif attribute == "agents":
            agents_json = stored_args.get("agents", "[]")
            agents = json.loads(agents_json) if isinstance(agents_json, str) else agents_json
            return pd.DataFrame(agents) if agents else pd.DataFrame()
        else:
            agents_raw = stored_args.get("agents", "[]")
            agents = json.loads(agents_raw) if isinstance(agents_raw, str) else agents_raw
            info = {
                "name": "AG2 Multi-Agent Handler",
                "version": "0.0.1",
                "mode": stored_args.get("mode", "groupchat"),
                "max_rounds": stored_args.get("max_rounds", 8),
                "speaker_selection": stored_args.get("speaker_selection", "auto"),
                "num_agents": len(agents) if isinstance(agents, list) else 0,
            }
            return pd.DataFrame([info])
