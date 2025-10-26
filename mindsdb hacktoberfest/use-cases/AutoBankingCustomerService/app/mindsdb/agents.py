"""MindsDB Agent management and initialization.

This module provides:
1. Agent configuration (prompts, models)
2. Agent creation functions
3. Agent query interfaces
"""

import os
import re
from typing import Any, Dict, Optional

from dotenv import load_dotenv

load_dotenv()

# Agent configurations
AGENT_CONFIGS = {
    "classification_agent": {
        "model": {
            "provider": "openai",
            "model_name": "gpt-4o",
            "api_key": os.getenv("OPENAI_API_KEY", ""),
        },
        "prompt_template": """You are a banking customer service analyst. Analyze the conversation transcript and provide:

1. A concise summary (2-3 sentences) of the customer interaction
2. Classification of issue resolution status

IMPORTANT GUIDELINES FOR UNRESOLVED:
- If the customer expresses dissatisfaction, frustration, or complaints, mark as UNRESOLVED
- If the agent promises to "pass feedback along" or "escalate" without immediate resolution, mark as UNRESOLVED
- If the customer raises concerns about bank policies, procedures, or communication, mark as UNRESOLVED
- If the conversation ends without clear resolution, mark as UNRESOLVED
- If customer audio is missing or incomplete (e.g., only agent responses visible), mark as UNRESOLVED
- If the agent offers a solution but customer confirmation is missing, mark as UNRESOLVED
- If the conversation is cut off or incomplete, mark as UNRESOLVED

GUIDELINES FOR RESOLVED:
- Only mark as RESOLVED if the customer explicitly confirms satisfaction
- Look for explicit confirmation words like "thank you", "that worked", "issue resolved", "problem solved", "perfect"
- The customer must express contentment with the outcome, not just acceptance

Format your response EXACTLY as:
Summary: [your 2-3 sentence summary describing what happened in the conversation]
Status: [RESOLVED or UNRESOLVED]

Conversation to analyze:""",
    },
    "recommendation_agent": {
        "model": {
            "provider": "openai",
            "model_name": "gpt-4o",
            "api_key": os.getenv("OPENAI_API_KEY", ""),
        },
        "prompt_template": """You are a banking customer service expert. Based on the conversation summary and context, provide actionable recommendations for resolving the customer's issue.

Provide:
1. Immediate actions to take
2. Escalation procedures if needed
3. Follow-up recommendations

Be specific and actionable.

Conversation summary:""",
    },
    "analytics_agent": {
        "model": {
            "provider": "openai",
            "model_name": "gpt-4o",
            "api_key": os.getenv("OPENAI_API_KEY", ""),
        },
        "prompt_template": """You are a banking customer service analytics expert. Analyze the provided conversation data and generate comprehensive insights.

Your task:
1. Identify the most common customer issues (top 5-10)
2. Calculate resolution rates and trends
3. Provide key insights about customer service performance
4. Generate actionable recommendations for improvement

Input data will be provided as:
- Total conversations
- Resolved count
- Unresolved count
- Individual conversation summaries

Format your response EXACTLY as JSON:
{
  "top_issues": [
    {"issue": "issue name", "count": number, "percentage": number},
    ...
  ],
  "resolution_rate": number,
  "key_insights": "Your insights as a single text block",
  "recommendations": "Your recommendations as a single text block",
  "trend_direction": "improving|declining|stable"
}

Analysis data:""",
    },
    # Add more agents here as needed
}


def check_agent_exists(server, agent_name: str) -> bool:
    """Check if an agent exists in MindsDB."""
    try:
        result = server.query("SHOW AGENTS")
        data = result.fetch()
        return agent_name in data["NAME"].values if "NAME" in data.columns else False
    except Exception as e:
        print(f"⚠ Error checking agent '{agent_name}': {e}")
        return False


def create_agent(server, agent_name: str, config: Optional[Dict[str, Any]] = None) -> bool:
    """Create a single agent in MindsDB.

    Args:
        server: MindsDB server connection
        agent_name: Name of the agent to create
        config: Optional agent configuration (uses AGENT_CONFIGS if not provided)

    Returns:
        True if agent was created successfully or already exists
    """
    # Check if agent already exists
    if check_agent_exists(server, agent_name):
        print(f"✓ Agent '{agent_name}' already exists, skipping creation")
        return True

    # Get configuration
    agent_config = config or AGENT_CONFIGS.get(agent_name)
    if not agent_config:
        print(f"✗ No configuration found for agent '{agent_name}'")
        return False

    # Validate API key
    api_key = agent_config.get("model", {}).get("api_key", "")
    if not api_key:
        print(f"⚠ No API key found for agent '{agent_name}', skipping creation")
        return False

    print(f"\nCreating agent '{agent_name}'...")

    # Use SDK API instead of SQL to create agent
    model = agent_config["model"]
    prompt = agent_config["prompt_template"]

    try:
        server.agents.create(
            name=agent_name,
            model={
                "provider": model["provider"],
                "model_name": model["model_name"],
                "api_key": model["api_key"]
            },
            prompt_template=prompt
        )
        print(f"✓ Successfully created agent '{agent_name}'")
        return True
    except Exception as e:
        print(f"✗ Failed to create agent '{agent_name}': {e}")
        return False


def create_agents(server, agent_names: Optional[list] = None) -> Dict[str, bool]:
    """Create multiple agents in MindsDB.

    Args:
        server: MindsDB server connection
        agent_names: List of agent names to create (creates all if not provided)

    Returns:
        Dictionary mapping agent names to creation success status
    """
    agents_to_create = agent_names or list(AGENT_CONFIGS.keys())
    results = {}

    print(f"\nCreating {len(agents_to_create)} agent(s)...")

    for agent_name in agents_to_create:
        success = create_agent(server, agent_name)
        results[agent_name] = success

    # Summary
    successful = sum(1 for success in results.values() if success)
    print(f"\n✓ Agent creation completed: {successful}/{len(agents_to_create)} successful")

    return results


def query_classification_agent(agent, conversation_text: str) -> Dict[str, Any]:
    """Query the classification agent to analyze a conversation.

    Args:
        agent: MindsDB agent instance
        conversation_text: The conversation to analyze

    Returns:
        Dictionary with 'summary' and 'resolved' keys
    """
    if agent is None:
        raise ValueError("Classification agent not initialized")

    try:
        completion = agent.completion(
            [
                {
                    "question": conversation_text,
                    "answer": None,
                }
            ]
        )
    except Exception as exc:
        raise RuntimeError(f"Agent query failed: {exc}") from exc

    response_text = completion.content

    # Parse response
    summary_match = re.search(r"Summary:\s*(.+?)(?=Status:)", response_text, re.DOTALL)
    status_match = re.search(r"Status:\s*(RESOLVED|UNRESOLVED)", response_text, re.IGNORECASE)

    summary = summary_match.group(1).strip() if summary_match else response_text.strip()
    status = status_match.group(1).upper() if status_match else "UNRESOLVED"

    return {"summary": summary, "resolved": status == "RESOLVED"}


def query_recommendation_agent(
    agent, conversation_summary: str, conversation_text: str
) -> str:
    """Query the recommendation agent for actionable advice.

    Args:
        agent: MindsDB recommendation agent instance
        conversation_summary: Summary of the conversation
        conversation_text: Full conversation text

    Returns:
        Recommendation text with actionable steps
    """
    if agent is None:
        raise ValueError("Recommendation agent not initialized")

    try:
        # Create a comprehensive prompt for the recommendation agent
        prompt = f"""
Based on the customer complaints handling manual, provide specific recommendations for handling this unresolved customer service issue:

Conversation Summary: {conversation_summary}

Full Conversation:
{conversation_text}

Please provide:
1. Immediate action steps for the customer service team
2. Escalation procedures if needed
3. Follow-up recommendations
4. Any specific policies or procedures that apply

Format your response as clear, actionable recommendations that the customer service team can implement immediately.
"""

        completion = agent.completion(
            [
                {
                    "question": prompt,
                    "answer": None,
                }
            ]
        )
        return completion.content.strip()
    except Exception as exc:
        raise RuntimeError(f"Recommendation agent query failed: {exc}") from exc


# Agent registry (populated at startup)
_agents: Dict[str, Any] = {}


def register_agent(name: str, agent: Any) -> None:
    """Register an agent instance."""
    _agents[name] = agent


def get_agent(name: str) -> Optional[Any]:
    """Get a registered agent by name."""
    return _agents.get(name)


def clear_agents() -> None:
    """Clear all registered agents."""
    _agents.clear()
