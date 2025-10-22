"""Recommendation agent client for generating customer service recommendations."""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import HTTPException


class RecommendationClientError(RuntimeError):
    """Raised when recommendation agent interaction fails."""


class RecommendationClient:
    """Client for interacting with the MindsDB recommendation agent."""

    def __init__(self, agent: Any):
        """Initialize the recommendation client with a MindsDB agent."""
        self._agent = agent

    def get_recommendation(self, conversation_text: str, summary: str) -> str:
        """
        Get a recommendation for handling an unresolved customer service issue.
        
        Args:
            conversation_text: The full conversation transcript
            summary: The AI-generated summary of the conversation
            
        Returns:
            A recommendation string based on the customer complaints handling manual
            
        Raises:
            RecommendationClientError: If the recommendation request fails
        """
        if self._agent is None:
            raise RecommendationClientError("Recommendation agent not initialized")

        try:
            # Create a prompt that includes both the conversation and summary
            prompt = f"""
Based on the customer complaints handling manual, provide specific recommendations for handling this unresolved customer service issue:

Conversation Summary: {summary}

Full Conversation:
{conversation_text}

Please provide:
1. Immediate action steps for the customer service team
2. Escalation procedures if needed
3. Follow-up recommendations
4. Any specific policies or procedures that apply

Format your response as clear, actionable recommendations that the customer service team can implement immediately.
"""
            
            completion = self._agent.completion([
                {
                    "question": prompt,
                    "answer": None,
                }
            ])
            
            return completion.content.strip()
            
        except Exception as exc:
            raise RecommendationClientError(f"Failed to get recommendation: {exc}") from exc


def build_recommendation_client(agent: Any) -> Optional[RecommendationClient]:
    """Helper that creates a RecommendationClient from a MindsDB agent."""
    if agent is None:
        return None
    return RecommendationClient(agent)


__all__ = [
    "RecommendationClient",
    "RecommendationClientError", 
    "build_recommendation_client",
]


