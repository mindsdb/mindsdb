#!/usr/bin/env python3
"""Production server for Support Agent using LangGraph."""

from dotenv import load_dotenv

from agent import SupportAgent

# Load environment
load_dotenv()

# Create the agent
agent = SupportAgent()

# Export for LangGraph server
# The graph will automatically handle messages and thread_id from the state
support_agent = agent.agent
