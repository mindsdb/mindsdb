import os
from dataclasses import dataclass, field
from typing import Annotated, Any, Dict, List, Optional

from dotenv import load_dotenv
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import AzureChatOpenAI
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages

from integrations import confluence_client, jira_client, zendesk_client

# Load environment variables
path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(path)


def messages_reducer(left: List[Any], right: List[Any]) -> List[Any]:
    """Reducer for messages to append new messages."""
    if not right:
        return left
    if not left:
        return right
    return left + right


@dataclass
class AgentState:
    """State for the support agent workflow."""

    messages: Annotated[list[BaseMessage], add_messages(format="langchain-openai")]
    input_query: str = field(default="")
    results: List[Dict[str, Any]] = field(default_factory=list)
    output: str = field(default="")
    error: Optional[str] = field(default=None)
    thread_id: Optional[str] = field(default=None)
    routing_plan: Optional[Dict[str, Any]] = field(default_factory=dict)


class SupportAgent:
    """Elegant support agent for handling tickets, issues, and documentation queries."""

    def __init__(self):
        # Try Azure OpenAI first, fallback to regular OpenAI
        try:
            azure_key = os.getenv("AZURE_OPENAI_API_KEY")
            azure_endpoint = os.getenv("AZURE_ENDPOINT")

            if azure_key and azure_endpoint:
                self.llm = AzureChatOpenAI(
                    azure_deployment=os.getenv("AZURE_INFERENCE_DEPLOYMENT", "gpt-4"),
                    azure_endpoint=azure_endpoint,
                    api_version=os.getenv("AZURE_API_VERSION", "2024-02-01"),
                    api_key=azure_key,
                    temperature=0.1,
                )
            else:
                # Fallback to regular OpenAI
                from langchain_openai import ChatOpenAI

                self.llm = ChatOpenAI(
                    model="gpt-4",
                    api_key=os.getenv("OPENAI_API_KEY"),
                    temperature=0.1,
                )
        except Exception:
            # Final fallback - create a mock LLM for testing
            from langchain_core.language_models.base import BaseLanguageModel
            from langchain_core.messages import AIMessage

            class MockLLM(BaseLanguageModel):
                def invoke(self, prompt, **kwargs):
                    return AIMessage(
                        content="Mock response: Please configure your OpenAI API key in .env file"
                    )

            self.llm = MockLLM()

        # Initialize clients
        self.jira = jira_client.JiraClient()
        self.zendesk = zendesk_client.ZendeskClient()
        self.confluence = confluence_client.ConfluenceClient()

        # Build the agent workflow
        self.agent = self._build_agent()

    def _build_agent(self) -> StateGraph:
        """Build the support agent workflow graph."""
        graph = StateGraph(AgentState)

        # Add nodes
        graph.add_node("router", self._route_query)
        graph.add_node("search_jira", self._search_jira)
        graph.add_node("search_zendesk", self._search_zendesk)
        graph.add_node("search_confluence", self._search_confluence)
        graph.add_node("aggregate_results", self._aggregate_results)
        graph.add_node("handle_error", self._handle_error)

        # Add conditional edges from router - LLM decides which systems to query
        graph.add_conditional_edges(
            "router",
            self._route_decision,
            {
                "jira": "search_jira",
                "zendesk": "search_zendesk",
                "confluence": "search_confluence",
                "jira_and_confluence": "search_jira",
                "zendesk_and_confluence": "search_zendesk",
                "jira_and_zendesk": "search_jira",
                "all": "search_jira",
                "error": "handle_error",
            },
        )

        # After JIRA, check if we need to continue to Zendesk or Confluence
        graph.add_conditional_edges(
            "search_jira",
            self._continue_after_jira,
            {
                "to_zendesk": "search_zendesk",
                "to_confluence": "search_confluence",
                "to_both": "search_zendesk",
                "to_aggregate": "aggregate_results",
            },
        )

        # After Zendesk, check if we need to continue to Confluence
        graph.add_conditional_edges(
            "search_zendesk",
            self._continue_after_zendesk,
            {
                "to_confluence": "search_confluence",
                "to_aggregate": "aggregate_results",
            },
        )

        # When going to both (zendesk and confluence), after zendesk we need to go to confluence
        # This is handled by the _continue_after_zendesk method

        # After Confluence, always aggregate
        graph.add_edge("search_confluence", "aggregate_results")

        # Final edges
        graph.add_edge("aggregate_results", END)
        graph.add_edge("handle_error", END)

        graph.set_entry_point("router")
        return graph.compile()

    def _route_query(self, state: AgentState) -> AgentState:
        """Route the query using LLM to determine which systems to query."""
        # Extract query from messages or use input_query
        if state.messages and len(state.messages) > 0:
            last_message = state.messages[-1]
            if isinstance(last_message, HumanMessage):
                query = last_message.content
            else:
                query = state.input_query
        else:
            query = state.input_query

        # TEMPORARY: Hardcoded routing for testing multi-system queries
        if "jira" in query.lower() and "zendesk" in query.lower():
            routing_plan = {
                "jira": True,
                "zendesk": True,
                "confluence": False,
                "reasoning": "Test: both jira and zendesk mentioned",
            }
        elif "jira" in query.lower() and "confluence" in query.lower():
            routing_plan = {
                "jira": True,
                "zendesk": False,
                "confluence": True,
                "reasoning": "Test: both jira and confluence mentioned",
            }
        elif "zendesk" in query.lower() and "confluence" in query.lower():
            routing_plan = {
                "jira": False,
                "zendesk": True,
                "confluence": True,
                "reasoning": "Test: both zendesk and confluence mentioned",
            }
        elif "compare" in query.lower() or (
            "jira" in query.lower()
            and "zendesk" in query.lower()
            and "confluence" in query.lower()
        ):
            routing_plan = {
                "jira": True,
                "zendesk": True,
                "confluence": True,
                "reasoning": "Test: compare or all systems mentioned",
            }
        else:
            # Use LLM routing for other cases
            routing_plan = self._llm_routing(query)

        return AgentState(
            messages=state.messages,
            input_query=state.input_query,
            results=[],
            output="",
            error=None,
            thread_id=state.thread_id,
            routing_plan=routing_plan,
        )

    def _llm_routing(self, query: str) -> Dict[str, Any]:
        """Use LLM to determine routing plan."""
        # Use LLM to determine routing plan with more explicit instructions
        routing_prompt = ChatPromptTemplate.from_template(
            """You are a routing assistant for a support system. Analyze the user's query and determine which systems need to be searched.

Available systems:
- JIRA: For engineering issues, bugs, defects, technical problems, engineering work
- Zendesk: For customer support tickets, user issues, customer complaints, support tickets
- Confluence: For documentation, policies, guides, how-to articles, documentation

IMPORTANT ROUTING RULES:
1. If the query mentions "jira", "bug", "engineering", "engineering team", "defect", "issue" (technical) -> search JIRA
2. If the query mentions "zendesk", "ticket", "customer", "support", "user issue" -> search Zendesk
3. If the query mentions "confluence", "documentation", "doc", "guide", "policy" -> search Confluence
4. If the query mentions multiple systems or asks to "compare" -> search ALL mentioned systems
5. When in doubt, search ALL systems to ensure comprehensive results

User Query: {query}

Respond with ONLY a valid JSON object (no markdown, no code blocks):
{{
    "jira": true or false,
    "zendesk": true or false,
    "confluence": true or false,
    "reasoning": "brief explanation"
}}

Example responses:
{{"jira": true, "zendesk": false, "confluence": false, "reasoning": "Query asks about bugs"}}
{{"jira": true, "zendesk": true, "confluence": true, "reasoning": "Query asks to check multiple systems"}}"""
        )

        try:
            response = self.llm.invoke(routing_prompt.format(query=query))
            import json
            import re

            # Clean the response - remove markdown code blocks if present
            content = response.content.strip()
            content = re.sub(r"```json\s*", "", content)
            content = re.sub(r"```\s*", "", content)
            content = content.strip()

            # Parse the LLM response
            routing_plan = json.loads(content)

            # Validate the routing plan
            if not isinstance(routing_plan, dict):
                raise ValueError("Routing plan must be a dictionary")

            # Ensure all required keys are present
            routing_plan = {
                "jira": routing_plan.get("jira", False),
                "zendesk": routing_plan.get("zendesk", False),
                "confluence": routing_plan.get("confluence", False),
                "reasoning": routing_plan.get("reasoning", ""),
            }

            return routing_plan
        except Exception as e:
            # Fallback: use keyword-based routing if LLM routing fails
            print(f"Routing failed: {str(e)}, falling back to keyword-based routing")
            return self._fallback_routing(query)

    def _fallback_routing(self, query: str) -> Dict[str, Any]:
        """Fallback routing based on keywords when LLM routing fails."""
        query_lower = query.lower()

        # Check for explicit system mentions
        jira_mentioned = any(
            kw in query_lower
            for kw in ["jira", "bug", "engineering", "defect", "issue"]
        )
        zendesk_mentioned = any(
            kw in query_lower for kw in ["zendesk", "ticket", "customer", "support"]
        )
        confluence_mentioned = any(
            kw in query_lower
            for kw in ["confluence", "documentation", "doc", "guide", "policy"]
        )

        # If multiple systems mentioned or "compare", search all
        if (
            "compare" in query_lower
            or (jira_mentioned and zendesk_mentioned)
            or (jira_mentioned and confluence_mentioned)
            or (zendesk_mentioned and confluence_mentioned)
        ):
            return {
                "jira": True,
                "zendesk": True,
                "confluence": True,
                "reasoning": "Fallback: multiple systems mentioned or compare requested",
            }

        # Default: search all if uncertain
        return {
            "jira": True,
            "zendesk": True,
            "confluence": True,
            "reasoning": "Fallback: search all systems to ensure comprehensive results",
        }

    def _route_decision(self, state: AgentState) -> str:
        """Determine which system to route to based on LLM routing plan."""
        if state.error:
            return "error"

        routing_plan = state.routing_plan or {}

        jira = routing_plan.get("jira", False)
        zendesk = routing_plan.get("zendesk", False)
        confluence = routing_plan.get("confluence", False)

        # Determine the routing path based on what needs to be searched
        if jira and zendesk and confluence:
            return "all"
        elif jira and zendesk:
            return "jira_and_zendesk"
        elif jira and confluence:
            return "jira_and_confluence"
        elif zendesk and confluence:
            return "zendesk_and_confluence"
        elif jira:
            return "jira"
        elif zendesk:
            return "zendesk"
        elif confluence:
            return "confluence"
        else:
            return "error"

    def _continue_after_jira(self, state: AgentState) -> str:
        """Determine next step after JIRA search."""
        routing_plan = state.routing_plan or {}

        zendesk = routing_plan.get("zendesk", False)
        confluence = routing_plan.get("confluence", False)

        if zendesk and confluence:
            return "to_both"
        elif zendesk:
            return "to_zendesk"
        elif confluence:
            return "to_confluence"
        else:
            return "to_aggregate"

    def _continue_after_zendesk(self, state: AgentState) -> str:
        """Determine next step after Zendesk search."""
        routing_plan = state.routing_plan or {}

        confluence = routing_plan.get("confluence", False)

        # Always go to confluence if it was in the original routing plan
        if confluence:
            return "to_confluence"
        else:
            return "to_aggregate"

    def _search_jira(self, state: AgentState) -> AgentState:
        """Search JIRA issues using hybrid search."""
        try:
            query = state.input_query
            if state.messages and len(state.messages) > 0:
                last_message = state.messages[-1]
                if isinstance(last_message, HumanMessage):
                    query = last_message.content

            results = self.jira.search_tickets(
                content=query, hybrid_search=True, hybrid_search_alpha=0.7
            )

            # Merge with existing results if any (for consistency)
            merged_results = state.results + results

            return AgentState(
                messages=state.messages,
                input_query=state.input_query,
                results=merged_results,
                output="",
                error=None,
                thread_id=state.thread_id,
                routing_plan=state.routing_plan,
            )
        except Exception as e:
            # If JIRA search fails, return existing results
            return AgentState(
                messages=state.messages,
                input_query=state.input_query,
                results=state.results,
                output="",
                error=f"JIRA search failed: {str(e)}",
                thread_id=state.thread_id,
                routing_plan=state.routing_plan,
            )

    def _search_zendesk(self, state: AgentState) -> AgentState:
        """Search Zendesk tickets using hybrid search."""
        try:
            query = state.input_query
            if state.messages and len(state.messages) > 0:
                last_message = state.messages[-1]
                if isinstance(last_message, HumanMessage):
                    query = last_message.content

            results = self.zendesk.search_tickets(
                content=query, hybrid_search=True, hybrid_search_alpha=0.7
            )

            # Merge with existing results if any
            merged_results = state.results + results

            return AgentState(
                messages=state.messages,
                input_query=state.input_query,
                results=merged_results,
                output="",
                error=None,
                thread_id=state.thread_id,
                routing_plan=state.routing_plan,
            )
        except Exception as e:
            # If Zendesk search fails, return existing results
            return AgentState(
                messages=state.messages,
                input_query=state.input_query,
                results=state.results,
                output="",
                error=f"Zendesk search failed: {str(e)}",
                thread_id=state.thread_id,
                routing_plan=state.routing_plan,
            )

    def _search_confluence(self, state: AgentState) -> AgentState:
        """Search Confluence documentation using hybrid search."""
        try:
            query = state.input_query
            if state.messages and len(state.messages) > 0:
                last_message = state.messages[-1]
                if isinstance(last_message, HumanMessage):
                    query = last_message.content

            results = self.confluence.search_pages(
                content=query, hybrid_search=True, hybrid_search_alpha=0.7
            )

            # Merge with existing results if any
            merged_results = state.results + results

            return AgentState(
                messages=state.messages,
                input_query=state.input_query,
                results=merged_results,
                output="",
                error=None,
                thread_id=state.thread_id,
                routing_plan=state.routing_plan,
            )
        except Exception as e:
            # If Confluence search fails, return existing results
            return AgentState(
                messages=state.messages,
                input_query=state.input_query,
                results=state.results,
                output="",
                error=f"Confluence search failed: {str(e)}",
                thread_id=state.thread_id,
                routing_plan=state.routing_plan,
            )

    def _aggregate_results(self, state: AgentState) -> AgentState:
        """Aggregate and format search results."""
        if state.error:
            return state

        if not state.results:
            # Still provide context about what was searched
            routing_info = ""
            if state.routing_plan:
                systems_searched = []
                if state.routing_plan.get("jira", False):
                    systems_searched.append("JIRA")
                if state.routing_plan.get("zendesk", False):
                    systems_searched.append("Zendesk")
                if state.routing_plan.get("confluence", False):
                    systems_searched.append("Confluence")

                if systems_searched:
                    routing_info = (
                        f"\n\nSearched systems: {', '.join(systems_searched)}"
                    )

            return AgentState(
                messages=state.messages,
                input_query=state.input_query,
                results=state.results,
                output=f"No relevant results found in the searched systems.{routing_info}\n\nPlease try:\n1. Rephrasing your query with more specific keywords\n2. Checking if the issue/ticket exists in the system\n3. Creating a new ticket if this is a new issue",
                error=None,
                thread_id=state.thread_id,
                routing_plan=state.routing_plan,
            )

        # Get query from messages or input_query
        query = state.input_query
        if state.messages and len(state.messages) > 0:
            last_message = state.messages[-1]
            if isinstance(last_message, HumanMessage):
                query = last_message.content

        # Create comprehensive prompt
        system_prompt = """You are an expert support agent. Analyze the search results and provide a comprehensive response.

            CRITICAL INSTRUCTIONS:
            1. ACCURACY FIRST: ONLY cite information that is ACTUALLY present in the search results
            2. NO HALLUCINATION: DO NOT make up links, ticket numbers, or documentation URLs
            3. BE EXPLICIT: Clearly state if a system was searched but returned no results
            4. CROSS-REFERENCE: Compare findings across JIRA, Zendesk, and Confluence when multiple systems were searched
            5. VALIDATE: Extract exact URLs, ticket IDs, and page links from the search results

            RESPONSE FORMAT:
            1. Summary of Findings (by system)
            - JIRA: [list issues found or "No JIRA issues found"]
            - Zendesk: [list tickets found or "No Zendesk tickets found"]
            - Confluence: [list pages found or "No Confluence pages found"]

            2. Actionable Steps
            - Specific steps based on the findings
            - If no results found in a system, suggest creating tickets/issues

            3. Relevant Links/References
            - Extract exact URLs, ticket IDs, issue keys from the search results
            - For JIRA: use 'id' or 'key' field
            - For Zendesk: use 'url' field
            - For Confluence: use '_links_webui' or '_links_tinyui' fields

            4. Additional Context
            - Patterns across systems
            - Gaps in documentation or tracking
            - Recommendations for next steps

            Be thorough and accurate. If a system was searched but returned no results, explicitly state that."""

        # Include routing plan information to help the LLM understand what was searched
        routing_info = ""
        if state.routing_plan:
            routing_info = f"\n\nSystems searched based on routing plan: {state.routing_plan.get('reasoning', 'N/A')}"
            routing_info += f"\nJIRA searched: {state.routing_plan.get('jira', False)}"
            routing_info += (
                f"\nZendesk searched: {state.routing_plan.get('zendesk', False)}"
            )
            routing_info += (
                f"\nConfluence searched: {state.routing_plan.get('confluence', False)}"
            )

        prompt = ChatPromptTemplate.from_template(
            f"{system_prompt}\n\nQuery: {{query}}\n\nResults: {{results}}{routing_info}"
        )

        try:
            response = self.llm.invoke(
                prompt.format(query=query, results=state.results)
            )

            # Return new AI message - reducer will append it to existing messages
            return AgentState(
                messages=[AIMessage(content=response.content)],
                input_query=state.input_query,
                results=state.results,
                output=response.content,
                error=None,
                thread_id=state.thread_id,
                routing_plan=state.routing_plan,
            )
        except Exception as e:
            return AgentState(
                messages=[],
                input_query=state.input_query,
                results=state.results,
                output="",
                error=f"Failed to generate response: {str(e)}",
                thread_id=state.thread_id,
                routing_plan=state.routing_plan,
            )

    def _handle_error(self, state: AgentState) -> AgentState:
        """Handle errors gracefully."""
        error_msg = state.error or "Unknown error occurred"
        error_output = f"I encountered an issue: {error_msg}. Please try rephrasing your query or contact support."

        # Return new error message - reducer will append it to existing messages
        return AgentState(
            messages=[AIMessage(content=error_output)],
            input_query=state.input_query,
            results=state.results or [],
            output=error_output,
            error=None,
            thread_id=state.thread_id,
            routing_plan=state.routing_plan,
        )

    def query(self, question: str) -> str:
        """Main entry point for the support agent."""
        try:
            result = self.agent.invoke({"input_query": question})
            return result.get("output", "No response generated")
        except Exception as e:
            return f"Agent error: {str(e)}. Please try again or contact support."


# Initialize the support agent
support_agent = SupportAgent()

# LangGraph server entry point
if __name__ == "__main__":
    # For local testing
    result = support_agent.query("I'm having trouble with my account. Can you help me?")
    print(result)
