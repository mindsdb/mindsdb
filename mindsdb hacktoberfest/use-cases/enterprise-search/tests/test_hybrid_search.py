#!/usr/bin/env python3
"""
Comprehensive test suite for hybrid search implementation.

This script tests all hybrid search functionality including:
- Basic semantic search
- Metadata filtering
- Advanced operators (LIKE, BETWEEN, IN, NOT_NULL)
- Cross-system search
- Error handling
"""

import os
import sys
from typing import Dict, List

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from integrations import confluence_client, jira_client, zendesk_client
from tools.search import SemanticSearchTool


class HybridSearchTester:
    """Comprehensive tester for hybrid search functionality."""

    def __init__(self):
        self.results = {"passed": 0, "failed": 0, "errors": []}
        self.jira = None
        self.zendesk = None
        self.confluence = None

    def setup_clients(self):
        """Initialize clients with error handling."""
        try:
            self.jira = jira_client.JiraClient()
            print("✅ JIRA client initialized")
        except Exception as e:
            print(f"❌ JIRA client failed: {e}")
            self.jira = None

        try:
            self.zendesk = zendesk_client.ZendeskClient()
            print("✅ Zendesk client initialized")
        except Exception as e:
            print(f"❌ Zendesk client failed: {e}")
            self.zendesk = None

        try:
            self.confluence = confluence_client.ConfluenceClient()
            print("✅ Confluence client initialized")
        except Exception as e:
            print(f"❌ Confluence client failed: {e}")
            self.confluence = None

    def test_basic_search(
        self, test_name: str, client, content: str, expected_fields: List[str]
    ):
        """Test basic semantic search functionality."""
        print(f"\n🔍 Testing {test_name}")

        try:
            if client is None:
                print(f"❌ {test_name}: Client not available")
                self.results["failed"] += 1
                return

            results = (
                client.search_tickets(content=content)
                if hasattr(client, "search_tickets")
                else client.search_pages(content=content)
            )

            if isinstance(results, list):
                print(f"✅ {test_name}: Found {len(results)} results")
                if results and len(results) > 0:
                    # Check if results have expected structure
                    first_result = results[0]
                    missing_fields = [
                        field for field in expected_fields if field not in first_result
                    ]
                    if missing_fields:
                        print(f"⚠️  {test_name}: Missing fields: {missing_fields}")
                    else:
                        print(f"✅ {test_name}: All expected fields present")
                self.results["passed"] += 1
            else:
                print(f"❌ {test_name}: Expected list, got {type(results)}")
                self.results["failed"] += 1

        except Exception as e:
            print(f"❌ {test_name}: Error - {e}")
            self.results["failed"] += 1
            self.results["errors"].append(f"{test_name}: {e}")

    def test_metadata_filtering(
        self, test_name: str, client, content: str, filters: Dict
    ):
        """Test metadata filtering functionality."""
        print(f"\n🔍 Testing {test_name}")

        try:
            if client is None:
                print(f"❌ {test_name}: Client not available")
                self.results["failed"] += 1
                return

            method = (
                client.search_tickets
                if hasattr(client, "search_tickets")
                else client.search_pages
            )
            results = method(content=content, filters=filters)

            if isinstance(results, list):
                print(
                    f"✅ {test_name}: Found {len(results)} results with filters {filters}"
                )
                self.results["passed"] += 1
            else:
                print(f"❌ {test_name}: Expected list, got {type(results)}")
                self.results["failed"] += 1

        except Exception as e:
            print(f"❌ {test_name}: Error - {e}")
            self.results["failed"] += 1
            self.results["errors"].append(f"{test_name}: {e}")

    def test_advanced_operators(
        self, test_name: str, client, content: str, filters: Dict
    ):
        """Test advanced filtering operators."""
        print(f"\n🔍 Testing {test_name}")

        try:
            if client is None:
                print(f"❌ {test_name}: Client not available")
                self.results["failed"] += 1
                return

            method = (
                client.search_tickets
                if hasattr(client, "search_tickets")
                else client.search_pages
            )
            results = method(content=content, filters=filters)

            if isinstance(results, list):
                print(
                    f"✅ {test_name}: Found {len(results)} results with advanced filters"
                )
                self.results["passed"] += 1
            else:
                print(f"❌ {test_name}: Expected list, got {type(results)}")
                self.results["failed"] += 1

        except Exception as e:
            print(f"❌ {test_name}: Error - {e}")
            self.results["failed"] += 1
            self.results["errors"].append(f"{test_name}: {e}")

    def test_specialized_methods(self, test_name: str, method, *args):
        """Test specialized search methods."""
        print(f"\n🔍 Testing {test_name}")

        try:
            if method is None:
                print(f"❌ {test_name}: Method not available")
                self.results["failed"] += 1
                return

            results = method(*args)

            if isinstance(results, list):
                print(f"✅ {test_name}: Found {len(results)} results")
                self.results["passed"] += 1
            else:
                print(f"❌ {test_name}: Expected list, got {type(results)}")
                self.results["failed"] += 1

        except Exception as e:
            print(f"❌ {test_name}: Error - {e}")
            self.results["failed"] += 1
            self.results["errors"].append(f"{test_name}: {e}")

    def test_semantic_search_tool(self):
        """Test the SemanticSearchTool directly."""
        print("\n🔍 Testing SemanticSearchTool")

        try:
            # Test with JIRA KB
            search_tool = SemanticSearchTool(kb_name="jira_kb")

            # Basic search
            results = search_tool.search(content="test", top_k=3)
            if isinstance(results, list):
                print(
                    f"✅ SemanticSearchTool basic search: Found {len(results)} results"
                )
                self.results["passed"] += 1
            else:
                print(
                    f"❌ SemanticSearchTool basic search: Expected list, got {type(results)}"
                )
                self.results["failed"] += 1

            # LIKE search with assignee (JIRA has this field)
            results = search_tool.hybrid_search_with_like(
                content="test", field="assignee", pattern="%test%"
            )
            if isinstance(results, list):
                print(
                    f"✅ SemanticSearchTool LIKE search: Found {len(results)} results"
                )
                self.results["passed"] += 1
            else:
                print(
                    f"❌ SemanticSearchTool LIKE search: Expected list, got {type(results)}"
                )
                self.results["failed"] += 1

            # Test with Zendesk KB for date range (Zendesk has created_at)
            zendesk_tool = SemanticSearchTool(kb_name="zendesk_kb")
            results = zendesk_tool.hybrid_search_with_date_range(
                content="test",
                date_field="created_at",
                start_date="2024-01-01",
                end_date="2024-12-31",
            )
            if isinstance(results, list):
                print(f"✅ SemanticSearchTool date range: Found {len(results)} results")
                self.results["passed"] += 1
            else:
                print(
                    f"❌ SemanticSearchTool date range: Expected list, got {type(results)}"
                )
                self.results["failed"] += 1

        except Exception as e:
            print(f"❌ SemanticSearchTool: Error - {e}")
            self.results["failed"] += 1
            self.results["errors"].append(f"SemanticSearchTool: {e}")

    def run_all_tests(self):
        """Run comprehensive test suite."""
        print("🚀 Starting Hybrid Search Test Suite")
        print("=" * 60)

        # Setup
        self.setup_clients()

        # Test basic search functionality
        print("\n📋 BASIC SEARCH TESTS")
        print("-" * 30)

        if self.jira:
            self.test_basic_search(
                "JIRA Basic Search",
                self.jira,
                "test",
                ["id", "key", "summary", "status"],
            )

        if self.zendesk:
            self.test_basic_search(
                "Zendesk Basic Search",
                self.zendesk,
                "test",
                ["id", "subject", "status", "priority"],
            )

        if self.confluence:
            self.test_basic_search(
                "Confluence Basic Search",
                self.confluence,
                "test",
                ["id", "title", "status", "spaceId"],
            )

        # Test metadata filtering
        print("\n📋 METADATA FILTERING TESTS")
        print("-" * 30)

        if self.jira:
            self.test_metadata_filtering(
                "JIRA Status Filter", self.jira, "test", {"status": "Open"}
            )

            self.test_metadata_filtering(
                "JIRA Priority Filter", self.jira, "test", {"priority": "High"}
            )

        if self.zendesk:
            self.test_metadata_filtering(
                "Zendesk Status Filter", self.zendesk, "test", {"status": "open"}
            )

            self.test_metadata_filtering(
                "Zendesk Priority Filter", self.zendesk, "test", {"priority": "urgent"}
            )

        if self.confluence:
            self.test_metadata_filtering(
                "Confluence Status Filter",
                self.confluence,
                "test",
                {"status": "current"},
            )

        # Test advanced operators
        print("\n📋 ADVANCED OPERATORS TESTS")
        print("-" * 30)

        if self.jira:
            self.test_advanced_operators(
                "JIRA LIKE Operator",
                self.jira,
                "test",
                {"assignee": {"operator": "LIKE", "value": "%test%"}},
            )

            self.test_advanced_operators(
                "JIRA IN Operator",
                self.jira,
                "test",
                {"status": {"operator": "IN", "values": ["Open", "In Progress"]}},
            )

        if self.zendesk:
            self.test_advanced_operators(
                "Zendesk BETWEEN Operator",
                self.zendesk,
                "test",
                {
                    "created_at": {
                        "operator": "BETWEEN",
                        "start": "2024-01-01",
                        "end": "2024-12-31",
                    }
                },
            )

        # Test specialized methods
        print("\n📋 SPECIALIZED METHODS TESTS")
        print("-" * 30)

        if self.jira:
            self.test_specialized_methods(
                "JIRA Search by Status",
                self.jira.search_tickets_by_status,
                "test",
                "Open",
            )

            self.test_specialized_methods(
                "JIRA Search by Priority",
                self.jira.search_tickets_by_priority,
                "test",
                "High",
            )

        if self.zendesk:
            self.test_specialized_methods(
                "Zendesk Search by Status",
                self.zendesk.search_tickets_by_status,
                "test",
                "open",
            )

            self.test_specialized_methods(
                "Zendesk Search by Type",
                self.zendesk.search_tickets_by_type,
                "test",
                "question",
            )

        if self.confluence:
            self.test_specialized_methods(
                "Confluence Search by Space",
                self.confluence.search_pages_by_space,
                "test",
                "DEV",
            )

            self.test_specialized_methods(
                "Confluence Search by Title",
                self.confluence.search_pages_by_title,
                "test",
                "%setup%",
            )

        # Test SemanticSearchTool directly
        print("\n📋 SEMANTIC SEARCH TOOL TESTS")
        print("-" * 30)
        self.test_semantic_search_tool()

        # Generate report
        self.generate_report()

    def generate_report(self):
        """Generate comprehensive test report."""
        print("\n" + "=" * 60)
        print("📊 TEST RESULTS SUMMARY")
        print("=" * 60)

        total_tests = self.results["passed"] + self.results["failed"]
        success_rate = (
            (self.results["passed"] / total_tests * 100) if total_tests > 0 else 0
        )

        print(f"✅ Passed: {self.results['passed']}")
        print(f"❌ Failed: {self.results['failed']}")
        print(f"📈 Success Rate: {success_rate:.1f}%")

        if self.results["errors"]:
            print("\n🚨 ERRORS ENCOUNTERED:")
            for error in self.results["errors"]:
                print(f"   • {error}")

        print("\n📋 CLIENT AVAILABILITY:")
        print(f"   • JIRA: {'✅ Available' if self.jira else '❌ Not Available'}")
        print(f"   • Zendesk: {'✅ Available' if self.zendesk else '❌ Not Available'}")
        print(
            f"   • Confluence: {'✅ Available' if self.confluence else '❌ Not Available'}"
        )

        if success_rate >= 80:
            print(f"\n🎉 OVERALL RESULT: PASSED ({success_rate:.1f}% success rate)")
        elif success_rate >= 60:
            print(f"\n⚠️  OVERALL RESULT: PARTIAL ({success_rate:.1f}% success rate)")
        else:
            print(f"\n❌ OVERALL RESULT: FAILED ({success_rate:.1f}% success rate)")


def main():
    """Run the test suite."""
    tester = HybridSearchTester()
    tester.run_all_tests()


if __name__ == "__main__":
    main()
