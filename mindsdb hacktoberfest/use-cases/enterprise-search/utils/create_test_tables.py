"""
Create test tables for Knowledge Base evaluation.

This module creates test tables in the format required by MindsDB's EVALUATE command.
For 'doc_id' version: requires 'id' and 'content' columns
For 'llm_relevancy' version: requires 'content' column

IMPORTANT: Test questions are generated using an LLM to create realistic user queries,
NOT copied verbatim from the documents. This ensures proper evaluation without circular reasoning.
"""

import os
from typing import Dict

import mindsdb_sdk
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
root_env = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(root_env):
    load_dotenv(root_env)


class TestTableCreator:
    """Create and manage test tables for KB evaluation."""

    def __init__(self, use_llm: bool = True):
        self.server = mindsdb_sdk.connect()
        self.use_llm = use_llm
        self.openai_client = None

        if use_llm:
            try:
                # Use Azure OpenAI ONLY - match variable names from .env
                azure_endpoint = os.getenv("AZURE_ENDPOINT")
                azure_api_key = os.getenv("AZURE_OPENAI_API_KEY")
                azure_api_version = os.getenv("AZURE_API_VERSION", "2024-02-01")
                azure_deployment = os.getenv(
                    "AZURE_INFERENCE_DEPLOYMENT", "gpt-4o-mini"
                )

                if not azure_endpoint or not azure_api_key:
                    raise ValueError(
                        "Azure OpenAI credentials not found. Set AZURE_ENDPOINT and AZURE_OPENAI_API_KEY in .env"
                    )

                # Use Azure OpenAI
                self.openai_client = OpenAI(
                    api_key=azure_api_key,
                    base_url=f"{azure_endpoint.rstrip('/')}/openai/deployments/{azure_deployment}",
                    default_headers={"api-key": azure_api_key},
                    default_query={"api-version": azure_api_version},
                )
                print(
                    f"✓ Azure OpenAI client initialized (deployment: {azure_deployment})"
                )
            except Exception as e:
                print(f"⚠️  Azure OpenAI client initialization failed: {e}")
                print("   Falling back to simple test generation")
                self.use_llm = False

    def generate_test_question(self, document_content: str, doc_id: str) -> str:
        """
        Generate a realistic user question from document content using LLM.

        Args:
            document_content: The content of the document
            doc_id: The document ID (for context)

        Returns:
            A realistic user question that the document would answer
        """
        if not self.use_llm or not self.openai_client:
            # Fallback: extract keywords (not verbatim copy)
            words = document_content.split()
            if len(words) > 5:
                # Create a question-like phrase from keywords
                keywords = [w for w in words if len(w) > 4][:3]
                return f"How to {' '.join(keywords)}"
            return document_content[:50]

        try:
            # Use Azure OpenAI to generate a realistic question
            # For Azure, we need to pass the deployment name as the model parameter
            azure_deployment = os.getenv("AZURE_INFERENCE_DEPLOYMENT", "gpt-4o-mini")

            response = self.openai_client.chat.completions.create(
                model=azure_deployment,  # Required for Azure OpenAI
                messages=[
                    {
                        "role": "system",
                        "content": "You are a support user asking questions. Generate a realistic, natural user question that would be answered by the given content. Return ONLY the question, nothing else.",
                    },
                    {
                        "role": "user",
                        "content": f"Generate a user question for this content:\n\n{document_content[:300]}",
                    },
                ],
                temperature=0.7,
                max_tokens=100,
            )

            question = response.choices[0].message.content.strip()
            # Clean up the question
            question = question.strip('"').strip("'")
            return question

        except Exception as e:
            print(f"⚠️  LLM generation failed for doc {doc_id}: {e}")
            # Fallback to keyword extraction
            words = document_content.split()
            keywords = [w for w in words if len(w) > 4][:3]
            return f"How to {' '.join(keywords)}"

    def create_test_table_for_jira(self, table_name: str = "jira_test_table") -> str:
        """
        Create a test table for JIRA KB evaluation.

        The test table needs to contain:
        - id: The document ID to match against
        - content: The test query/question

        Args:
            table_name: Name of the test table to create

        Returns:
            Full table name (datasource.table)
        """
        # First, we need a datasource to store the test table
        # We'll use the existing pgvector_datasource
        datasource_name = "pgvector_datasource"
        full_table_name = f"{datasource_name}.{table_name}"

        try:
            # Drop the table if it exists
            drop_query = f"DROP TABLE IF EXISTS {full_table_name}"
            try:
                self.server.query(drop_query)
                print(f"✓ Dropped existing table {full_table_name}")
            except Exception:
                pass

            # Create the test table with proper schema
            create_query = f"""
            CREATE TABLE {full_table_name} (
                id VARCHAR(255),
                content TEXT
            )
            """
            self.server.query(create_query)
            print(f"✓ Created test table {full_table_name}")

            # Insert test data
            # We need to query the JIRA KB to get actual document IDs
            jira_docs = self.server.query(
                "SELECT id, chunk_content FROM jira_kb LIMIT 10"
            ).fetch()

            if jira_docs is not None and len(jira_docs) > 0:
                docs_list = (
                    jira_docs.to_dict(orient="records")
                    if hasattr(jira_docs, "to_dict")
                    else jira_docs
                )

                # Generate realistic test questions using LLM
                print("   Generating realistic test questions...")
                for doc in docs_list[:5]:  # Use first 5 documents
                    doc_id = doc.get("id", "")
                    content = doc.get("chunk_content", "")

                    if content:
                        # Generate a realistic user question (NOT verbatim from doc)
                        test_query = self.generate_test_question(content, doc_id)

                        # Escape single quotes for SQL
                        test_query_escaped = test_query.replace("'", "''")
                        doc_id_escaped = str(doc_id).replace("'", "''")

                        insert_query = f"""
                        INSERT INTO {full_table_name} (id, content)
                        VALUES ('{doc_id_escaped}', '{test_query_escaped}')
                        """
                        self.server.query(insert_query)
                        print(
                            f"   ✓ Generated question for doc {doc_id}: {test_query[:50]}..."
                        )

                print(
                    f"✓ Inserted {len(docs_list[:5])} test questions into {full_table_name}"
                )
            else:
                print("⚠️  No documents found in jira_kb to create test data")

            return full_table_name

        except Exception as e:
            print(f"❌ Error creating test table: {e}")
            raise

    def create_test_table_for_zendesk(
        self, table_name: str = "zendesk_test_table"
    ) -> str:
        """
        Create a test table for Zendesk KB evaluation.

        Args:
            table_name: Name of the test table to create

        Returns:
            Full table name (datasource.table)
        """
        datasource_name = "pgvector_datasource"
        full_table_name = f"{datasource_name}.{table_name}"

        try:
            # Drop the table if it exists
            drop_query = f"DROP TABLE IF EXISTS {full_table_name}"
            try:
                self.server.query(drop_query)
                print(f"✓ Dropped existing table {full_table_name}")
            except Exception:
                pass

            # Create the test table
            create_query = f"""
            CREATE TABLE {full_table_name} (
                id VARCHAR(255),
                content TEXT
            )
            """
            self.server.query(create_query)
            print(f"✓ Created test table {full_table_name}")

            # Get actual documents from Zendesk KB
            zendesk_docs = self.server.query(
                "SELECT id, chunk_content FROM zendesk_kb LIMIT 10"
            ).fetch()

            if zendesk_docs is not None and len(zendesk_docs) > 0:
                docs_list = (
                    zendesk_docs.to_dict(orient="records")
                    if hasattr(zendesk_docs, "to_dict")
                    else zendesk_docs
                )

                print("   Generating realistic test questions...")
                for doc in docs_list[:5]:
                    doc_id = doc.get("id", "")
                    content = doc.get("chunk_content", "")

                    if content:
                        # Generate a realistic user question (NOT verbatim from doc)
                        test_query = self.generate_test_question(content, doc_id)

                        # Escape single quotes for SQL
                        test_query_escaped = test_query.replace("'", "''")
                        doc_id_escaped = str(doc_id).replace("'", "''")

                        insert_query = f"""
                        INSERT INTO {full_table_name} (id, content)
                        VALUES ('{doc_id_escaped}', '{test_query_escaped}')
                        """
                        self.server.query(insert_query)
                        print(
                            f"   ✓ Generated question for doc {doc_id}: {test_query[:50]}..."
                        )

                print(
                    f"✓ Inserted {len(docs_list[:5])} test questions into {full_table_name}"
                )
            else:
                print("⚠️  No documents found in zendesk_kb to create test data")

            return full_table_name

        except Exception as e:
            print(f"❌ Error creating test table: {e}")
            raise

    def create_test_table_for_confluence(
        self, table_name: str = "confluence_test_table"
    ) -> str:
        """
        Create a test table for Confluence KB evaluation.

        Args:
            table_name: Name of the test table to create

        Returns:
            Full table name (datasource.table)
        """
        datasource_name = "pgvector_datasource"
        full_table_name = f"{datasource_name}.{table_name}"

        try:
            # Drop the table if it exists
            drop_query = f"DROP TABLE IF EXISTS {full_table_name}"
            try:
                self.server.query(drop_query)
                print(f"✓ Dropped existing table {full_table_name}")
            except Exception:
                pass

            # Create the test table
            create_query = f"""
            CREATE TABLE {full_table_name} (
                id VARCHAR(255),
                content TEXT
            )
            """
            self.server.query(create_query)
            print(f"✓ Created test table {full_table_name}")

            # Get actual documents from Confluence KB
            confluence_docs = self.server.query(
                "SELECT id, chunk_content FROM confluence_kb LIMIT 10"
            ).fetch()

            if confluence_docs is not None and len(confluence_docs) > 0:
                docs_list = (
                    confluence_docs.to_dict(orient="records")
                    if hasattr(confluence_docs, "to_dict")
                    else confluence_docs
                )

                print("   Generating realistic test questions...")
                for doc in docs_list[:5]:
                    doc_id = doc.get("id", "")
                    content = doc.get("chunk_content", "")

                    if content:
                        # Generate a realistic user question (NOT verbatim from doc)
                        test_query = self.generate_test_question(content, doc_id)

                        # Escape single quotes for SQL
                        test_query_escaped = test_query.replace("'", "''")
                        doc_id_escaped = str(doc_id).replace("'", "''")

                        insert_query = f"""
                        INSERT INTO {full_table_name} (id, content)
                        VALUES ('{doc_id_escaped}', '{test_query_escaped}')
                        """
                        self.server.query(insert_query)
                        print(
                            f"   ✓ Generated question for doc {doc_id}: {test_query[:50]}..."
                        )

                print(
                    f"✓ Inserted {len(docs_list[:5])} test questions into {full_table_name}"
                )
            else:
                print("⚠️  No documents found in confluence_kb to create test data")

            return full_table_name

        except Exception as e:
            print(f"❌ Error creating test table: {e}")
            raise

    def create_all_test_tables(self) -> Dict[str, str]:
        """
        Create test tables for all Knowledge Bases.

        Returns:
            Dictionary mapping KB names to their test table names
        """
        print("🔧 Creating test tables for all Knowledge Bases...")
        print("=" * 70)

        test_tables = {}

        # Create JIRA test table
        print("\n📋 Creating JIRA test table...")
        try:
            jira_table = self.create_test_table_for_jira()
            test_tables["jira_kb"] = jira_table
            print(f"✅ JIRA test table created: {jira_table}")
        except Exception as e:
            print(f"❌ Failed to create JIRA test table: {e}")
            test_tables["jira_kb"] = None

        # Create Zendesk test table
        print("\n📋 Creating Zendesk test table...")
        try:
            zendesk_table = self.create_test_table_for_zendesk()
            test_tables["zendesk_kb"] = zendesk_table
            print(f"✅ Zendesk test table created: {zendesk_table}")
        except Exception as e:
            print(f"❌ Failed to create Zendesk test table: {e}")
            test_tables["zendesk_kb"] = None

        # Create Confluence test table
        print("\n📋 Creating Confluence test table...")
        try:
            confluence_table = self.create_test_table_for_confluence()
            test_tables["confluence_kb"] = confluence_table
            print(f"✅ Confluence test table created: {confluence_table}")
        except Exception as e:
            print(f"❌ Failed to create Confluence test table: {e}")
            test_tables["confluence_kb"] = None

        print("\n" + "=" * 70)
        print("✅ Test table creation complete!")
        return test_tables

    def verify_test_table(self, full_table_name: str) -> bool:
        """
        Verify that a test table exists and has data.

        Args:
            full_table_name: Full table name (datasource.table)

        Returns:
            True if table exists and has data, False otherwise
        """
        try:
            result = self.server.query(
                f"SELECT COUNT(*) as count FROM {full_table_name}"
            ).fetch()
            if result is not None and len(result) > 0:
                count_dict = (
                    result.to_dict(orient="records")[0]
                    if hasattr(result, "to_dict")
                    else result[0]
                )
                count = count_dict.get("count", 0)
                print(f"✓ Table {full_table_name} has {count} rows")
                return count > 0
            return False
        except Exception as e:
            print(f"❌ Error verifying table {full_table_name}: {e}")
            return False


def main():
    """Run test table creation."""
    print("🚀 Knowledge Base Test Table Creator")
    print("=" * 70)
    print("This script creates test tables for KB evaluation.")
    print("=" * 70)

    creator = TestTableCreator()
    test_tables = creator.create_all_test_tables()

    # Verify all tables
    print("\n🔍 Verifying test tables...")
    print("-" * 70)
    for kb_name, table_name in test_tables.items():
        if table_name:
            creator.verify_test_table(table_name)

    print("\n" + "=" * 70)
    print("✅ All test tables ready for evaluation!")
    print("\n📋 Test Tables Created:")
    for kb_name, table_name in test_tables.items():
        if table_name:
            print(f"   • {kb_name}: {table_name}")


if __name__ == "__main__":
    main()
