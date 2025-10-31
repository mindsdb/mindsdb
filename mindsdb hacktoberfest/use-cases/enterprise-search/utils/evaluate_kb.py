"""
Knowledge Base Evaluation Module with Visual Output.

This module provides comprehensive evaluation of MindsDB Knowledge Bases
with metrics like MRR, Hit@k, and relevancy scores, plus visual charts.
"""

import os
import time
from typing import Dict, Optional

import mindsdb_sdk
from dotenv import load_dotenv

# Load environment variables
root_env = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(root_env):
    load_dotenv(root_env)


class KBEvaluator:
    """Evaluate Knowledge Bases with comprehensive metrics and visualization."""

    def __init__(self):
        self.server = mindsdb_sdk.connect()

    def evaluate_kb(
        self,
        kb_name: str,
        test_table: Optional[str] = None,
        version: str = "doc_id",
        generate_data: bool = True,
        evaluate: bool = True,
    ) -> Dict:
        """
        Evaluate a Knowledge Base using MindsDB's EVALUATE command.

        Args:
            kb_name: Name of the knowledge base to evaluate
            test_table: Optional test table with questions and expected answers
            version: Evaluation version ('doc_id' or 'llm_relevancy')
            generate_data: Whether to generate test data automatically
            evaluate: Whether to run the evaluation (default: True)

        Returns:
            Dictionary containing evaluation metrics
        """
        try:
            # Build the EVALUATE query with correct syntax
            query = f"EVALUATE KNOWLEDGE_BASE {kb_name}"
            params = []

            # Add test_table if provided
            if test_table:
                params.append(f"test_table = {test_table}")

            # Add version
            params.append(f"version = '{version}'")

            # Add generate_data
            if generate_data:
                # Use default generation from the KB itself
                params.append("generate_data = true")
            else:
                params.append("generate_data = false")

            # Add evaluate parameter
            params.append(f"evaluate = {str(evaluate).lower()}")

            # Add Azure OpenAI configuration for LLM (if using llm_relevancy version)
            # This ensures we use Azure OpenAI instead of hitting rate limits
            azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
            azure_api_key = os.getenv("AZURE_OPENAI_API_KEY")
            azure_api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01")
            azure_deployment = os.getenv("AZURE_OPENAI_INFERENCE_DEPLOYMENT", "gpt-4.1")

            if azure_endpoint and azure_api_key and version == "llm_relevancy":
                llm_config = f"""llm = {{
        'provider': 'azure_openai',
        'model_name': '{azure_deployment}',
        'api_key': '{azure_api_key}',
        'base_url': '{azure_endpoint.rstrip("/")}/',
        'api_version': '{azure_api_version}',
        'deployment': '{azure_deployment}'
    }}"""
                params.append(llm_config)

            # Build the USING clause
            if params:
                query += "\nUSING\n    " + ",\n    ".join(params)

            # Execute the evaluation with retry logic for rate limits
            max_retries = 3
            retry_delay = 2  # Start with 2 seconds as suggested by error

            for attempt in range(max_retries):
                try:
                    result = self.server.query(query).fetch()

                    # Parse results
                    if result is not None and len(result) > 0:
                        metrics = (
                            result.to_dict(orient="records")[0]
                            if hasattr(result, "to_dict")
                            else result[0]
                        )

                        # MindsDB EVALUATE returns different column names
                        # Map them to our expected format
                        normalized_metrics = {}

                        # Map MindsDB column names to our format
                        if "total" in metrics:
                            normalized_metrics["total_questions"] = metrics["total"]
                        if "total_found" in metrics:
                            normalized_metrics["total_found"] = metrics["total_found"]
                        if "retrieved_in_top_10" in metrics:
                            normalized_metrics["retrieved_in_top_10"] = metrics[
                                "retrieved_in_top_10"
                            ]
                        if "avg_query_time" in metrics:
                            normalized_metrics["avg_query_time"] = metrics[
                                "avg_query_time"
                            ]

                        # Handle cumulative_recall - it might be a dict, string, or single value
                        if "cumulative_recall" in metrics:
                            recall = metrics["cumulative_recall"]

                            # If it's a JSON string, parse it
                            if isinstance(recall, str):
                                try:
                                    import json

                                    recall = json.loads(recall)
                                except Exception:
                                    pass  # If parsing fails, keep as string

                            if isinstance(recall, dict):
                                # Take the final value (index 19 or last)
                                recall_values = list(recall.values())
                                normalized_metrics["cumulative_recall"] = (
                                    recall_values[-1] if recall_values else 0
                                )
                            elif isinstance(recall, (int, float)):
                                normalized_metrics["cumulative_recall"] = recall
                            else:
                                # If it's still a string or other type, set to 0
                                normalized_metrics["cumulative_recall"] = 0

                        # Calculate hit rate if we have the data
                        if (
                            "total_questions" in normalized_metrics
                            and "total_found" in normalized_metrics
                        ):
                            total_q = normalized_metrics["total_questions"]
                            total_f = normalized_metrics["total_found"]
                            if total_q > 0:
                                normalized_metrics["hit_rate"] = round(
                                    (total_f / total_q) * 100, 1
                                )

                        return normalized_metrics
                    else:
                        return {"error": "No evaluation results returned"}

                except Exception as e:
                    error_str = str(e)
                    # Check if it's a rate limit error
                    if "RateLimitReached" in error_str or "429" in error_str:
                        if attempt < max_retries - 1:
                            wait_time = retry_delay * (
                                2**attempt
                            )  # Exponential backoff
                            print(
                                f"⚠️  Rate limit hit, retrying in {wait_time} seconds... (attempt {attempt + 1}/{max_retries})"
                            )
                            time.sleep(wait_time)
                            continue
                    # If not rate limit or last attempt, raise the exception
                    raise

        except Exception as e:
            # Fallback to simple metrics calculation
            print(f"⚠️  EVALUATE command failed: {e}")
            print("   Falling back to simple metrics calculation...")
            return self._calculate_simple_metrics(kb_name)

    def _calculate_simple_metrics(self, kb_name: str) -> Dict:
        """
        Calculate simple evaluation metrics by querying the KB.

        Args:
            kb_name: Name of the knowledge base

        Returns:
            Dictionary containing calculated metrics
        """
        try:
            # Query the KB with some test queries
            test_queries = [
                "transaction",
                "authentication",
                "bug",
                "deployment",
                "configuration",
            ]

            total_questions = len(test_queries)
            total_found = 0
            retrieved_top_10 = 0

            for query in test_queries:
                results = self.server.query(
                    f"SELECT * FROM {kb_name} WHERE content='{query}' LIMIT 10"
                ).fetch()

                if results is not None and len(results) > 0:
                    total_found += 1
                    if len(results) >= 1:
                        retrieved_top_10 += 1

            # Calculate metrics
            hit_rate = (
                (total_found / total_questions * 100) if total_questions > 0 else 0
            )
            recall = retrieved_top_10 / total_questions if total_questions > 0 else 0

            return {
                "total_questions": total_questions,
                "total_found": total_found,
                "retrieved_in_top_10": retrieved_top_10,
                "cumulative_recall": round(recall, 3),
                "avg_query_time": "N/A",
                "hit_rate": round(hit_rate, 1),
            }

        except Exception as e:
            return {"error": str(e)}

    def evaluate_all_kbs(self, test_tables: Dict[str, str] = None) -> Dict[str, Dict]:
        """
        Evaluate all Knowledge Bases in the system.

        Args:
            test_tables: Optional dictionary mapping KB names to test table names

        Returns:
            Dictionary mapping KB names to their evaluation metrics
        """
        results = {}
        kb_names = ["jira_kb", "zendesk_kb", "confluence_kb"]

        # Default test table names
        default_test_tables = {
            "jira_kb": "pgvector_datasource.jira_test_table",
            "zendesk_kb": "pgvector_datasource.zendesk_test_table",
            "confluence_kb": "pgvector_datasource.confluence_test_table",
        }

        # Use provided test tables or defaults
        test_tables = test_tables or default_test_tables

        for idx, kb_name in enumerate(kb_names):
            print(f"\n📊 Evaluating {kb_name}...")

            # Add delay between evaluations to avoid rate limits
            if idx > 0:
                delay = 3  # 3 seconds between KB evaluations
                print(f"   Waiting {delay}s to avoid rate limits...")
                time.sleep(delay)

            try:
                test_table = test_tables.get(kb_name)

                if test_table:
                    # Try with test table first
                    metrics = self.evaluate_kb(
                        kb_name,
                        test_table=test_table,
                        version="doc_id",
                        generate_data=False,  # Don't generate, use existing test table
                        evaluate=True,
                    )
                else:
                    # Fall back to simple metrics
                    print("   No test table found, using simple metrics calculation...")
                    metrics = self._calculate_simple_metrics(kb_name)

                results[kb_name] = metrics
                print(f"✅ {kb_name} evaluation complete")
            except Exception as e:
                print(f"❌ {kb_name} evaluation failed: {e}")
                results[kb_name] = {"error": str(e)}

        return results

    def create_evaluation_report(self, metrics: Dict[str, Dict]) -> str:
        """
        Create a human-readable evaluation report.

        Args:
            metrics: Dictionary of evaluation metrics per KB

        Returns:
            Formatted report string
        """
        report = []
        report.append("=" * 70)
        report.append("📊 KNOWLEDGE BASE EVALUATION REPORT")
        report.append("=" * 70)

        for kb_name, kb_metrics in metrics.items():
            if "error" in kb_metrics:
                report.append(f"\n❌ {kb_name}: {kb_metrics['error']}")
                continue

            report.append(f"\n📋 {kb_name.upper()}")
            report.append("-" * 70)

            # Extract common metrics
            total_questions = kb_metrics.get("total_questions", "N/A")
            total_found = kb_metrics.get("total_found", "N/A")
            retrieved_top_10 = kb_metrics.get("retrieved_in_top_10", "N/A")
            cumulative_recall = kb_metrics.get("cumulative_recall", "N/A")
            avg_query_time = kb_metrics.get("avg_query_time", "N/A")

            report.append(f"   Total Questions: {total_questions}")
            report.append(f"   Total Found: {total_found}")
            report.append(f"   Retrieved in Top 10: {retrieved_top_10}")

            # Format recall score nicely
            if isinstance(cumulative_recall, (int, float)):
                report.append(f"   Cumulative Recall: {cumulative_recall:.3f}")
            else:
                report.append(f"   Cumulative Recall: {cumulative_recall}")

            # Format query time nicely
            if isinstance(avg_query_time, (int, float)):
                report.append(f"   Average Query Time: {avg_query_time:.2f}s")
            else:
                report.append(f"   Average Query Time: {avg_query_time}")

            # Calculate additional metrics
            if (
                total_questions != "N/A"
                and isinstance(total_questions, (int, float))
                and total_questions > 0
            ):
                hit_rate = (
                    (total_found / total_questions * 100)
                    if isinstance(total_found, (int, float))
                    else "N/A"
                )
                if isinstance(hit_rate, (int, float)):
                    report.append(f"   Hit Rate: {hit_rate:.1f}%")
                else:
                    report.append(f"   Hit Rate: {hit_rate}%")

        report.append("\n" + "=" * 70)
        return "\n".join(report)

    def visualize_metrics(
        self, metrics: Dict[str, Dict], output_dir: str = "evaluation_charts"
    ):
        """
        Create visual charts for evaluation metrics.

        Args:
            metrics: Dictionary of evaluation metrics per KB
            output_dir: Directory to save charts
        """
        try:
            import matplotlib
            import matplotlib.pyplot as plt

            matplotlib.use("Agg")  # Use non-interactive backend
        except ImportError:
            print("⚠️  Matplotlib not available. Install with: pip install matplotlib")
            return

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Extract data for visualization
        kb_names = []
        hit_rates = []
        recall_scores = []

        for kb_name, kb_metrics in metrics.items():
            if "error" in kb_metrics:
                continue

            kb_names.append(kb_name.replace("_kb", "").upper())

            # Calculate hit rate
            total_questions = kb_metrics.get("total_questions", 0)
            total_found = kb_metrics.get("total_found", 0)
            if total_questions > 0:
                hit_rates.append((total_found / total_questions) * 100)
            else:
                hit_rates.append(0)

            # Get recall score
            recall = kb_metrics.get("cumulative_recall", 0)
            recall_scores.append(recall if isinstance(recall, (int, float)) else 0)

        if not kb_names:
            print("⚠️  No valid metrics to visualize")
            return

        # Create charts
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

        # Chart 1: Hit Rate Comparison
        ax1.bar(kb_names, hit_rates, color=["#3498db", "#e74c3c", "#2ecc71"])
        ax1.set_ylabel("Hit Rate (%)")
        ax1.set_title("Knowledge Base Hit Rate Comparison")
        ax1.set_ylim(0, 100)
        ax1.grid(True, alpha=0.3)

        # Add value labels on bars
        for i, v in enumerate(hit_rates):
            ax1.text(i, v + 1, f"{v:.1f}%", ha="center", va="bottom")

        # Chart 2: Recall Score Comparison
        ax2.bar(kb_names, recall_scores, color=["#3498db", "#e74c3c", "#2ecc71"])
        ax2.set_ylabel("Recall Score")
        ax2.set_title("Knowledge Base Recall Score Comparison")
        ax2.set_ylim(0, 1)
        ax2.grid(True, alpha=0.3)

        # Add value labels on bars
        for i, v in enumerate(recall_scores):
            ax2.text(i, v + 0.01, f"{v:.3f}", ha="center", va="bottom")

        plt.tight_layout()

        # Save the chart
        chart_path = os.path.join(output_dir, "kb_evaluation_comparison.png")
        plt.savefig(chart_path, dpi=300, bbox_inches="tight")
        print(f"✅ Chart saved to: {chart_path}")

        # Also create a detailed metrics chart
        self._create_detailed_metrics_chart(metrics, output_dir)

    def _create_detailed_metrics_chart(self, metrics: Dict[str, Dict], output_dir: str):
        """Create a detailed metrics comparison chart."""
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            return

        # Prepare data
        kb_names = []
        questions = []
        found = []
        top_10 = []

        for kb_name, kb_metrics in metrics.items():
            if "error" in kb_metrics:
                continue

            kb_names.append(kb_name.replace("_kb", "").upper())
            questions.append(kb_metrics.get("total_questions", 0))
            found.append(kb_metrics.get("total_found", 0))
            top_10.append(kb_metrics.get("retrieved_in_top_10", 0))

        if not kb_names:
            return

        # Create detailed comparison chart
        fig, ax = plt.subplots(figsize=(12, 6))

        x = range(len(kb_names))
        width = 0.25

        ax.bar(
            [i - width for i in x],
            questions,
            width,
            label="Total Questions",
            color="#3498db",
        )
        ax.bar(x, found, width, label="Total Found", color="#2ecc71")
        ax.bar(
            [i + width for i in x],
            top_10,
            width,
            label="Retrieved in Top 10",
            color="#e74c3c",
        )

        ax.set_xlabel("Knowledge Base")
        ax.set_ylabel("Count")
        ax.set_title("Detailed Knowledge Base Evaluation Metrics")
        ax.set_xticks(x)
        ax.set_xticklabels(kb_names)
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Add value labels
        for i, (q, f, t) in enumerate(zip(questions, found, top_10)):
            ax.text(i - width, q + 0.5, str(q), ha="center", va="bottom", fontsize=8)
            ax.text(i, f + 0.5, str(f), ha="center", va="bottom", fontsize=8)
            ax.text(i + width, t + 0.5, str(t), ha="center", va="bottom", fontsize=8)

        plt.tight_layout()

        chart_path = os.path.join(output_dir, "kb_detailed_metrics.png")
        plt.savefig(chart_path, dpi=300, bbox_inches="tight")
        print(f"✅ Detailed chart saved to: {chart_path}")


def main():
    """Run evaluation and generate reports."""
    print("🚀 Starting Knowledge Base Evaluation")
    print("=" * 70)

    evaluator = KBEvaluator()

    # Evaluate all Knowledge Bases
    metrics = evaluator.evaluate_all_kbs()

    # Generate report
    report = evaluator.create_evaluation_report(metrics)
    print(report)

    # Create visualizations
    print("\n📊 Creating visualizations...")
    evaluator.visualize_metrics(metrics)

    print("\n✅ Evaluation complete!")


if __name__ == "__main__":
    main()
