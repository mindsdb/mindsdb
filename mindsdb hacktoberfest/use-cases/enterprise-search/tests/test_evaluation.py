#!/usr/bin/env python3
"""
Complete Knowledge Base Evaluation with Test Table Creation.

This script:
1. Creates test tables for all Knowledge Bases
2. Evaluates all KBs using the official EVALUATE command
3. Generates comprehensive metrics (MRR, Hit@k, relevancy)
4. Creates visual charts
5. Displays formatted report
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.create_test_tables import TestTableCreator
from utils.evaluate_kb import KBEvaluator


def main():
    """Run complete evaluation with test table creation."""
    print("🚀 Complete Knowledge Base Evaluation")
    print("=" * 70)
    print("This demo will:")
    print("  • Create test tables for all Knowledge Bases")
    print("  • Evaluate using MindsDB's EVALUATE command")
    print("  • Calculate metrics: MRR, Hit@k, relevancy scores")
    print("  • Generate visual charts")
    print("  • Display comprehensive report")
    print("=" * 70)

    # Step 1: Create test tables
    print("\n🔧 Step 1: Creating Test Tables...")
    print("-" * 70)
    table_creator = TestTableCreator()
    test_tables = table_creator.create_all_test_tables()

    # Verify test tables
    print("\n🔍 Verifying test tables...")
    print("-" * 70)
    valid_tables = {}
    for kb_name, table_name in test_tables.items():
        if table_name and table_creator.verify_test_table(table_name):
            valid_tables[kb_name] = table_name
            print(f"✅ {kb_name}: {table_name} is ready")
        else:
            print(f"⚠️  {kb_name}: Test table not available, will use fallback")

    # Step 2: Evaluate all Knowledge Bases
    print("\n📊 Step 2: Evaluating Knowledge Bases...")
    print("-" * 70)
    evaluator = KBEvaluator()
    metrics = evaluator.evaluate_all_kbs(test_tables=valid_tables)

    # Step 3: Generate report
    print("\n📋 Step 3: Generating Report...")
    print("-" * 70)
    report = evaluator.create_evaluation_report(metrics)
    print(report)

    # Step 4: Create visualizations
    print("\n📊 Step 4: Creating Visual Charts...")
    print("-" * 70)
    evaluator.visualize_metrics(metrics)

    # Step 5: Summary
    print("\n" + "=" * 70)
    print("✅ COMPLETE EVALUATION FINISHED")
    print("=" * 70)
    print("\n📊 Generated Files:")
    print("  • evaluation_charts/kb_evaluation_comparison.png")
    print("  • evaluation_charts/kb_detailed_metrics.png")
    print("\n💡 What was evaluated:")
    print("  • Test tables created from actual KB documents")
    print("  • Official MindsDB EVALUATE command used")
    print("  • Metrics include MRR, Hit@k, and recall scores")
    print("  • Visual charts show cross-KB comparison")
    print("\n🎯 Next Steps:")
    print("  • Review the metrics and charts")
    print("  • Include in your competition submission")
    print("  • Share results with your team")
    print("\n📋 Test Tables Created:")
    for kb_name, table_name in valid_tables.items():
        if table_name:
            print(f"   • {kb_name}: {table_name}")


if __name__ == "__main__":
    main()
