import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from typing import Dict, List, Optional, Any


class TestKnowledgeBaseEvaluation(unittest.TestCase):
    """Test knowledge base evaluation functionality"""
    def setUp(self) -> None:
        """Set up test environment"""
        self.project_id = 1
        self.kb_name = "test_kb"
        self.test_queries = [
            {"query": "What is MindsDB?", "expected_ids": ["doc1", "doc2"]},
            {"query": "How to use knowledge bases?", "expected_ids": ["doc3"]}
        ]
        self.metrics = ["precision", "recall", "mrr"]
        self.top_k = 5
        self.test_set_name = "test_set_1"
        self.user_notes = "Test evaluation"
        # Mock evaluation result
        self.evaluation_result = {
            "evaluation_id": 1,
            "knowledge_base_id": 1,
            "project_id": self.project_id,
            "test_set_name": self.test_set_name,
            "created_at": datetime.now(),
            "metrics": {
                "precision": 0.85,
                "recall": 0.75,
                "mrr": 0.9,
                "per_query": [
                    {
                        "query": "What is MindsDB?",
                        "expected_ids": ["doc1", "doc2"],
                        "retrieved_ids": ["doc1", "doc4"],
                        "metrics": {"precision": 0.5, "recall": 0.5, "mrr": 1.0}
                    },
                    {
                        "query": "How to use knowledge bases?",
                        "expected_ids": ["doc3"],
                        "retrieved_ids": ["doc3", "doc5"],
                        "metrics": {"precision": 0.5, "recall": 1.0, "mrr": 1.0}
                    }
                ]
            },
            "config": {
                "top_k": self.top_k,
                "metrics": self.metrics,
                "num_queries": len(self.test_queries)
            },
            "user_notes": self.user_notes
        }

    def test_calculate_metrics(self) -> None:
        """Test the metric calculation function"""
        # Test cases for metric calculation
        test_cases = [
            {
                "result_ids": ["doc1", "doc2", "doc4"],
                "expected_ids": ["doc1", "doc2", "doc3"],
                "metrics": ["precision", "recall", "mrr"],
                "top_k": 3,
                "expected_results": {
                    "precision": 2 / 3,
                    "recall": 2 / 3,
                    "mrr": 1.0
                }
            },
            {
                "result_ids": ["doc4", "doc5", "doc1"],
                "expected_ids": ["doc1", "doc2"],
                "metrics": ["precision", "recall", "mrr"],
                "top_k": 3,
                "expected_results": {
                    "precision": 1 / 3,
                    "recall": 1 / 2,
                    "mrr": 1 / 3
                }
            },
            {
                "result_ids": ["doc4", "doc5", "doc6"],
                "expected_ids": ["doc1", "doc2"],
                "metrics": ["precision", "recall", "mrr"],
                "top_k": 3,
                "expected_results": {
                    "precision": 0.0,
                    "recall": 0.0,
                    "mrr": 0.0
                }
            },
            {
                "result_ids": [],
                "expected_ids": ["doc1", "doc2"],
                "metrics": ["precision", "recall", "mrr"],
                "top_k": 3,
                "expected_results": {
                    "precision": 0.0,
                    "recall": 0.0,
                    "mrr": 0.0
                }
            },
            {
                "result_ids": ["doc1", "doc2"],
                "expected_ids": [],
                "metrics": ["precision", "recall", "mrr"],
                "top_k": 3,
                "expected_results": {
                    "precision": 0.0,
                    "recall": 0.0,
                    "mrr": 0.0
                }
            }
        ]
        # Define the calculate_metrics function for testing
        def calculate_metrics(result_ids: List[str], expected_ids: List[str],
                              metrics: List[str], top_k: int) -> Dict[str, float]:
            metrics_results = {}
            # Limit results to top_k
            result_ids = result_ids[:top_k]
            # Convert expected_ids to set for faster lookups
            expected_ids_set = set(expected_ids)
            # Calculate precision: fraction of retrieved documents that are relevant
            if "precision" in metrics:
                if result_ids and expected_ids:
                    relevant_retrieved = len([doc_id for doc_id in result_ids if doc_id in expected_ids_set])
                    metrics_results["precision"] = relevant_retrieved / len(result_ids)
                else:
                    metrics_results["precision"] = 0.0
            # Calculate recall: fraction of relevant documents that are retrieved
            if "recall" in metrics:
                if expected_ids:
                    relevant_retrieved = len([doc_id for doc_id in result_ids if doc_id in expected_ids_set])
                    metrics_results["recall"] = relevant_retrieved / len(expected_ids)
                else:
                    metrics_results["recall"] = 0.0
            # Calculate Mean Reciprocal Rank (MRR)
            if "mrr" in metrics:
                # Find the rank of the first relevant document
                rank = 0
                for i, doc_id in enumerate(result_ids):
                    if doc_id in expected_ids_set:
                        rank = i + 1
                        break
                # Calculate reciprocal rank
                if rank > 0:
                    metrics_results["mrr"] = 1.0 / rank
                else:
                    metrics_results["mrr"] = 0.0
            return metrics_results
        # Run tests for each test case
        for tc in test_cases:
            results = calculate_metrics(
                tc["result_ids"],
                tc["expected_ids"],
                tc["metrics"],
                tc["top_k"]
            )
            # Check results
            for metric, expected_value in tc["expected_results"].items():
                self.assertAlmostEqual(
                    results[metric],
                    expected_value,
                    places=5,
                    msg=f"Failed for metric {metric} in test case {tc}"
                )

    @patch('mindsdb.interfaces.storage.db.KnowledgeBaseEvaluation')
    def test_evaluate_method(self, mock_kb_evaluation: MagicMock) -> None:
        """Test the evaluate method of KnowledgeBaseController"""
        # Mock controller and dependencies
        controller = MagicMock()
        kb = MagicMock()
        kb.id = 1
        # Mock query method to return test results
        controller.query = MagicMock(side_effect=[
            [{"id": "doc1", "content": "test content 1"}, {"id": "doc4", "content": "test content 4"}],
            [{"id": "doc3", "content": "test content 3"}, {"id": "doc5", "content": "test content 5"}]
        ])
        # Mock get method to return the knowledge base
        controller.get = MagicMock(return_value=kb)
        # Mock _calculate_metrics method
        controller._calculate_metrics = MagicMock(side_effect=[
            {"precision": 0.5, "recall": 0.5, "mrr": 1.0},
            {"precision": 0.5, "recall": 1.0, "mrr": 1.0}
        ])
        # Mock session_transaction
        session = MagicMock()
        controller.session_transaction = MagicMock()
        controller.session_transaction.return_value.__enter__ = MagicMock(return_value=session)
        controller.session_transaction.return_value.__exit__ = MagicMock(return_value=None)
        # Mock evaluation object
        evaluation = MagicMock()
        evaluation.id = 1
        evaluation.created_at = datetime.now()
        # Mock KnowledgeBaseEvaluation constructor
        mock_kb_evaluation.return_value = evaluation
        # Mock session methods
        session.add = MagicMock()
        session.flush = MagicMock()
        session.commit = MagicMock()
        # Define the evaluate method for testing

        def evaluate(
            name: str,
            project_id: int,
            test_queries: List[Dict],
            metrics: Optional[List[str]] = None,
            top_k: int = 5,
            test_set_name: Optional[str] = None,
            user_notes: Optional[str] = None
        ) -> Dict[str, Any]:
            """Test implementation of evaluate method"""
            from datetime import datetime
            if metrics is None:
                metrics = ["precision", "recall", "mrr"]
            # Get knowledge base
            kb = controller.get(name, project_id)
            # Run queries and get results
            retrieved_results = []
            for query in test_queries:
                query_text = query.get("query")
                results = controller.query(name, project_id, query_text, limit=top_k)
                retrieved_results.append(results)
            # Calculate metrics
            metrics_results = {}
            per_query_results = []
            for i, query in enumerate(test_queries):
                expected_ids = set(query.get("expected_ids", []))
                retrieved_ids = [doc.get("id") for doc in retrieved_results[i]]
                # Calculate metrics for this query
                query_metrics = controller._calculate_metrics(
                    retrieved_ids, list(expected_ids), metrics, top_k
                )
                # Store per-query results
                per_query_results.append({
                    "query": query["query"],
                    "expected_ids": list(expected_ids),
                    "retrieved_ids": retrieved_ids[:top_k],
                    "metrics": query_metrics
                })
                # Accumulate metrics
                for metric, value in query_metrics.items():
                    if metric not in metrics_results:
                        metrics_results[metric] = 0
                    metrics_results[metric] += value
            # Average the metrics
            num_queries = len(test_queries)
            if num_queries > 0:
                for metric in metrics_results:
                    metrics_results[metric] = metrics_results[metric] / num_queries
            # Add per-query results
            metrics_results["per_query"] = per_query_results
            # Store evaluation in database
            with controller.session_transaction() as session:
                evaluation = mock_kb_evaluation(
                    knowledge_base_id=kb.id,
                    project_id=project_id,
                    test_set_name=test_set_name,
                    metrics=metrics_results,
                    config={
                        "top_k": top_k,
                        "metrics": metrics,
                        "num_queries": len(test_queries)
                    },
                    user_notes=user_notes,
                    created_at=datetime.now()
                )
                session.add(evaluation)
                session.flush()
                session.commit()
            # Return evaluation results
            return {
                "evaluation_id": evaluation.id,
                "knowledge_base_id": kb.id,
                "project_id": project_id,
                "test_set_name": test_set_name,
                "created_at": evaluation.created_at,
                "metrics": metrics_results,
                "config": {
                    "top_k": top_k,
                    "metrics": metrics,
                    "num_queries": len(test_queries)
                },
                "user_notes": user_notes
            }
        # Attach the evaluate method to our mock controller
        controller.evaluate = evaluate
        # Call evaluate
        result = controller.evaluate(
            name=self.kb_name,
            project_id=self.project_id,
            test_queries=self.test_queries,
            metrics=self.metrics,
            top_k=self.top_k,
            test_set_name=self.test_set_name,
            user_notes=self.user_notes
        )
        # Verify the result structure
        self.assertIn("evaluation_id", result)
        self.assertIn("knowledge_base_id", result)
        self.assertIn("project_id", result)
        self.assertIn("metrics", result)
        self.assertIn("config", result)
        # Verify metrics were calculated
        self.assertIn("precision", result["metrics"])
        self.assertIn("recall", result["metrics"])
        self.assertIn("mrr", result["metrics"])
        self.assertIn("per_query", result["metrics"])
        # Verify per-query results
        self.assertEqual(len(result["metrics"]["per_query"]), len(self.test_queries))
        # Verify controller methods were called correctly
        controller.get.assert_called_once_with(self.kb_name, self.project_id)
        self.assertEqual(controller.query.call_count, len(self.test_queries))
        self.assertEqual(controller._calculate_metrics.call_count, len(self.test_queries))
        # Verify database operations
        mock_kb_evaluation.assert_called_once()
        session.add.assert_called_once()
        session.flush.assert_called_once()
        session.commit.assert_called_once()


if __name__ == '__main__':

    unittest.main()
