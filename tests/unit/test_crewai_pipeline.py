import unittest
from unittest.mock import patch, MagicMock

from mindsdb.interfaces.agents.crewai_pipeline import CrewAITextToSQLPipeline
from mindsdb.interfaces.agents.agents_controller import AgentsController


class TestCrewAIPipeline(unittest.TestCase):
    @patch("mindsdb.interfaces.agents.crewai_pipeline.ChatOpenAI")
    def test_crewai_pipeline_initialization(self, mock_chat):
        """Test that the CrewAI pipeline can be initialized."""
        mock_chat.return_value = MagicMock()

        pipeline = CrewAITextToSQLPipeline(
            tables=["db1.table1", "db2.table2"],
            knowledge_bases=["kb1", "kb2"],
            model="gpt-4",
            api_key="test-key",
            verbose=True,
            max_tokens=100,
        )

        # Check that the pipeline components were initialized
        self.assertIsNotNone(pipeline.query_understanding_agent)
        self.assertIsNotNone(pipeline.sql_generation_agent)
        self.assertIsNotNone(pipeline.sql_execution_agent)
        self.assertIsNotNone(pipeline.sql_validation_agent)

    @patch("mindsdb.interfaces.agents.crewai_pipeline.ChatOpenAI")
    @patch("mindsdb.interfaces.agents.crewai_pipeline.Crew")
    def test_process_query(self, mock_crew, mock_chat):
        """Test that the process_query method works as expected."""
        mock_chat.return_value = MagicMock()
        mock_crew_instance = MagicMock()
        mock_crew_instance.kickoff.return_value = "Sample crew result"
        mock_crew.return_value = mock_crew_instance

        pipeline = CrewAITextToSQLPipeline(
            tables=["db1.table1"], knowledge_bases=["kb1"], model="gpt-4", api_key="test-key"
        )

        result = pipeline.process_query("What is the average sales for 2021?")

        self.assertTrue(mock_crew.called)
        self.assertTrue(mock_crew_instance.kickoff.called)
        self.assertEqual(result["user_query"], "What is the average sales for 2021?")
        self.assertIsInstance(result["result"], dict)
        self.assertEqual(result["result"].get("text"), "Sample crew result")

    @patch("mindsdb.interfaces.database.projects.ProjectController")
    @patch("mindsdb.interfaces.skills.skills_controller.SkillsController")
    @patch("mindsdb.interfaces.model.model_controller.ModelController")
    def test_create_crewai_agents(self, mock_model_controller, mock_skills_controller, mock_project_controller):
        """Test the create_crewai_agents method in AgentsController."""
        # Set up the mocks
        mock_project = MagicMock()
        mock_project_controller.return_value.get.return_value = mock_project

        # Create the controller
        agents_controller = AgentsController(
            project_controller=mock_project_controller(),
            skills_controller=mock_skills_controller(),
            model_controller=mock_model_controller(),
        )

        # Create a CrewAI agent
        agent = agents_controller.create_crewai_agents(
            name="crewai_agent",
            project_name="default",
            model_name="gpt-4o",
            tables=["db1.table1", "db2.table2"],
            knowledge_bases=["kb1"],
            provider="openai",
            params={"verbose": True, "max_tokens": 500},
        )

        # Verify agent properties
        self.assertEqual(agent.name, "crewai_agent")
        self.assertEqual(agent.model_name, "gpt-4o")
        self.assertEqual(agent.provider, "openai")
        self.assertEqual(agent.params.get("agent_type"), "crewai")
        self.assertEqual(agent.params.get("tables"), ["db1.table1", "db2.table2"])
        self.assertEqual(agent.params.get("knowledge_bases"), ["kb1"])


if __name__ == "__main__":
    unittest.main()
