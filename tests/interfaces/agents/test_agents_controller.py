from unittest.mock import patch

import pandas as pd

from mindsdb.interfaces.storage import db
from mindsdb.interfaces.agents.agents_controller import AgentsController


@patch('mindsdb.api.executor.datahub.datanodes.project_datanode.ProjectDataNode')
@patch('mindsdb.api.executor.datahub.datahub.InformationSchemaDataNode')
@patch('mindsdb.interfaces.model.model_controller.ModelController')
def test_get_completion(mock_model_controller, mock_schema_datanode, mock_project_datanode):
    mock_project_datanode_instance = mock_project_datanode.return_value
    mock_datanode_instance = mock_schema_datanode.return_value
    mock_datanode_instance.get.return_value = mock_project_datanode_instance
    mock_model_controller_instance = mock_model_controller.return_value
    mock_model_controller_instance.get_model.return_value = {
        'model_name': 'test_model',
        'predict': 'answer',
        'problem_definition': {
            'using':
                {
                    'prompt_template': 'What is the meaning of life?'
                }
        }
    }
    agents_controller = AgentsController(
        mock_datanode_instance,
        model_controller=mock_model_controller_instance)

    completion_response = {'answer': '42'}
    df = pd.DataFrame.from_records([completion_response])
    mock_project_datanode_instance.predict.return_value = df
    agent = db.Agents()
    agent.model_name = 'test_model'
    agent.provider = 'mindsdb'
    agent.params = {}
    messages = [{'question': 'What is the meaning of life?', 'answer': None}]
    completion_df = agents_controller.get_completion(agent, messages)

    assert not completion_df.empty
    assert 'answer' in completion_df.columns
    assert completion_df['answer'].loc[0] == '42'
