import pytest
from unittest.mock import MagicMock, patch

from mindsdb.interfaces.skills.sql_agent import SQLAgent


@pytest.fixture
def sql_agent_setup():
    command_executor = MagicMock()
    cache_mock = MagicMock()
    sql_agent = SQLAgent(command_executor=command_executor, database='test_db', cache=cache_mock)
    return sql_agent, cache_mock


def test_get_table_info_cache_miss(sql_agent_setup):
    sql_agent, cache_mock = sql_agent_setup
    cache_mock.get.return_value = None
    with patch.object(SQLAgent, '_fetch_table_info') as mock_fetch_table_info:
        sql_agent.get_table_info(['test_table'])
        mock_fetch_table_info.assert_called_once()


def test_get_table_info_cache_hit(sql_agent_setup):
    sql_agent, cache_mock = sql_agent_setup
    cache_mock.get.return_value = {'test_table': 'table_info'}
    with patch.object(SQLAgent, '_fetch_table_info') as mock_fetch_table_info:
        sql_agent.get_table_info(['test_table'])
        assert not mock_fetch_table_info.called
