import os
from unittest.mock import patch, mock_open
import pytest
from mindsdb.integrations.handlers.langchain_handler.agent_tool_fetcher import AgentToolFetcher
from mindsdb.integrations.handlers.frappe_handler.frappe_handler import FrappeHandler


@pytest.fixture
def tool_fetcher():
    return AgentToolFetcher()


@pytest.fixture
def fake_os_path_exists():
    with patch('os.path.exists') as mock_exists:
        yield mock_exists


@pytest.fixture
def fake_open():
    with patch('builtins.open', mock_open(read_data='data')) as mock_file:
        yield mock_file


def test_get_token_path_success(tool_fetcher, fake_os_path_exists):
    """
    Test that get_token_path returns the expected path when the file exists
    """
    fake_os_path_exists.return_value = True
    base_dir = '/fake/base/dir'
    username = 'johndoe'
    token_file = 'token.txt'
    expected_path = os.path.join(base_dir, username, token_file)
    assert tool_fetcher.get_token_path(base_dir, username, token_file) == expected_path


def test_get_token_path_failure(tool_fetcher, fake_os_path_exists):
    """
    Test that get_token_path raises an AssertionError when the file does not exist
    """
    fake_os_path_exists.return_value = False
    with pytest.raises(AssertionError):
        tool_fetcher.get_token_path('/fake/base/dir', 'johndoe', 'token.txt')


def test_get_frappe_handler(tool_fetcher, fake_os_path_exists, fake_open):
    """
    Test that get_frappe_handler returns a FrappeHandler instance when the token files exists
    """
    fake_os_path_exists.return_value = True
    handler = tool_fetcher.get_frappe_handler('/fake/base/dir', 'johndoe')
    assert isinstance(handler, FrappeHandler)


def test_get_tools_for_agent_unknown(tool_fetcher):
    """
    Test that get_tools_for_agent returns an empty list when the agent name is unknown
    """
    tools = tool_fetcher.get_tools_for_agent('unknown', '/fake/base/dir', 'johndoe')
    assert tools == []


def test_get_tools_for_agent_gmail(tool_fetcher, fake_os_path_exists, fake_open):
    """
    Test that get_tools_for_agent returns the expected tools when the agent name is gmail
    """
    fake_os_path_exists.return_value = True
    with patch('mindsdb.integrations.handlers.langchain_handler.agent_tool_fetcher.GmailHandler') as mock_gmail_handler:
        instance = mock_gmail_handler.return_value
        instance.get_agent_tools.return_value = ['tool1', 'tool2']
        tools = tool_fetcher.get_tools_for_agent('gmail', '/fake/base/dir', 'johndoe')
        assert tools == ['tool1', 'tool2']


def test_get_tools_for_agent_frappe(tool_fetcher, fake_os_path_exists, fake_open):
    """
    Test that get_tools_for_agent returns the expected tools when the agent name is frappe
    """
    fake_os_path_exists.return_value = True
    with patch('mindsdb.integrations.handlers.langchain_handler.agent_tool_fetcher.FrappeHandler') as mock_frappe_handler:
        instance = mock_frappe_handler.return_value
        instance.get_agent_tools.return_value = ['tool1', 'tool2']
        tools = tool_fetcher.get_tools_for_agent('frappe', '/fake/base/dir', 'johndoe')
        assert tools == ['tool1', 'tool2']


def test_get_tools_for_agent_error_logging(tool_fetcher, caplog, fake_os_path_exists):
    """
    Test that get_tools_for_agent logs an error when an exception is raised
    """
    fake_os_path_exists.side_effect = Exception('Boom!')
    with patch('mindsdb.integrations.handlers.langchain_handler.agent_tool_fetcher.GmailHandler') as mock_gmail_handler:
        tools = tool_fetcher.get_tools_for_agent('gmail', '/fake/base/dir', 'johndoe')
        assert "Failed to get gmail tools" in caplog.text
        assert tools == []
