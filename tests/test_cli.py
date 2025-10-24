"""Tests for CLI interface."""

import pytest
from unittest.mock import patch, Mock
import sys

from db_fwd import parse_args, main


def test_parse_args_minimal():
    """Test parsing minimal arguments."""
    with patch('sys.argv', ['db_fwd.py', 'query_name']):
        args = parse_args()
        assert args.query_name == 'query_name'
        assert args.query_params == []
        assert args.log_level is None
        assert args.log_file is None
        assert args.config_file == 'db_fwd.toml'


def test_parse_args_with_params():
    """Test parsing with query parameters."""
    with patch('sys.argv', ['db_fwd.py', 'query_name', 'param1', 'param2']):
        args = parse_args()
        assert args.query_name == 'query_name'
        assert args.query_params == ['param1', 'param2']


def test_parse_args_with_log_level():
    """Test parsing with log level."""
    with patch('sys.argv', ['db_fwd.py', '--log-level', 'debug', 'query_name']):
        args = parse_args()
        assert args.log_level == 'debug'


def test_parse_args_with_log_file():
    """Test parsing with log file."""
    with patch('sys.argv', ['db_fwd.py', '--log-file', 'custom.log', 'query_name']):
        args = parse_args()
        assert args.log_file == 'custom.log'


def test_parse_args_with_config_file():
    """Test parsing with config file."""
    with patch('sys.argv', ['db_fwd.py', '--config-file', 'custom.toml', 'query_name']):
        args = parse_args()
        assert args.config_file == 'custom.toml'


def test_parse_args_all_options():
    """Test parsing with all options."""
    test_argv = [
        'db_fwd.py',
        '--log-level', 'info',
        '--log-file', 'test.log',
        '--config-file', 'test.toml',
        'my_query',
        'param1',
        'param2'
    ]

    with patch('sys.argv', test_argv):
        args = parse_args()
        assert args.log_level == 'info'
        assert args.log_file == 'test.log'
        assert args.config_file == 'test.toml'
        assert args.query_name == 'my_query'
        assert args.query_params == ['param1', 'param2']


@patch('db_fwd.forward_to_api')
@patch('db_fwd.execute_query')
@patch('db_fwd.DatabaseLogger')
@patch('db_fwd.set_up_logging')
@patch('db_fwd.Config')
def test_main_success(mock_config_class, mock_setup_logging, mock_db_logger_class,
                      mock_execute_query, mock_forward_to_api):
    """Test successful main execution."""
    # Setup mocks
    mock_config = Mock()
    mock_config.get_log_level.return_value = 'info'
    mock_config.get_log_file.return_value = 'test.log'
    mock_config.get_log_db_url.return_value = None
    mock_config.get_db_url.return_value = 'postgresql://localhost/test'
    mock_config.get_query.return_value = "SELECT data FROM test;"
    mock_config.get_api_url.return_value = 'https://example.com/api'
    mock_config.get_api_credentials.return_value = ('user', 'pass')
    mock_config_class.return_value = mock_config

    mock_db_logger = Mock()
    mock_db_logger_class.return_value = mock_db_logger

    mock_execute_query.return_value = '{"test": "data"}'

    # Execute
    with patch('sys.argv', ['db_fwd.py', 'test_query']):
        main()

    # Verify
    mock_config_class.assert_called_once_with('db_fwd.toml')
    mock_setup_logging.assert_called_once_with('info', 'test.log')
    mock_execute_query.assert_called_once()
    mock_forward_to_api.assert_called_once()


@patch('db_fwd.Config')
def test_main_config_file_not_found(mock_config_class):
    """Test main with missing config file."""
    mock_config_class.side_effect = FileNotFoundError("Config not found")

    with patch('sys.argv', ['db_fwd.py', 'test_query']):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1


@patch('db_fwd.execute_query')
@patch('db_fwd.DatabaseLogger')
@patch('db_fwd.set_up_logging')
@patch('db_fwd.Config')
def test_main_query_error(mock_config_class, mock_setup_logging,
                          mock_db_logger_class, mock_execute_query):
    """Test main with query execution error."""
    # Setup mocks
    mock_config = Mock()
    mock_config.get_log_level.return_value = 'info'
    mock_config.get_log_file.return_value = 'test.log'
    mock_config.get_log_db_url.return_value = None
    mock_config.get_db_url.return_value = 'postgresql://localhost/test'
    mock_config.get_query.return_value = "SELECT data FROM test;"
    mock_config.get_api_url.return_value = 'https://example.com/api'
    mock_config.get_api_credentials.return_value = ('user', 'pass')
    mock_config_class.return_value = mock_config

    mock_db_logger = Mock()
    mock_db_logger_class.return_value = mock_db_logger

    mock_execute_query.side_effect = ValueError("Query failed")

    # Execute
    with patch('sys.argv', ['db_fwd.py', 'test_query']):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1


@patch('db_fwd.forward_to_api')
@patch('db_fwd.execute_query')
@patch('db_fwd.DatabaseLogger')
@patch('db_fwd.set_up_logging')
@patch('db_fwd.Config')
def test_main_api_error(mock_config_class, mock_setup_logging,
                        mock_db_logger_class, mock_execute_query,
                        mock_forward_to_api):
    """Test main with API forwarding error."""
    # Setup mocks
    mock_config = Mock()
    mock_config.get_log_level.return_value = 'info'
    mock_config.get_log_file.return_value = 'test.log'
    mock_config.get_log_db_url.return_value = None
    mock_config.get_db_url.return_value = 'postgresql://localhost/test'
    mock_config.get_query.return_value = "SELECT data FROM test;"
    mock_config.get_api_url.return_value = 'https://example.com/api'
    mock_config.get_api_credentials.return_value = ('user', 'pass')
    mock_config_class.return_value = mock_config

    mock_db_logger = Mock()
    mock_db_logger_class.return_value = mock_db_logger

    mock_execute_query.return_value = '{"test": "data"}'
    mock_forward_to_api.side_effect = Exception("API failed")

    # Execute
    with patch('sys.argv', ['db_fwd.py', 'test_query']):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1


@patch('db_fwd.forward_to_api')
@patch('db_fwd.execute_query')
@patch('db_fwd.DatabaseLogger')
@patch('db_fwd.set_up_logging')
@patch('db_fwd.Config')
def test_main_with_query_params(mock_config_class, mock_setup_logging,
                                 mock_db_logger_class, mock_execute_query,
                                 mock_forward_to_api):
    """Test main with query parameters."""
    # Setup mocks
    mock_config = Mock()
    mock_config.get_log_level.return_value = 'info'
    mock_config.get_log_file.return_value = 'test.log'
    mock_config.get_log_db_url.return_value = None
    mock_config.get_db_url.return_value = 'postgresql://localhost/test'
    mock_config.get_query.return_value = "SELECT data WHERE period = :param1;"
    mock_config.get_api_url.return_value = 'https://example.com/api'
    mock_config.get_api_credentials.return_value = ('user', 'pass')
    mock_config_class.return_value = mock_config

    mock_db_logger = Mock()
    mock_db_logger_class.return_value = mock_db_logger

    mock_execute_query.return_value = '{"test": "data"}'

    # Execute
    with patch('sys.argv', ['db_fwd.py', 'test_query', '2024Q1']):
        main()

    # Verify query was called with parameters
    mock_execute_query.assert_called_once()
    call_args = mock_execute_query.call_args[0]
    assert call_args[2] == ['2024Q1']  # params argument


@patch('db_fwd.forward_to_api')
@patch('db_fwd.execute_query')
@patch('db_fwd.DatabaseLogger')
@patch('db_fwd.set_up_logging')
@patch('db_fwd.Config')
def test_main_with_cli_overrides(mock_config_class, mock_setup_logging,
                                  mock_db_logger_class, mock_execute_query,
                                  mock_forward_to_api):
    """Test main with CLI overrides for config values."""
    # Setup mocks
    mock_config = Mock()
    mock_config.get_log_level.return_value = 'info'
    mock_config.get_log_file.return_value = 'default.log'
    mock_config.get_log_db_url.return_value = None
    mock_config.get_db_url.return_value = 'postgresql://localhost/test'
    mock_config.get_query.return_value = "SELECT data FROM test;"
    mock_config.get_api_url.return_value = 'https://example.com/api'
    mock_config.get_api_credentials.return_value = ('user', 'pass')
    mock_config_class.return_value = mock_config

    mock_db_logger = Mock()
    mock_db_logger_class.return_value = mock_db_logger

    mock_execute_query.return_value = '{"test": "data"}'

    # Execute with CLI overrides
    with patch('sys.argv', ['db_fwd.py', '--log-level', 'debug',
                            '--log-file', 'custom.log', 'test_query']):
        main()

    # Verify CLI values override config
    mock_setup_logging.assert_called_once_with('debug', 'custom.log')
