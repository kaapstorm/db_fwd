"""Tests for database operations."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError

from db_fwd import execute_query, DatabaseLogger


@patch('db_fwd.create_engine')
def test_execute_query_success(mock_create_engine):
    """Test successful query execution."""
    # Setup mock
    mock_engine = Mock()
    mock_conn = Mock()
    mock_result = Mock()
    mock_row = ('{"test": "data"}',)

    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value = mock_result
    mock_result.fetchone.return_value = mock_row

    # Execute
    result = execute_query('postgresql://localhost/test', 'SELECT data;', [])

    # Verify
    assert result == '{"test": "data"}'
    mock_conn.execute.assert_called_once()


@patch('db_fwd.create_engine')
def test_execute_query_with_params(mock_create_engine):
    """Test query execution with parameters."""
    # Setup mock
    mock_engine = Mock()
    mock_conn = Mock()
    mock_result = Mock()
    mock_row = ('{"period": "2024Q1"}',)

    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value = mock_result
    mock_result.fetchone.return_value = mock_row

    # Execute
    result = execute_query(
        'postgresql://localhost/test',
        "SELECT data WHERE period = :param1;",
        ['2024Q1']
    )

    # Verify
    assert result == '{"period": "2024Q1"}'
    # Verify parameterized query was used
    call_args = mock_conn.execute.call_args
    assert call_args[0][1] == {'param1': '2024Q1'}


@patch('db_fwd.create_engine')
def test_execute_query_no_results(mock_create_engine):
    """Test query that returns no results."""
    # Setup mock
    mock_engine = Mock()
    mock_conn = Mock()
    mock_result = Mock()

    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value = mock_result
    mock_result.fetchone.return_value = None

    # Execute and verify
    with pytest.raises(ValueError, match="Query returned no results"):
        execute_query('postgresql://localhost/test', 'SELECT data;', [])


@patch('db_fwd.create_engine')
def test_execute_query_multiple_fields(mock_create_engine):
    """Test query that returns multiple fields."""
    # Setup mock
    mock_engine = Mock()
    mock_conn = Mock()
    mock_result = Mock()
    mock_row = ('field1', 'field2')

    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value = mock_result
    mock_result.fetchone.return_value = mock_row

    # Execute and verify
    with pytest.raises(ValueError, match="Query must return exactly one field"):
        execute_query('postgresql://localhost/test', 'SELECT data, extra;', [])


@patch('db_fwd.create_engine')
def test_execute_query_database_error(mock_create_engine):
    """Test database error during query execution."""
    # Setup mock
    mock_engine = Mock()
    mock_conn = Mock()

    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.side_effect = SQLAlchemyError("Connection failed")

    # Execute and verify
    with pytest.raises(SQLAlchemyError):
        execute_query('postgresql://localhost/test', 'SELECT data;', [])


@patch('db_fwd.create_engine')
def test_database_logger_init_no_url(mock_create_engine):
    """Test DatabaseLogger without URL."""
    logger = DatabaseLogger(None)
    assert logger.engine is None
    mock_create_engine.assert_not_called()


@patch('db_fwd.create_engine')
def test_database_logger_init_with_url(mock_create_engine):
    """Test DatabaseLogger with URL."""
    mock_engine = Mock()
    mock_conn = Mock()

    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    logger = DatabaseLogger('postgresql://localhost/logs')

    assert logger.engine is not None
    mock_create_engine.assert_called_once()
    mock_conn.execute.assert_called_once()  # CREATE TABLE


@patch('db_fwd.create_engine')
def test_database_logger_log(mock_create_engine):
    """Test logging to database."""
    mock_engine = Mock()
    mock_conn = Mock()

    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    logger = DatabaseLogger('postgresql://localhost/logs')
    logger.log('INFO', 'Test message')

    # Should be called twice: once for CREATE TABLE, once for INSERT
    assert mock_conn.execute.call_count == 2


@patch('db_fwd.create_engine')
def test_database_logger_log_no_engine(mock_create_engine):
    """Test logging without engine doesn't fail."""
    logger = DatabaseLogger(None)
    logger.log('INFO', 'Test message')  # Should not raise


@patch('db_fwd.create_engine')
def test_database_logger_log_error(mock_create_engine):
    """Test that logging errors don't fail the operation."""
    mock_engine = Mock()
    mock_conn = Mock()

    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.side_effect = [None, SQLAlchemyError("Log insert failed")]

    logger = DatabaseLogger('postgresql://localhost/logs')

    # This should not raise an exception
    logger.log('INFO', 'Test message')


@patch('db_fwd.create_engine')
def test_execute_query_sql_injection_safe(mock_create_engine):
    """Test that parameterized queries prevent SQL injection."""
    # Setup mock
    mock_engine = Mock()
    mock_conn = Mock()
    mock_result = Mock()
    mock_row = ('{"data": "safe"}',)

    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value = mock_result
    mock_result.fetchone.return_value = mock_row

    # Try to inject SQL via parameter (should be safely escaped)
    malicious_param = "'; DROP TABLE users; --"

    result = execute_query(
        'postgresql://localhost/test',
        "SELECT data WHERE id = :param1;",
        [malicious_param]
    )

    # Verify the parameter was passed safely, not interpolated into the query
    call_args = mock_conn.execute.call_args
    # First argument should be the text object with the query
    assert ":param1" in str(call_args[0][0])
    # Second argument should be the params dict with the malicious string safely contained
    assert call_args[0][1] == {'param1': "'; DROP TABLE users; --"}
    assert result == '{"data": "safe"}'


@patch('db_fwd.create_engine')
def test_execute_query_multiple_params(mock_create_engine):
    """Test query execution with multiple parameters."""
    # Setup mock
    mock_engine = Mock()
    mock_conn = Mock()
    mock_result = Mock()
    mock_row = ('{"result": "success"}',)

    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value = mock_result
    mock_result.fetchone.return_value = mock_row

    # Execute with multiple parameters
    result = execute_query(
        'postgresql://localhost/test',
        "SELECT data WHERE category = :param1 AND period = :param2 AND status = :param3;",
        ['category1', '2024Q1', 'active']
    )

    # Verify all parameters were passed correctly
    call_args = mock_conn.execute.call_args
    assert call_args[0][1] == {
        'param1': 'category1',
        'param2': '2024Q1',
        'param3': 'active'
    }
    assert result == '{"result": "success"}'
