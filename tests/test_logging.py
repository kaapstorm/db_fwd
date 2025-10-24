"""Tests for logging functionality."""

import logging
import tempfile
from pathlib import Path

import pytest

from db_fwd import set_up_logging


def test_setup_logging_info_level():
    """Test setting up logging with info level."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        log_file = f.name

    try:
        set_up_logging('info', log_file)

        # Test that info messages are logged
        logging.info("Test info message")

        # Read log file
        with open(log_file, 'r') as f:
            content = f.read()

        assert "Test info message" in content
        assert "INFO" in content
    finally:
        Path(log_file).unlink()
        # Reset logging
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)


def test_setup_logging_debug_level():
    """Test setting up logging with debug level."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        log_file = f.name

    try:
        set_up_logging('debug', log_file)

        # Test that debug messages are logged
        logging.debug("Test debug message")
        logging.info("Test info message")

        # Read log file
        with open(log_file, 'r') as f:
            content = f.read()

        assert "Test debug message" in content
        assert "DEBUG" in content
        assert "Test info message" in content
    finally:
        Path(log_file).unlink()
        # Reset logging
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)


def test_setup_logging_none_level():
    """Test setting up logging with none level."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        log_file = f.name

    try:
        set_up_logging('none', log_file)

        # Test that messages are not logged
        logging.critical("Test critical message")
        logging.error("Test error message")
        logging.info("Test info message")

        # Read log file
        with open(log_file, 'r') as f:
            content = f.read()

        # Only critical+ messages should be logged, but we set it higher than critical
        assert "Test info message" not in content
        assert "Test error message" not in content
    finally:
        Path(log_file).unlink()
        # Reset logging
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)


def test_setup_logging_creates_file():
    """Test that setup_logging creates the log file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / 'new_log.log'
        assert not log_file.exists()

        set_up_logging('info', str(log_file))

        # Log something to trigger file creation
        logging.info("Test message")

        assert log_file.exists()

        # Reset logging
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)


def test_setup_logging_format():
    """Test that log messages have the correct format."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        log_file = f.name

    try:
        set_up_logging('info', log_file)

        logging.info("Test message")

        # Read log file
        with open(log_file, 'r') as f:
            content = f.read()

        # Check format: timestamp - level - message
        assert " - INFO - Test message" in content
        # Should have a timestamp (check for date pattern)
        import re
        assert re.search(r'\d{4}-\d{2}-\d{2}', content)
    finally:
        Path(log_file).unlink()
        # Reset logging
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)


def test_setup_logging_invalid_level():
    """Test setup_logging with invalid level defaults to info."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        log_file = f.name

    try:
        set_up_logging('invalid', log_file)

        # Should default to info level
        logging.info("Test info message")
        logging.debug("Test debug message")

        # Read log file
        with open(log_file, 'r') as f:
            content = f.read()

        assert "Test info message" in content
        # Debug should not be logged at info level
        assert "Test debug message" not in content
    finally:
        Path(log_file).unlink()
        # Reset logging
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
