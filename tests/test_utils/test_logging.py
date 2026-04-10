"""Tests for the structured logging module."""

from __future__ import annotations

import logging

from src.utils.logging import (
    DevFormatter,
    StructuredFormatter,
    request_id_var,
    setup_logging,
)


def test_setup_logging_dev():
    setup_logging(level="debug", dev_mode=True)
    root = logging.getLogger()
    assert root.level == logging.DEBUG
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, DevFormatter)


def test_setup_logging_prod():
    setup_logging(level="info", dev_mode=False)
    root = logging.getLogger()
    assert isinstance(root.handlers[0].formatter, StructuredFormatter)


def test_request_id_var_default():
    assert request_id_var.get() == "-"


def test_request_id_var_set():
    token = request_id_var.set("req-123")
    assert request_id_var.get() == "req-123"
    request_id_var.reset(token)


def test_dev_formatter_output():
    fmt = DevFormatter()
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="hello %s", args=("world",), exc_info=None,
    )
    output = fmt.format(record)
    assert "hello world" in output


def test_structured_formatter_output():
    fmt = StructuredFormatter()
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="test message", args=(), exc_info=None,
    )
    output = fmt.format(record)
    assert '"msg": "test message"' in output
    assert '"level": "INFO"' in output
