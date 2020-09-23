# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
import inspect
import logging
from unittest.mock import patch

import pytz

from swh.core import logger


def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno


def test_db_level():
    assert logger.db_level_of_py_level(10) == "debug"
    assert logger.db_level_of_py_level(20) == "info"
    assert logger.db_level_of_py_level(30) == "warning"
    assert logger.db_level_of_py_level(40) == "error"
    assert logger.db_level_of_py_level(50) == "critical"


def test_flatten_scalar():
    assert list(logger.flatten("")) == [("", "")]
    assert list(logger.flatten("toto")) == [("", "toto")]

    assert list(logger.flatten(10)) == [("", 10)]
    assert list(logger.flatten(10.5)) == [("", 10.5)]


def test_flatten_list():
    assert list(logger.flatten([])) == []
    assert list(logger.flatten([1])) == [("0", 1)]

    assert list(logger.flatten([1, 2, ["a", "b"]])) == [
        ("0", 1),
        ("1", 2),
        ("2_0", "a"),
        ("2_1", "b"),
    ]

    assert list(logger.flatten([1, 2, ["a", ("x", 1)]])) == [
        ("0", 1),
        ("1", 2),
        ("2_0", "a"),
        ("2_1_0", "x"),
        ("2_1_1", 1),
    ]


def test_flatten_dict():
    assert list(logger.flatten({})) == []
    assert list(logger.flatten({"a": 1})) == [("a", 1)]

    assert sorted(logger.flatten({"a": 1, "b": (2, 3,), "c": {"d": 4, "e": "f"}})) == [
        ("a", 1),
        ("b_0", 2),
        ("b_1", 3),
        ("c_d", 4),
        ("c_e", "f"),
    ]


def test_flatten_dict_binary_keys():
    d = {b"a": "a"}
    str_d = str(d)
    assert list(logger.flatten(d)) == [("", str_d)]
    assert list(logger.flatten({"a": d})) == [("a", str_d)]
    assert list(logger.flatten({"a": [d, d]})) == [("a_0", str_d), ("a_1", str_d)]


def test_stringify():
    assert logger.stringify(None) == "None"
    assert logger.stringify(123) == "123"
    assert logger.stringify("abc") == "abc"

    date = datetime(2019, 9, 1, 16, 32)
    assert logger.stringify(date) == "2019-09-01T16:32:00"

    tzdate = datetime(2019, 9, 1, 16, 32, tzinfo=pytz.utc)
    assert logger.stringify(tzdate) == "2019-09-01T16:32:00+00:00"


@patch("swh.core.logger.send")
def test_journal_handler(send):
    log = logging.getLogger("test_logger")
    log.addHandler(logger.JournalHandler())
    log.setLevel(logging.DEBUG)

    _, ln = log.info("hello world"), lineno()

    send.assert_called_with(
        "hello world",
        CODE_FILE=__file__,
        CODE_FUNC="test_journal_handler",
        CODE_LINE=ln,
        LOGGER="test_logger",
        PRIORITY="6",
        THREAD_NAME="MainThread",
    )


@patch("swh.core.logger.send")
def test_journal_handler_w_data(send):
    log = logging.getLogger("test_logger")
    log.addHandler(logger.JournalHandler())
    log.setLevel(logging.DEBUG)

    _, ln = (
        log.debug("something cool %s", ["with", {"extra": "data"}]),
        lineno() - 1,
    )

    send.assert_called_with(
        "something cool ['with', {'extra': 'data'}]",
        CODE_FILE=__file__,
        CODE_FUNC="test_journal_handler_w_data",
        CODE_LINE=ln,
        LOGGER="test_logger",
        PRIORITY="7",
        THREAD_NAME="MainThread",
        SWH_LOGGING_ARGS_0_0="with",
        SWH_LOGGING_ARGS_0_1_EXTRA="data",
    )
