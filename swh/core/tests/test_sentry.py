# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import pytest
import sentry_sdk
from sentry_sdk import capture_exception, capture_message, set_tag

from swh.core.sentry import (
    init_sentry,
    override_with_bool_envvar,
    override_with_float_envvar,
)

SENTRY_DSN = "https://user@example.org/1234"


@pytest.mark.parametrize(
    "envvalue,retval",
    (
        ("y", True),
        ("n", False),
        ("0", False),
        ("true", True),
        ("FaLsE", False),
        ("1", True),
    ),
)
def test_override_with_bool_envvar(monkeypatch, envvalue: str, retval: bool):
    """Test if the override_with_bool_envvar function returns appropriate results"""
    envvar = "OVERRIDE_WITH_BOOL_ENVVAR"
    monkeypatch.setenv(envvar, envvalue)
    for default in (True, False):
        assert override_with_bool_envvar(envvar, default) == retval


def test_override_with_bool_envvar_logging(monkeypatch, caplog):
    envvar = "OVERRIDE_WITH_BOOL_ENVVAR"
    monkeypatch.setenv(envvar, "not a boolean env value")
    for default in (True, False):
        caplog.clear()
        assert override_with_bool_envvar(envvar, default) == default
        assert len(caplog.records) == 1
        assert (
            "OVERRIDE_WITH_BOOL_ENVVAR='not a boolean env value'"
            in caplog.records[0].getMessage()
        )
        assert f"using default value {default}" in caplog.records[0].getMessage()
        assert caplog.records[0].levelname == "WARNING"


@pytest.mark.parametrize(
    "envvalue,retval",
    (("1.0", 1.0), ("0.0", 0.0), ("0", 0.0), ("a", None)),
)
def test_override_with_float_envvar(monkeypatch, envvalue: str, retval: bool):
    envvar = "OVERRIDE_WITH_FLOAT_ENVVAR"
    monkeypatch.setenv(envvar, envvalue)
    assert override_with_float_envvar(envvar, None) == retval


def test_override_with_float_envvar_logging(monkeypatch, caplog):
    envvar = "OVERRIDE_WITH_FLOAT_ENVVAR"
    monkeypatch.setenv(envvar, "not a float env value")
    for default in (None, 1.0):
        caplog.clear()
        assert override_with_float_envvar(envvar, default) == default
        assert len(caplog.records) == 1
        assert (
            "OVERRIDE_WITH_FLOAT_ENVVAR='not a float env value'"
            in caplog.records[0].getMessage()
        )
        assert f"using default value {default}" in caplog.records[0].getMessage()
        assert caplog.records[0].levelname == "WARNING"


def test_sentry():
    reports = []
    init_sentry(SENTRY_DSN, extra_kwargs={"transport": reports.append})

    capture_message("Something went wrong")
    logging.error("Stupid error")

    assert len(reports) == 2
    assert reports[0]["message"] == "Something went wrong"
    assert reports[1]["logentry"]["message"] == "Stupid error"


def test_sentry_no_logging():
    reports = []
    init_sentry(
        SENTRY_DSN,
        disable_logging_events=True,
        extra_kwargs={"transport": reports.append},
    )

    capture_message("Something went wrong")
    logging.error("Stupid error")

    assert len(reports) == 1
    assert reports[0]["message"] == "Something went wrong"


def test_sentry_no_logging_from_venv(monkeypatch):
    monkeypatch.setenv("SWH_SENTRY_DISABLE_LOGGING_EVENTS", "True")

    reports = []
    init_sentry(
        SENTRY_DSN,
        extra_kwargs={"transport": reports.append},
    )

    capture_message("Something went wrong")
    logging.error("Stupid error")

    assert len(reports) == 1
    assert reports[0]["message"] == "Something went wrong"


def test_sentry_logging_from_venv(monkeypatch):
    monkeypatch.setenv("SWH_SENTRY_DISABLE_LOGGING_EVENTS", "false")

    reports = []
    init_sentry(
        SENTRY_DSN,
        extra_kwargs={"transport": reports.append},
    )

    capture_message("Something went wrong")
    logging.error("Stupid error")

    assert len(reports) == 2


def test_sentry_events_fixture_capture_message(sentry_events):
    message = "Something went wrong"
    capture_message(message)
    assert sentry_events
    assert "message" in sentry_events[0]
    assert sentry_events[0]["message"] == message


def test_sentry_events_fixture_capture_exception(sentry_events):
    message = "Invalid value"
    exception = ValueError(message)
    capture_exception(exception)
    assert sentry_events
    assert "exception" in sentry_events[0]
    assert "values" in sentry_events[0]["exception"]
    exception_data = sentry_events[0]["exception"]["values"]
    assert exception_data
    assert exception_data[0].get("type") == type(exception).__name__
    assert exception_data[0].get("value") == message


def test_sentry_events_fixture_set_tag(sentry_events):
    tag_name = "swh.test"
    tag_value = "test"
    set_tag(tag_name, tag_value)
    message = "Something went wrong"
    capture_message(message)
    assert sentry_events
    assert "tags" in sentry_events[0]
    sentry_events[0]["tags"] == {tag_name: tag_value}


def test_sentry_main_package(mocker):
    sentry_sdk_init = mocker.patch.object(sentry_sdk, "init")
    init_sentry(
        SENTRY_DSN,
    )
    assert sentry_sdk_init.call_args_list[-1][1]["release"] is None

    init_sentry(SENTRY_DSN, main_package="swh.core")
    assert sentry_sdk_init.call_args_list[-1][1]["release"].startswith("swh.core@")


@pytest.mark.parametrize("deferred_init", [False, True])
def test_sentry_deferred_init(caplog, deferred_init):
    init_sentry(None, deferred_init=deferred_init)
    if not deferred_init:
        assert caplog.records
        assert (
            caplog.records[0].message
            == "Sentry DSN not provided, events will not be sent."
        )
    else:
        assert not caplog.records


@pytest.mark.parametrize("traces_sample_rate", [1.0, 0.0, None])
def test_sentry_traces_sample_rate(caplog, traces_sample_rate):
    init_sentry(None, traces_sample_rate=traces_sample_rate)
    client = sentry_sdk.get_client()
    if traces_sample_rate is not None:
        assert client.options["traces_sample_rate"] == traces_sample_rate
    else:
        assert client.options["enable_tracing"] is None
        assert client.options["traces_sample_rate"] is None
