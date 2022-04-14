# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from sentry_sdk import capture_message

from swh.core.sentry import init_sentry


def test_sentry():
    reports = []
    init_sentry("http://example.org", extra_kwargs={"transport": reports.append})

    capture_message("Something went wrong")
    logging.error("Stupid error")

    assert len(reports) == 2
    assert reports[0]["message"] == "Something went wrong"
    assert reports[1]["logentry"]["message"] == "Stupid error"


def test_sentry_no_logging():
    reports = []
    init_sentry(
        "http://example.org",
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
        "http://example.org",
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
        "http://example.org",
        extra_kwargs={"transport": reports.append},
    )

    capture_message("Something went wrong")
    logging.error("Stupid error")

    assert len(reports) == 2
