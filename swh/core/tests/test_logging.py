# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os

import pytest
from yaml import safe_load

from swh.core.logging import BadLogLevel, logging_configure, validate_loglevel


def test_logging_configure_default():
    """Logging should be configured to INFO by default"""
    root_log_level = logging_configure()

    assert root_log_level == "INFO"


def test_logging_configure_with_override():
    """Logging module should be configured according to log_levels provided."""
    log_levels = [
        ("swh.core.tests", logging.DEBUG),
        ("swh.core.api", logging.CRITICAL),
        ("swh.core.db", logging.ERROR),
        ("swh.loader.core.tests", logging.ERROR),
        ("swh.loader.core", logging.WARNING),
    ]
    for module, expected_log_level in log_levels:
        logger = logging.getLogger(module)
        assert logger.getEffectiveLevel() != expected_log_level

    # Set it up
    root_log_level = logging_configure(log_levels)
    assert root_log_level == "INFO"

    for module, expected_log_level in log_levels:
        logger = logging.getLogger(module)
        assert logger.getEffectiveLevel() == expected_log_level


def test_logging_configure_from_yaml(datadir):
    """Logging should be configurable from yaml configuration file."""
    logging_config = os.path.join(datadir, "logging-config.yaml")
    root_log_level = logging_configure([], logging_config)

    with open(logging_config, "r") as f:
        config = safe_load(f.read())

    for module, logger_config in config["loggers"].items():
        if not logger_config:
            continue
        log_level = logger_config["level"]

        logger = logging.getLogger(module)
        assert logger.getEffectiveLevel() == logging.getLevelName(log_level)

    assert root_log_level == config["root"]["level"]


@pytest.mark.parametrize(
    "log_level,expected_module,expected_log_level",
    [
        ("swh.core:DEBUG", "swh.core", logging.DEBUG),
        ("swh.core:debug", "swh.core", logging.DEBUG),
        ("swh.core.api:info", "swh.core.api", logging.INFO),
    ],
)
def test_validate_loglevel_ok(log_level, expected_module, expected_log_level):
    """Correct log level should pass validation"""
    module, level = validate_loglevel(log_level)

    assert module == expected_module
    assert level == expected_log_level


def test_validate_loglevel_raise():
    """Unsupported log level should raise"""

    with pytest.raises(BadLogLevel, match="unknown"):
        validate_loglevel("inexistent")

    with pytest.raises(BadLogLevel, match="unknown"):
        validate_loglevel("swh.core:something-wrong")
