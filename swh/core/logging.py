# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Logging module providing common swh logging configuration. This only depends on
python core library.

"""

import logging
from logging.config import dictConfig
from pathlib import Path
from typing import List, Optional, Tuple

from yaml import safe_load


class BadLogLevel(ValueError):
    pass


def validate_loglevel(value: str) -> Tuple[Optional[str], int]:
    """Validate a single loglevel specification, of the form LOGLEVEL or
    module:LOGLEVEL."""

    LOG_LEVEL_NAMES = ["notset", "debug", "info", "warning", "error", "critical"]

    if ":" in value:
        try:
            module, log_level = value.split(":")
        except ValueError:
            raise BadLogLevel(
                "Invalid log level specification `%s`, "
                "needs to be in format `module:LOGLEVEL`" % value
            )
    else:
        module = None
        log_level = value

    if log_level.lower() not in LOG_LEVEL_NAMES:
        raise BadLogLevel(
            f"Log level {log_level} unknown (in `{value}`) needs to be one "
            f"of {', '.join(LOG_LEVEL_NAMES)}"
        )

    return (module, logging.getLevelName(log_level.upper()))


def logging_configure(
    log_levels: List[Tuple[str, int]] = [], log_config: Optional[Path] = None
) -> str:
    """A default configuration function to unify swh module logger configuration.

    The log_config YAML file must conform to the logging.config.dictConfig schema
    documented at https://docs.python.org/3/library/logging.config.html.

    Returns:
        The actual root logger log level name defined.

    """
    set_default_loglevel: Optional[str] = None

    if log_config:
        with open(log_config, "r") as f:
            config_dict = safe_load(f.read())
        # Configure logging using a dictionary config
        dictConfig(config_dict)
        effective_level = logging.root.getEffectiveLevel()
        set_default_loglevel = logging.getLevelName(effective_level)

    if not log_levels:
        log_levels = []

    for module, log_level in log_levels:
        logger = logging.getLogger(module)
        logger.setLevel(log_level)

        if module is None:
            set_default_loglevel = log_level

    if not set_default_loglevel:
        logging.root.setLevel("INFO")
        set_default_loglevel = "INFO"

    return set_default_loglevel
