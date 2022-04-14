# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Dict, List, Optional

import pkg_resources


def get_sentry_release():
    main_package = os.environ.get("SWH_MAIN_PACKAGE")
    if main_package:
        version = pkg_resources.get_distribution(main_package).version
        return f"{main_package}@{version}"
    else:
        return None


def envvar_is_positive(envvar: str) -> bool:
    """Check whether a given environment variable looks like a positive boolean value"""
    return os.environ.get(envvar, "false").lower() in ("t", "true", "y", "yes", "1")


def init_sentry(
    sentry_dsn: str,
    *,
    debug: Optional[bool] = None,
    disable_logging_events: Optional[bool] = None,
    integrations: Optional[List] = None,
    extra_kwargs: Optional[Dict] = None,
):
    """Configure the sentry integration

    Args:
      sentry_dsn: sentry DSN; where sentry report will be sent (if empty, pulled from
        :envvar:`SWH_SENTRY_DSN`)
      debug: turn on sentry SDK debug mode (if ``None``, pulled from
        :envvar:`SWH_SENTRY_DEBUG`)
      disable_logging_events: if set, disable the automatic reporting of error/exception
        log entries as sentry events (if ``None``, pulled from
        :envvar:`SWH_SENTRY_DISABLE_LOGGING_EVENTS`)
      integrations: list of dedicated sentry integrations objects
      extra_kwargs: dict of additional parameters passed to :func:`sentry_sdk.init`

    """
    if integrations is None:
        integrations = []
    if extra_kwargs is None:
        extra_kwargs = {}

    sentry_dsn = sentry_dsn or os.environ.get("SWH_SENTRY_DSN", "")
    environment = os.environ.get("SWH_SENTRY_ENVIRONMENT")

    if debug is None:
        debug = envvar_is_positive("SWH_SENTRY_DEBUG")
    if disable_logging_events is None:
        disable_logging_events = envvar_is_positive("SWH_SENTRY_DISABLE_LOGGING_EVENTS")

    if sentry_dsn:
        import sentry_sdk

        if disable_logging_events:
            from sentry_sdk.integrations.logging import LoggingIntegration

            integrations.append(LoggingIntegration(event_level=None))

        sentry_sdk.init(
            release=get_sentry_release(),
            environment=environment,
            dsn=sentry_dsn,
            integrations=integrations,
            debug=debug,
            **extra_kwargs,
        )
