# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from importlib.metadata import distribution
import logging
import os
import sys
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def get_sentry_release(
    main_package: Optional[str] = None, sentry_dsn: Optional[str] = None
):
    main_package = os.environ.get("SWH_MAIN_PACKAGE", main_package)
    if main_package:
        version = distribution(main_package).version
        return f"{main_package}@{version}"
    elif sentry_dsn is None:
        # return a dummy release when sentry_dsn is None to avoid side effects
        # related to sentry_sdk calling git command for fetching release info
        return "0.0.0"
    else:
        return None


def override_with_bool_envvar(envvar: str, default: bool) -> bool:
    """Override the `default` with the environment variable `envvar` parsed as a boolean"""
    envvalue = os.environ.get(envvar, "")
    if envvalue.lower() in ("t", "true", "y", "yes", "1"):
        return True
    elif envvalue.lower() in ("f", "false", "n", "no", "0"):
        return False
    else:
        if envvalue:
            logger.warning(
                "Could not interpret environment variable %s=%r as boolean, "
                "using default value %s",
                envvar,
                envvalue,
                default,
            )
        return default


def override_with_float_envvar(
    envvar: str, default: Optional[float]
) -> Optional[float]:
    """Override `default` with the environment variable `envvar` casted as a float.

    `default` is returned if the environment variable `envvar` is missing or if
    we're not able to cast it to a float.

    Args:
        envvar: the name of the environment variable
        default: default value

    Returns:
        A float or `default`
    """
    envvalue = os.environ.get(envvar)
    if envvalue is None:
        return default
    try:
        return float(envvalue)
    except ValueError:
        logger.warning(
            "Could not interpret environment variable %s=%r as float, "
            "using default value %s",
            envvar,
            envvalue,
            default,
        )
        return default


def init_sentry(
    sentry_dsn: Optional[str] = None,
    *,
    main_package: Optional[str] = None,
    environment: Optional[str] = None,
    debug: bool = False,
    disable_logging_events: bool = False,
    integrations: Optional[List] = None,
    traces_sample_rate: Optional[float] = None,
    extra_kwargs: Optional[Dict] = None,
    deferred_init: bool = False,
) -> None:
    """Configure the sentry integration.

    Args:
      sentry_dsn: Sentry DSN; where sentry report will be sent. Overridden by
        :envvar:`SWH_SENTRY_DSN`
      main_package: Full name of main Python package associated to Sentry DSN.
        Overridden by :envvar:`SWH_MAIN_PACKAGE`.
      environment: Sentry environment. Overridden by :envvar:`SWH_SENTRY_ENVIRONMENT`
      debug: turn on Sentry SDK debug mode. Overridden by :envvar:`SWH_SENTRY_DEBUG`
      disable_logging_events: if set, disable the automatic reporting of error/exception
        log entries as Sentry events. Overridden by
        :envvar:`SWH_SENTRY_DISABLE_LOGGING_EVENTS`
      integrations: list of dedicated Sentry integrations to include
      traces_sample_rate: a number between 0 and 1, controlling the percentage chance a
        given transaction will be sent to Sentry. Overridden by
        :envvar:`SWH_SENTRY_TRACES_SAMPLE_RATE`
      extra_kwargs: dict of additional parameters passed to :func:`sentry_sdk.init`
      deferred_init: indicates that sentry will be properly initialized in subsequent
        calls and that no warnings about missing DSN should be logged
    """
    if integrations is None:
        integrations = []
    if extra_kwargs is None:
        extra_kwargs = {}

    sentry_dsn = os.environ.get("SWH_SENTRY_DSN", sentry_dsn)
    environment = os.environ.get("SWH_SENTRY_ENVIRONMENT", environment)

    debug = override_with_bool_envvar("SWH_SENTRY_DEBUG", debug)
    disable_logging_events = override_with_bool_envvar(
        "SWH_SENTRY_DISABLE_LOGGING_EVENTS", disable_logging_events
    )

    if sentry_dsn is None and not deferred_init:
        # Don’t display a warning if there is a controlling terminal
        # as errors can be monitored from there.
        if not sys.stdout.isatty():
            logger.warning("Sentry DSN not provided, events will not be sent.")

    import sentry_sdk

    if disable_logging_events:
        from sentry_sdk.integrations.logging import LoggingIntegration

        integrations.append(LoggingIntegration(event_level=None))

    # to completely disable tracing `traces_sample_rate` should be set to None instead
    # of 0.0
    traces_sample_rate = override_with_float_envvar(
        "SWH_SENTRY_TRACES_SAMPLE_RATE", traces_sample_rate
    )

    sentry_sdk.init(
        release=get_sentry_release(main_package, sentry_dsn),
        environment=environment,
        dsn=sentry_dsn,
        traces_sample_rate=traces_sample_rate,
        integrations=integrations,
        debug=debug,
        **extra_kwargs,
    )
