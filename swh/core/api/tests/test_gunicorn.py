# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from importlib.metadata import distribution
import os

import swh.core.api.gunicorn_config as gunicorn_config


def test_post_fork_default(mocker):
    flask_integration = object()  # unique object to check for equality
    logging_integration = object()  # unique object to check for equality
    mocker.patch(
        "sentry_sdk.integrations.flask.FlaskIntegration", new=lambda: flask_integration
    )
    mocker.patch(
        "sentry_sdk.integrations.logging.LoggingIntegration",
        new=lambda event_level: logging_integration,
    )
    sentry_sdk_init = mocker.patch("sentry_sdk.init")

    gunicorn_config.post_fork(None, None)

    sentry_sdk_init.assert_called_once_with(
        dsn=None,
        integrations=[flask_integration, logging_integration],
        debug=False,
        release="0.0.0",
        environment=None,
        traces_sample_rate=None,
    )


def test_post_fork_with_dsn_env(mocker):
    flask_integration = object()  # unique object to check for equality
    logging_integration = object()  # unique object to check for equality
    mocker.patch(
        "sentry_sdk.integrations.flask.FlaskIntegration", new=lambda: flask_integration
    )
    mocker.patch(
        "sentry_sdk.integrations.logging.LoggingIntegration",
        new=lambda event_level: logging_integration,
    )
    sentry_sdk_init = mocker.patch("sentry_sdk.init")
    mocker.patch.dict(os.environ, {"SWH_SENTRY_DSN": "test_dsn"})

    gunicorn_config.post_fork(None, None)

    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        integrations=[flask_integration, logging_integration],
        debug=False,
        release=None,
        environment=None,
        traces_sample_rate=None,
    )


def test_post_fork_with_package_env(mocker):
    flask_integration = object()
    logging_integration = object()

    mocker.patch(
        "sentry_sdk.integrations.flask.FlaskIntegration", new=lambda: flask_integration
    )
    mocker.patch(
        "sentry_sdk.integrations.logging.LoggingIntegration",
        new=lambda event_level: logging_integration,
    )
    sentry_sdk_init = mocker.patch("sentry_sdk.init")
    mocker.patch.dict(
        os.environ,
        {
            "SWH_SENTRY_DSN": "test_dsn",
            "SWH_SENTRY_ENVIRONMENT": "tests",
            "SWH_MAIN_PACKAGE": "swh.core",
        },
    )

    gunicorn_config.post_fork(None, None)

    version = distribution("swh.core").version

    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        integrations=[flask_integration, logging_integration],
        debug=False,
        release="swh.core@" + version,
        environment="tests",
        traces_sample_rate=None,
    )


def test_post_fork_debug(mocker):
    flask_integration = object()
    logging_integration = object()

    mocker.patch(
        "sentry_sdk.integrations.flask.FlaskIntegration", new=lambda: flask_integration
    )
    mocker.patch(
        "sentry_sdk.integrations.logging.LoggingIntegration",
        new=lambda event_level: logging_integration,
    )
    sentry_sdk_init = mocker.patch("sentry_sdk.init")
    mocker.patch.dict(
        os.environ, {"SWH_SENTRY_DSN": "test_dsn", "SWH_SENTRY_DEBUG": "1"}
    )

    gunicorn_config.post_fork(None, None)

    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        integrations=[flask_integration, logging_integration],
        debug=True,
        release=None,
        environment=None,
        traces_sample_rate=None,
    )


def test_post_fork_no_flask(mocker):
    logging_integration = object()
    sentry_sdk_init = mocker.patch("sentry_sdk.init")
    mocker.patch(
        "sentry_sdk.integrations.logging.LoggingIntegration",
        new=lambda event_level: logging_integration,
    )
    mocker.patch.dict(os.environ, {"SWH_SENTRY_DSN": "test_dsn"})

    gunicorn_config.post_fork(None, None, flask=False)

    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        integrations=[logging_integration],
        debug=False,
        release=None,
        environment=None,
        traces_sample_rate=None,
    )


def test_post_fork_override_logging_events_envvar(mocker):
    sentry_sdk_init = mocker.patch("sentry_sdk.init")
    mocker.patch.dict(
        os.environ,
        {"SWH_SENTRY_DSN": "test_dsn", "SWH_SENTRY_DISABLE_LOGGING_EVENTS": "false"},
    )

    gunicorn_config.post_fork(None, None, flask=False)

    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        integrations=[],
        debug=False,
        release=None,
        environment=None,
        traces_sample_rate=None,
    )


def test_post_fork_extras(mocker):
    flask_integration = object()  # unique object to check for equality
    mocker.patch(
        "sentry_sdk.integrations.flask.FlaskIntegration", new=lambda: flask_integration
    )
    sentry_sdk_init = mocker.patch("sentry_sdk.init")
    mocker.patch.dict(os.environ, {"SWH_SENTRY_DSN": "test_dsn"})

    gunicorn_config.post_fork(
        None,
        None,
        sentry_integrations=["foo"],
        extra_sentry_kwargs={"bar": "baz"},
        disable_logging_events=False,
    )

    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        integrations=["foo", flask_integration],
        debug=False,
        bar="baz",
        release=None,
        environment=None,
        traces_sample_rate=None,
    )


def test_post_fork_traces_sample_rate(mocker):
    flask_integration = object()  # unique object to check for equality
    logging_integration = object()  # unique object to check for equality
    mocker.patch(
        "sentry_sdk.integrations.flask.FlaskIntegration", new=lambda: flask_integration
    )
    mocker.patch(
        "sentry_sdk.integrations.logging.LoggingIntegration",
        new=lambda event_level: logging_integration,
    )
    sentry_sdk_init = mocker.patch("sentry_sdk.init")
    mocker.patch.dict(os.environ, {"SWH_SENTRY_DSN": "test_dsn"})

    gunicorn_config.post_fork(None, None, traces_sample_rate=1.0)

    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        integrations=[flask_integration, logging_integration],
        debug=False,
        release=None,
        environment=None,
        traces_sample_rate=1.0,
    )


def test_post_fork_override_traces_sample_rate_envvar(mocker):
    flask_integration = object()  # unique object to check for equality
    logging_integration = object()  # unique object to check for equality
    mocker.patch(
        "sentry_sdk.integrations.flask.FlaskIntegration", new=lambda: flask_integration
    )
    mocker.patch(
        "sentry_sdk.integrations.logging.LoggingIntegration",
        new=lambda event_level: logging_integration,
    )
    sentry_sdk_init = mocker.patch("sentry_sdk.init")
    mocker.patch.dict(
        os.environ,
        {"SWH_SENTRY_DSN": "test_dsn", "SWH_SENTRY_TRACES_SAMPLE_RATE": "0.999"},
    )

    gunicorn_config.post_fork(None, None)

    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        integrations=[flask_integration, logging_integration],
        debug=False,
        release=None,
        environment=None,
        traces_sample_rate=0.999,
    )
