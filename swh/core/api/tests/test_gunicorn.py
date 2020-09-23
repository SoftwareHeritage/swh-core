# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from unittest.mock import patch

import pkg_resources

import swh.core.api.gunicorn_config as gunicorn_config


def test_post_fork_default():
    with patch("sentry_sdk.init") as sentry_sdk_init:
        gunicorn_config.post_fork(None, None)

    sentry_sdk_init.assert_not_called()


def test_post_fork_with_dsn_env():
    flask_integration = object()  # unique object to check for equality
    with patch(
        "sentry_sdk.integrations.flask.FlaskIntegration", new=lambda: flask_integration
    ):
        with patch("sentry_sdk.init") as sentry_sdk_init:
            with patch.dict(os.environ, {"SWH_SENTRY_DSN": "test_dsn"}):
                gunicorn_config.post_fork(None, None)

    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        integrations=[flask_integration],
        debug=False,
        release=None,
        environment=None,
    )


def test_post_fork_with_package_env():
    flask_integration = object()  # unique object to check for equality
    with patch(
        "sentry_sdk.integrations.flask.FlaskIntegration", new=lambda: flask_integration
    ):
        with patch("sentry_sdk.init") as sentry_sdk_init:
            with patch.dict(
                os.environ,
                {
                    "SWH_SENTRY_DSN": "test_dsn",
                    "SWH_SENTRY_ENVIRONMENT": "tests",
                    "SWH_MAIN_PACKAGE": "swh.core",
                },
            ):
                gunicorn_config.post_fork(None, None)

    version = pkg_resources.get_distribution("swh.core").version

    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        integrations=[flask_integration],
        debug=False,
        release="swh.core@" + version,
        environment="tests",
    )


def test_post_fork_debug():
    flask_integration = object()  # unique object to check for equality
    with patch(
        "sentry_sdk.integrations.flask.FlaskIntegration", new=lambda: flask_integration
    ):
        with patch("sentry_sdk.init") as sentry_sdk_init:
            with patch.dict(
                os.environ, {"SWH_SENTRY_DSN": "test_dsn", "SWH_SENTRY_DEBUG": "1"}
            ):
                gunicorn_config.post_fork(None, None)

    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        integrations=[flask_integration],
        debug=True,
        release=None,
        environment=None,
    )


def test_post_fork_no_flask():
    with patch("sentry_sdk.init") as sentry_sdk_init:
        with patch.dict(os.environ, {"SWH_SENTRY_DSN": "test_dsn"}):
            gunicorn_config.post_fork(None, None, flask=False)

    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn", integrations=[], debug=False, release=None, environment=None,
    )


def test_post_fork_extras():
    flask_integration = object()  # unique object to check for equality
    with patch(
        "sentry_sdk.integrations.flask.FlaskIntegration", new=lambda: flask_integration
    ):
        with patch("sentry_sdk.init") as sentry_sdk_init:
            with patch.dict(os.environ, {"SWH_SENTRY_DSN": "test_dsn"}):
                gunicorn_config.post_fork(
                    None,
                    None,
                    sentry_integrations=["foo"],
                    extra_sentry_kwargs={"bar": "baz"},
                )

    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        integrations=["foo", flask_integration],
        debug=False,
        bar="baz",
        release=None,
        environment=None,
    )
