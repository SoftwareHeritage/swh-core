# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Default values for gunicorn's configuration.

Other packages may override them by importing `*` from this module
and redefining functions and variables they want.

May be imported by gunicorn using
`--config 'python:swh.core.api.gunicorn_config'`."""

import os


def _init_sentry(
        sentry_dsn, *, flask=True, integrations=None, extra_kwargs={}):
    import sentry_sdk

    integrations = integrations or []

    if flask:
        from sentry_sdk.integrations.flask import FlaskIntegration
        integrations.append(FlaskIntegration())

    sentry_sdk.init(
        dsn=sentry_dsn,
        integrations=integrations,
        debug=bool(os.environ.get('SWH_SENTRY_DEBUG')),
        **extra_kwargs,
    )


def post_fork(
        server, worker, *, default_sentry_dsn=None, flask=True,
        sentry_integrations=None, extra_sentry_kwargs={}):

    # Initializes sentry as soon as possible in gunicorn's worker processes.
    sentry_dsn = os.environ.get('SWH_SENTRY_DSN', default_sentry_dsn)
    if sentry_dsn:
        _init_sentry(
            sentry_dsn, flask=flask, integrations=sentry_integrations,
            extra_kwargs=extra_sentry_kwargs)
