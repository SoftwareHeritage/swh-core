# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Default values for gunicorn's configuration.

Other packages may override them by importing `*` from this module
and redefining functions and variables they want.

May be imported by gunicorn using
`--config 'python:swh.core.api.gunicorn_config'`."""


from ..sentry import init_sentry


def post_fork(
    server,
    worker,
    *,
    default_sentry_dsn=None,
    flask=True,
    sentry_integrations=None,
    extra_sentry_kwargs={},
):
    # Initializes sentry as soon as possible in gunicorn's worker processes.

    sentry_integrations = sentry_integrations or []
    if flask:
        from sentry_sdk.integrations.flask import FlaskIntegration

        sentry_integrations.append(FlaskIntegration())

    init_sentry(
        default_sentry_dsn,
        integrations=sentry_integrations,
        extra_kwargs=extra_sentry_kwargs,
    )
