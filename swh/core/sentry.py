# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

import pkg_resources


def get_sentry_release():
    main_package = os.environ.get("SWH_MAIN_PACKAGE")
    if main_package:
        version = pkg_resources.get_distribution(main_package).version
        return f"{main_package}@{version}"
    else:
        return None


def init_sentry(sentry_dsn, *, debug=None, integrations=[], extra_kwargs={}):
    if debug is None:
        debug = bool(os.environ.get("SWH_SENTRY_DEBUG"))
    sentry_dsn = sentry_dsn or os.environ.get("SWH_SENTRY_DSN")
    environment = os.environ.get("SWH_SENTRY_ENVIRONMENT")

    if sentry_dsn:
        import sentry_sdk

        sentry_sdk.init(
            release=get_sentry_release(),
            environment=environment,
            dsn=sentry_dsn,
            integrations=integrations,
            debug=debug,
            **extra_kwargs,
        )
