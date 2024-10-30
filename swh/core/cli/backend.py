#!/usr/bin/env python3
# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from os import get_terminal_size

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group

logger = logging.getLogger(__name__)


@swh_cli_group.group(name="backend", context_settings=CONTEXT_SETTINGS)
@click.pass_context
def backend(ctx):
    """Software Heritage backend generic tools."""
    pass


@backend.command(name="list", context_settings=CONTEXT_SETTINGS)
@click.argument("package", required=True)
@click.argument("cls", required=False, default=None)
@click.pass_context
def bk_list(ctx, package, cls):
    """Show registered backends for the given package

    With their documentation, if any. Example::

        \b
        $ swh backend list vault

        memory     Stub vault backend, for use in the CLI.
        postgresql Backend for the Software Heritage Vault.
        remote     Client to the Software Heritage vault cache.

    If 'cls' is given, display the full docstring for the corresponding
    backend.

    Example::

        \b
        $ swh backend list vault memory

        vault:memory

        Stub vault backend, for use in the CLI.

    """
    from swh.core.config import get_swh_backend_module, list_swh_backends

    if cls is None:
        items = []
        for backend in list_swh_backends(package):
            _, BackendCls = get_swh_backend_module(package, backend)
            msg = BackendCls.__doc__
            if msg is None:
                msg = ""
            msg = msg.strip()
            if "\n" in msg:
                firstline = msg.splitlines()[0]
            else:
                firstline = msg
            items.append((backend, firstline, msg))
        if not items:
            click.secho(
                f"No backend found for package '{package}'",
                fg="red",
                bold=True,
                err=True,
            )
            raise click.Abort()

        max_name = max(len(name) for name, _, _ in items)
        try:
            width = get_terminal_size().columns
        except OSError:
            width = 78
        for name, firstline, msg in items:
            click.echo(
                click.style(
                    f"{name:<{max_name + 1}}",
                    fg="green",
                    bold=True,
                ),
                nl=False,
            )
            firstline = firstline[: width - max_name - 1]
            click.echo(firstline)
    else:
        try:
            _, BackendCls = get_swh_backend_module(package, cls)
        except ValueError:
            BackendCls = None

        if BackendCls is None:
            click.secho(
                f"No backend '{cls}' found for package '{package}'",
                fg="red",
                bold=True,
                err=True,
            )
            raise click.Abort()

        click.echo(
            click.style(
                package,
                fg="green",
                bold=True,
            )
            + ":"
            + click.style(
                cls,
                fg="yellow",
                bold=True,
            )
            + "\n",
        )
        click.echo(BackendCls.__doc__.strip())
