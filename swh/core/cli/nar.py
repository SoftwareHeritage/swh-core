# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group


@swh_cli_group.group(name="nar", context_settings=CONTEXT_SETTINGS)
def nar():
    """NAR (NixOS Archive) utils."""
    pass


@nar.command(name="hash", context_settings=CONTEXT_SETTINGS)
@click.argument("path")
@click.option(
    "--exclude-vcs",
    "-x",
    help="Exclude version control directories",
    is_flag=True,
)
@click.option(
    "--vcs-type",
    "-t",
    help="Type of version control system to exclude directories",
    default="git",
)
@click.option(
    "--hash-algo",
    "-H",
    "hash_names",
    multiple=True,
    default=["sha256"],
    type=click.Choice(["sha256", "sha1"]),
)
@click.option(
    "--format-output",
    "-f",
    default="hex",
    type=click.Choice(["hex", "base32", "base64"], case_sensitive=False),
)
@click.option("--debug/--no-debug", default=lambda: os.environ.get("DEBUG", False))
def nar_hash_cli(exclude_vcs, vcs_type, path, hash_names, format_output, debug):
    """Compute NAR hash of a given path."""

    from swh.core.nar import Nar

    nar = Nar(hash_names, exclude_vcs, vcs_type, debug=debug)

    convert_fn = {
        "base64": nar.b64digest,
        "base32": nar.b32digest,
        "hex": nar.hexdigest,
    }

    nar.serialize(path)
    result = convert_fn[format_output]()

    if len(hash_names) == 1:
        print(result[hash_names[0]])
    else:
        print(result)
