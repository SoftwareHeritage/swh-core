# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from pathlib import Path

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
@click.option(
    "--debug/--no-debug", default=lambda: os.environ.get("DEBUG", False), is_flag=True
)
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


@nar.command(name="serialize", context_settings=CONTEXT_SETTINGS)
@click.argument("path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output",
    "-o",
    type=click.File(mode="wb"),
    default="-",
    help="The file where to output the serialization, default to stdout",
)
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
def nar_serialize_cli(path, output, exclude_vcs, vcs_type):
    """Serialize a path into a NAR archive."""
    from swh.core.nar import nar_serialize

    os.write(
        output.fileno(), nar_serialize(path, exclude_vcs=exclude_vcs, vcs_type=vcs_type)
    )


@nar.command(name="unpack", context_settings=CONTEXT_SETTINGS)
@click.argument("nar_path", type=click.Path(exists=True))
@click.argument("extract_path", type=click.Path())
def nar_unpack_cli(nar_path, extract_path):
    """Unpack a NAR archive (possibly compressed with xz or bz2) into a given
    extract path.

    Please note that a nar archive can contain a single file instead of multiple
    files and directories, in that case extract_path will target a file after
    the unpacking.
    """
    from swh.core.nar import nar_unpack

    nar_unpack(nar_path, extract_path)
