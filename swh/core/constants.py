# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


SWH_CONFIG_DIRECTORIES = [
    "~/.config/swh",
    "~/.swh",
    "/etc/softwareheritage",
]

SWH_GLOBAL_CONFIG = "global.yml"

SWH_DEFAULT_GLOBAL_CONFIG = {
    "max_content_size": ("int", 100 * 1024 * 1024),
}

SWH_CONFIG_EXTENSIONS = [
    ".yml",
]
