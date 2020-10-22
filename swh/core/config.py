# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from copy import deepcopy
from itertools import chain
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

logger = logging.getLogger(__name__)


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

# conversion per type
_map_convert_fn: Dict[str, Callable] = {
    "int": int,
    "bool": lambda x: x.lower() == "true",
    "list[str]": lambda x: [value.strip() for value in x.split(",")],
    "list[int]": lambda x: [int(value.strip()) for value in x.split(",")],
}

_map_check_fn: Dict[str, Callable] = {
    "int": lambda x: isinstance(x, int),
    "bool": lambda x: isinstance(x, bool),
    "list[str]": lambda x: (isinstance(x, list) and all(isinstance(y, str) for y in x)),
    "list[int]": lambda x: (isinstance(x, list) and all(isinstance(y, int) for y in x)),
}


def exists_accessible(filepath: str) -> bool:
    """Check whether a file exists, and is accessible.

    Returns:
        True if the file exists and is accessible
        False if the file does not exist

    Raises:
        PermissionError if the file cannot be read.
    """

    try:
        os.stat(filepath)
    except PermissionError:
        raise
    except FileNotFoundError:
        return False
    else:
        if os.access(filepath, os.R_OK):
            return True
        else:
            raise PermissionError("Permission denied: {filepath!r}")


def config_basepath(config_path: str) -> str:
    """Return the base path of a configuration file"""
    if config_path.endswith(".yml"):
        return config_path[:-4]

    return config_path


def read_raw_config(base_config_path: str) -> Dict[str, Any]:
    """Read the raw config corresponding to base_config_path.

    Can read yml files.
    """
    yml_file = f"{base_config_path}.yml"
    if exists_accessible(yml_file):
        logger.debug("Loading config file %s", yml_file)
        with open(yml_file) as f:
            return yaml.safe_load(f)

    return {}


def config_exists(config_path):
    """Check whether the given config exists"""
    basepath = config_basepath(config_path)
    return any(
        exists_accessible(basepath + extension) for extension in SWH_CONFIG_EXTENSIONS
    )


def read(
    conf_file: Optional[str] = None,
    default_conf: Optional[Dict[str, Tuple[str, Any]]] = None,
) -> Dict[str, Any]:
    """Read the user's configuration file.

    Fill in the gap using `default_conf`.  `default_conf` is similar to this::

        DEFAULT_CONF = {
            'a': ('str', '/tmp/swh-loader-git/log'),
            'b': ('str', 'dbname=swhloadergit')
            'c': ('bool', true)
            'e': ('bool', None)
            'd': ('int', 10)
        }

    If conf_file is None, return the default config.

    """
    conf: Dict[str, Any] = {}

    if conf_file:
        base_config_path = config_basepath(os.path.expanduser(conf_file))
        conf = read_raw_config(base_config_path) or {}

    if not default_conf:
        return conf

    # remaining missing default configuration key are set
    # also type conversion is enforced for underneath layer
    for key, (nature_type, default_value) in default_conf.items():
        val = conf.get(key, None)
        if val is None:  # fallback to default value
            conf[key] = default_value
        elif not _map_check_fn.get(nature_type, lambda x: True)(val):
            # value present but not in the proper format, force type conversion
            conf[key] = _map_convert_fn.get(nature_type, lambda x: x)(val)

    return conf


def priority_read(
    conf_filenames: List[str], default_conf: Optional[Dict[str, Tuple[str, Any]]] = None
):
    """Try reading the configuration files from conf_filenames, in order,
       and return the configuration from the first one that exists.

       default_conf has the same specification as it does in read.
    """

    # Try all the files in order
    for filename in conf_filenames:
        full_filename = os.path.expanduser(filename)
        if config_exists(full_filename):
            return read(full_filename, default_conf)

    # Else, return the default configuration
    return read(None, default_conf)


def merge_default_configs(base_config, *other_configs):
    """Merge several default config dictionaries, from left to right"""
    full_config = base_config.copy()

    for config in other_configs:
        full_config.update(config)

    return full_config


def merge_configs(base: Optional[Dict[str, Any]], other: Optional[Dict[str, Any]]):
    """Merge two config dictionaries

    This does merge config dicts recursively, with the rules, for every value
    of the dicts (with 'val' not being a dict):

    - None + type -> type
    - type + None -> None
    - dict + dict -> dict (merged)
    - val + dict -> TypeError
    - dict + val -> TypeError
    - val + val -> val (other)

    for instance:

    >>> d1 = {
    ...   'key1': {
    ...     'skey1': 'value1',
    ...     'skey2': {'sskey1': 'value2'},
    ...   },
    ...   'key2': 'value3',
    ... }

    with

    >>> d2 = {
    ...   'key1': {
    ...     'skey1': 'value4',
    ...     'skey2': {'sskey2': 'value5'},
    ...   },
    ...   'key3': 'value6',
    ... }

    will give:

    >>> d3 = {
    ...   'key1': {
    ...     'skey1': 'value4',  # <-- note this
    ...     'skey2': {
    ...       'sskey1': 'value2',
    ...       'sskey2': 'value5',
    ...     },
    ...   },
    ...   'key2': 'value3',
    ...   'key3': 'value6',
    ... }
    >>> assert merge_configs(d1, d2) == d3

    Note that no type checking is done for anything but dicts.
    """
    if not isinstance(base, dict) or not isinstance(other, dict):
        raise TypeError("Cannot merge a %s with a %s" % (type(base), type(other)))

    output = {}
    allkeys = set(chain(base.keys(), other.keys()))
    for k in allkeys:
        vb = base.get(k)
        vo = other.get(k)

        if isinstance(vo, dict):
            output[k] = merge_configs(vb is not None and vb or {}, vo)
        elif isinstance(vb, dict) and k in other and other[k] is not None:
            output[k] = merge_configs(vb, vo is not None and vo or {})
        elif k in other:
            output[k] = deepcopy(vo)
        else:
            output[k] = deepcopy(vb)

    return output


def swh_config_paths(base_filename: str) -> List[str]:
    """Return the Software Heritage specific configuration paths for the given
       filename."""

    return [os.path.join(dirname, base_filename) for dirname in SWH_CONFIG_DIRECTORIES]


def prepare_folders(conf, *keys):
    """Prepare the folder mentioned in config under keys.
    """

    def makedir(folder):
        if not os.path.exists(folder):
            os.makedirs(folder)

    for key in keys:
        makedir(conf[key])


def load_global_config():
    """Load the global Software Heritage config"""

    return priority_read(
        swh_config_paths(SWH_GLOBAL_CONFIG), SWH_DEFAULT_GLOBAL_CONFIG,
    )


def load_named_config(name, default_conf=None, global_conf=True):
    """Load the config named `name` from the Software Heritage
       configuration paths.

       If global_conf is True (default), read the global configuration
       too.
    """

    conf = {}

    if global_conf:
        conf.update(load_global_config())

    conf.update(priority_read(swh_config_paths(name), default_conf))

    return conf


def load_from_envvar(default_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Load configuration yaml file from the environment variable SWH_CONFIG_FILENAME,
    eventually enriched with default configuration key/value from the default_config
    dict if provided.

    Returns:
        Configuration dict

    Raises:
        AssertionError if SWH_CONFIG_FILENAME is undefined

    """
    assert (
        "SWH_CONFIG_FILENAME" in os.environ
    ), "SWH_CONFIG_FILENAME environment variable is undefined."

    cfg_path = os.environ["SWH_CONFIG_FILENAME"]
    cfg = read_raw_config(config_basepath(cfg_path))
    cfg = merge_configs(default_config or {}, cfg)
    return cfg
