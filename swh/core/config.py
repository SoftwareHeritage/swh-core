# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import configparser
import os
import yaml


SWH_CONFIG_DIRECTORIES = [
    '~/.config/swh',
    '~/.swh',
    '/etc/softwareheritage',
]

SWH_GLOBAL_CONFIG = 'global.ini'

SWH_DEFAULT_GLOBAL_CONFIG = {
    'content_size_limit': ('int', 100 * 1024 * 1024),
    'log_db': ('str', 'dbname=softwareheritage-log'),
}

SWH_CONFIG_EXTENSIONS = [
    '.yml',
    '.ini',
]

# conversion per type
_map_convert_fn = {
    'int': int,
    'bool': lambda x: x.lower() == 'true',
    'list[str]': lambda x: [value.strip() for value in x.split(',')],
    'list[int]': lambda x: [int(value.strip()) for value in x.split(',')],
}

_map_check_fn = {
    'int': lambda x: isinstance(x, int),
    'bool': lambda x: isinstance(x, bool),
    'list[str]': lambda x: (isinstance(x, list) and
                            all(isinstance(y, str) for y in x)),
    'list[int]': lambda x: (isinstance(x, list) and
                            all(isinstance(y, int) for y in x)),
}


def exists_accessible(file):
    """Check whether a file exists, and is accessible.

    Returns:
        True if the file exists and is accessible
        False if the file does not exist

    Raises:
        PermissionError if the file cannot be read.
    """

    try:
        os.stat(file)
    except PermissionError:
        raise
    except FileNotFoundError:
        return False
    else:
        if os.access(file, os.R_OK):
            return True
        else:
            raise PermissionError("Permission denied: %r" % file)


def config_basepath(config_path):
    """Return the base path of a configuration file"""
    if config_path.endswith(('.ini', '.yml')):
        return config_path[:-4]

    return config_path


def read_raw_config(base_config_path):
    """Read the raw config corresponding to base_config_path.

    Can read yml or ini files.
    """

    yml_file = base_config_path + '.yml'
    if exists_accessible(yml_file):
        with open(yml_file) as f:
            return yaml.safe_load(f)

    ini_file = base_config_path + '.ini'
    if exists_accessible(ini_file):
        config = configparser.ConfigParser()
        config.read(ini_file)
        if 'main' in config._sections:
            return config._sections['main']

    return {}


def config_exists(config_path):
    """Check whether the given config exists"""
    basepath = config_basepath(config_path)
    return any(exists_accessible(basepath + extension)
               for extension in SWH_CONFIG_EXTENSIONS)


def read(conf_file=None, default_conf=None):
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
    conf = {}

    if conf_file:
        base_config_path = config_basepath(os.path.expanduser(conf_file))
        conf = read_raw_config(base_config_path)

    if not default_conf:
        default_conf = {}

    # remaining missing default configuration key are set
    # also type conversion is enforced for underneath layer
    for key in default_conf:
        nature_type, default_value = default_conf[key]
        val = conf.get(key, None)
        if val is None:  # fallback to default value
            conf[key] = default_value
        elif not _map_check_fn.get(nature_type, lambda x: True)(val):
            # value present but not in the proper format, force type conversion
            conf[key] = _map_convert_fn.get(nature_type, lambda x: x)(val)

    return conf


def priority_read(conf_filenames, default_conf=None):
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


def swh_config_paths(base_filename):
    """Return the Software Heritage specific configuration paths for the given
       filename."""

    return [os.path.join(dirname, base_filename)
            for dirname in SWH_CONFIG_DIRECTORIES]


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
        swh_config_paths(SWH_GLOBAL_CONFIG),
        SWH_DEFAULT_GLOBAL_CONFIG,
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


class SWHConfig:
    """Mixin to add configuration parsing abilities to classes

    The class should override the class attributes:
        - DEFAULT_CONFIG (default configuration to be parsed)
        - CONFIG_BASE_FILENAME (the filename of the configuration to be used)

    This class defines one classmethod, parse_config_file, which
    parses a configuration file using the default config as set in the
    class attribute.

    """

    DEFAULT_CONFIG = {}
    CONFIG_BASE_FILENAME = ''

    @classmethod
    def parse_config_file(cls, base_filename=None, config_filename=None,
                          additional_configs=None, global_config=True):
        """Parse the configuration file associated to the current class.

        By default, parse_config_file will load the configuration
        cls.CONFIG_BASE_FILENAME from one of the Software Heritage
        configuration directories, in order, unless it is overridden
        by base_filename or config_filename (which shortcuts the file
        lookup completely).

        Args:
            - base_filename (str) overrides the default
                cls.CONFIG_BASE_FILENAME
            - config_filename (str) sets the file to parse instead of
                the defaults set from cls.CONFIG_BASE_FILENAME
            - additional_configs (list of default configuration dicts)
                allows to override or extend the configuration set in
                cls.DEFAULT_CONFIG.
            - global_config (bool): Load the global configuration (default:
                True)
        """

        if config_filename:
            config_filenames = [config_filename]
        else:
            if not base_filename:
                base_filename = cls.CONFIG_BASE_FILENAME
            config_filenames = swh_config_paths(base_filename)
        if not additional_configs:
            additional_configs = []

        full_default_config = merge_default_configs(cls.DEFAULT_CONFIG,
                                                    *additional_configs)

        config = {}
        if global_config:
            config = load_global_config()

        config.update(priority_read(config_filenames, full_default_config))

        return config
