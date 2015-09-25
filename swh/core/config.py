# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import configparser
import os


SWH_CONFIG_DIRECTORIES = [
    '~/.config/softwareheritage',
    '~/.swh',
    '/etc/softwareheritage',
]


# conversion per type
_map_convert_fn = {
    'int': int,
    'bool': lambda x: x.lower() == 'true',
    'list[str]': lambda x: [value.strip() for value in x.split(',')],
    'list[int]': lambda x: [int(value.strip()) for value in x.split(',')],
}


def read(conf_file=None, default_conf=None):
    """Read the user's configuration file.
    Fill in the gap using `default_conf`.
`default_conf` is similar to this:
DEFAULT_CONF = {
    'a': ('string', '/tmp/swh-loader-git/log'),
    'b': ('string', 'dbname=swhloadergit')
    'c': ('bool', true)
    'e': ('bool', None)
    'd': ('int', 10)
}

If conf_file is None, return the default config.

    """
    conf = {}

    if conf_file:
        config_path = os.path.expanduser(conf_file)
        if os.path.exists(config_path):
            config = configparser.ConfigParser(defaults=default_conf)
            config.read(os.path.expanduser(conf_file))
            if 'main' in config._sections:
                conf = config._sections['main']

    if not default_conf:
        default_conf = {}

    # remaining missing default configuration key are set
    # also type conversion is enforced for underneath layer
    for key in default_conf:
        nature_type, default_value = default_conf[key]
        val = conf.get(key, None)
        if not val:  # fallback to default value
            conf[key] = default_value
        else:  # value present but in string format, force type conversion
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
        if os.path.exists(full_filename):
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
