#!/usr/bin/env python3

# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import configparser
import os


_map_convert_fn = {'int': int,
                   'bool': lambda x: x == 'true'}  # conversion per type


def read(conf_file, default_conf=None):
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

    """
    config = configparser.ConfigParser(defaults=default_conf)
    config.read(os.path.expanduser(conf_file))
    conf = config._sections['main']

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


def prepare_folders(conf, *keys):
    """Prepare the folder mentioned in config under keys.
    """
    def makedir(folder):
        if not os.path.exists(folder):
            os.makedirs(folder)

    for key in keys:
        makedir(conf[key])
