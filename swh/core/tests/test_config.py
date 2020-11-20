# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil

import pkg_resources.extern.packaging.version
import pytest
import yaml

from swh.core import config

pytest_v = pkg_resources.get_distribution("pytest").parsed_version
if pytest_v < pkg_resources.extern.packaging.version.parse("3.9"):

    @pytest.fixture
    def tmp_path():
        import pathlib
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            yield pathlib.Path(tmpdir)


default_conf = {
    "a": ("int", 2),
    "b": ("string", "default-string"),
    "c": ("bool", True),
    "d": ("int", 10),
    "e": ("int", None),
    "f": ("bool", None),
    "g": ("string", None),
    "h": ("bool", True),
    "i": ("bool", True),
    "ls": ("list[str]", ["a", "b", "c"]),
    "li": ("list[int]", [42, 43]),
}

other_default_conf = {
    "a": ("int", 3),
}

full_default_conf = default_conf.copy()
full_default_conf["a"] = other_default_conf["a"]

parsed_default_conf = {key: value for key, (type, value) in default_conf.items()}

parsed_conffile = {
    "a": 1,
    "b": "this is a string",
    "c": True,
    "d": 10,
    "e": None,
    "f": None,
    "g": None,
    "h": False,
    "i": True,
    "ls": ["list", "of", "strings"],
    "li": [1, 2, 3, 4],
}


@pytest.fixture
def swh_config(tmp_path):
    # create a temporary folder
    conffile = tmp_path / "config.yml"
    conf_contents = """
a: 1
b: this is a string
c: true
h: false
ls: list, of, strings
li: 1, 2, 3, 4
"""
    conffile.open("w").write(conf_contents)
    return conffile


@pytest.fixture
def swh_config_unreadable(swh_config):
    # Create an unreadable, proper configuration file
    os.chmod(str(swh_config), 0o000)
    yield swh_config
    # Make the broken perms file readable again to be able to remove them
    os.chmod(str(swh_config), 0o644)


@pytest.fixture
def swh_config_unreadable_dir(swh_config):
    # Create a proper configuration file in an unreadable directory
    perms_broken_dir = swh_config.parent / "unreadabledir"
    perms_broken_dir.mkdir()
    shutil.move(str(swh_config), str(perms_broken_dir))
    os.chmod(str(perms_broken_dir), 0o000)
    yield perms_broken_dir / swh_config.name
    # Make the broken perms items readable again to be able to remove them
    os.chmod(str(perms_broken_dir), 0o755)


@pytest.fixture
def swh_config_empty(tmp_path):
    # create a temporary folder
    conffile = tmp_path / "config.yml"
    conffile.touch()
    return conffile


def test_read(swh_config):
    # when
    res = config.read(str(swh_config), default_conf)

    # then
    assert res == parsed_conffile


def test_read_no_default_conf(swh_config):
    """If no default config if provided to read, this should directly parse the config file
    yaml

    """
    config_path = str(swh_config)
    actual_config = config.read(config_path)
    with open(config_path) as f:
        expected_config = yaml.safe_load(f)
    assert actual_config == expected_config


def test_read_empty_file():
    # when
    res = config.read(None, default_conf)

    # then
    assert res == parsed_default_conf


def test_support_non_existing_conffile(tmp_path):
    # when
    res = config.read(str(tmp_path / "void.yml"), default_conf)

    # then
    assert res == parsed_default_conf


def test_support_empty_conffile(swh_config_empty):
    # when
    res = config.read(str(swh_config_empty), default_conf)

    # then
    assert res == parsed_default_conf


def test_raise_on_broken_directory_perms(swh_config_unreadable_dir):
    with pytest.raises(PermissionError):
        config.read(str(swh_config_unreadable_dir), default_conf)


def test_raise_on_broken_file_perms(swh_config_unreadable):
    with pytest.raises(PermissionError):
        config.read(str(swh_config_unreadable), default_conf)


def test_merge_default_configs():
    # when
    res = config.merge_default_configs(default_conf, other_default_conf)

    # then
    assert res == full_default_conf


def test_priority_read_nonexist_conf(swh_config):
    noexist = str(swh_config.parent / "void.yml")
    # when
    res = config.priority_read([noexist, str(swh_config)], default_conf)

    # then
    assert res == parsed_conffile


def test_priority_read_conf_nonexist_empty(swh_config):
    noexist = swh_config.parent / "void.yml"
    empty = swh_config.parent / "empty.yml"
    empty.touch()

    # when
    res = config.priority_read(
        [str(p) for p in (swh_config, noexist, empty)], default_conf
    )

    # then
    assert res == parsed_conffile


def test_priority_read_empty_conf_nonexist(swh_config):
    noexist = swh_config.parent / "void.yml"
    empty = swh_config.parent / "empty.yml"
    empty.touch()

    # when
    res = config.priority_read(
        [str(p) for p in (empty, swh_config, noexist)], default_conf
    )

    # then
    assert res == parsed_default_conf


def test_swh_config_paths():
    res = config.swh_config_paths("foo/bar.yml")

    assert res == [
        "~/.config/swh/foo/bar.yml",
        "~/.swh/foo/bar.yml",
        "/etc/softwareheritage/foo/bar.yml",
    ]


def test_prepare_folder(tmp_path):
    # given
    conf = {
        "path1": str(tmp_path / "path1"),
        "path2": str(tmp_path / "path2" / "depth1"),
    }

    # the folders does not exists
    assert not os.path.exists(conf["path1"]), "path1 should not exist."
    assert not os.path.exists(conf["path2"]), "path2 should not exist."

    # when
    config.prepare_folders(conf, "path1")

    # path1 exists but not path2
    assert os.path.exists(conf["path1"]), "path1 should now exist!"
    assert not os.path.exists(conf["path2"]), "path2 should not exist."

    # path1 already exists, skips it but creates path2
    config.prepare_folders(conf, "path1", "path2")

    assert os.path.exists(conf["path1"]), "path1 should still exist!"
    assert os.path.exists(conf["path2"]), "path2 should now exist."


def test_merge_config():
    cfg_a = {
        "a": 42,
        "b": [1, 2, 3],
        "c": None,
        "d": {"gheez": 27},
        "e": {
            "ea": "Mr. Bungle",
            "eb": None,
            "ec": [11, 12, 13],
            "ed": {"eda": "Secret Chief 3", "edb": "Faith No More"},
            "ee": 451,
        },
        "f": "Janis",
    }
    cfg_b = {
        "a": 43,
        "b": [41, 42, 43],
        "c": "Tom Waits",
        "d": None,
        "e": {
            "ea": "Igorrr",
            "ec": [51, 52],
            "ed": {"edb": "Sleepytime Gorilla Museum", "edc": "Nils Peter Molvaer"},
        },
        "g": "Hüsker Dü",
    }

    # merge A, B
    cfg_m = config.merge_configs(cfg_a, cfg_b)
    assert cfg_m == {
        "a": 43,  # b takes precedence
        "b": [41, 42, 43],  # b takes precedence
        "c": "Tom Waits",  # b takes precedence
        "d": None,  # b['d'] takes precedence (explicit None)
        "e": {
            "ea": "Igorrr",  # a takes precedence
            "eb": None,  # only in a
            "ec": [51, 52],  # b takes precedence
            "ed": {
                "eda": "Secret Chief 3",  # only in a
                "edb": "Sleepytime Gorilla Museum",  # b takes precedence
                "edc": "Nils Peter Molvaer",
            },  # only defined in b
            "ee": 451,
        },
        "f": "Janis",  # only defined in a
        "g": "Hüsker Dü",  # only defined in b
    }

    # merge B, A
    cfg_m = config.merge_configs(cfg_b, cfg_a)
    assert cfg_m == {
        "a": 42,  # a takes precedence
        "b": [1, 2, 3],  # a takes precedence
        "c": None,  # a takes precedence
        "d": {"gheez": 27},  # a takes precedence
        "e": {
            "ea": "Mr. Bungle",  # a takes precedence
            "eb": None,  # only defined in a
            "ec": [11, 12, 13],  # a takes precedence
            "ed": {
                "eda": "Secret Chief 3",  # only in a
                "edb": "Faith No More",  # a takes precedence
                "edc": "Nils Peter Molvaer",
            },  # only in b
            "ee": 451,
        },
        "f": "Janis",  # only in a
        "g": "Hüsker Dü",  # only in b
    }


def test_merge_config_type_error():
    for v in (1, "str", None):
        with pytest.raises(TypeError):
            config.merge_configs(v, {})
        with pytest.raises(TypeError):
            config.merge_configs({}, v)

    for v in (1, "str"):
        with pytest.raises(TypeError):
            config.merge_configs({"a": v}, {"a": {}})
        with pytest.raises(TypeError):
            config.merge_configs({"a": {}}, {"a": v})


def test_load_from_envvar_no_environment_var_swh_config_filename_set():
    """Without SWH_CONFIG_FILENAME set, load_from_envvar raises"""

    with pytest.raises(AssertionError, match="SWH_CONFIG_FILENAME environment"):
        config.load_from_envvar()


def test_load_from_envvar_no_default_config(swh_config, monkeypatch):
    config_path = str(swh_config)
    monkeypatch.setenv("SWH_CONFIG_FILENAME", config_path)

    actual_config = config.load_from_envvar()

    expected_config = config.read(config_path)
    assert actual_config == expected_config


def test_load_from_envvar_with_default_config(swh_config, monkeypatch):
    default_config = {
        "number": 666,
        "something-cool": ["something", "cool"],
    }

    config_path = str(swh_config)
    monkeypatch.setenv("SWH_CONFIG_FILENAME", config_path)

    actual_config = config.load_from_envvar(default_config)

    expected_config = config.read(config_path)
    expected_config.update(
        {"number": 666, "something-cool": ["something", "cool"],}
    )

    assert actual_config == expected_config
