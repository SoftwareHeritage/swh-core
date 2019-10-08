# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import requests

from os import path

from pytest_swh_core import local_get_factory


def test_get_response_cb_with_visits_nominal(local_get_visits):
    response = requests.get('https://example.com/file.json')
    assert response.ok
    assert response.json() == {'hello': 'you'}

    response = requests.get('https://example.com/file.json')
    assert response.ok
    assert response.json() == {'hello': 'world'}

    response = requests.get('https://example.com/file.json')
    assert not response.ok
    assert response.status_code == 404


def test_get_response_cb_with_visits(local_get_visits):
    response = requests.get('https://example.com/file.json')
    assert response.ok
    assert response.json() == {'hello': 'you'}

    response = requests.get('https://example.com/other.json')
    assert response.ok
    assert response.json() == "foobar"

    response = requests.get('https://example.com/file.json')
    assert response.ok
    assert response.json() == {'hello': 'world'}

    response = requests.get('https://example.com/other.json')
    assert not response.ok
    assert response.status_code == 404

    response = requests.get('https://example.com/file.json')
    assert not response.ok
    assert response.status_code == 404


def test_get_response_cb_no_visit(local_get):
    response = requests.get('https://example.com/file.json')
    assert response.ok
    assert response.json() == {'hello': 'you'}

    response = requests.get('https://example.com/file.json')
    assert response.ok
    assert response.json() == {'hello': 'you'}


local_get_ignore = local_get_factory(
    ignore_urls=['https://example.com/file.json'],
    has_multi_visit=False,
)


def test_get_response_cb_ignore_url(local_get_ignore):
    response = requests.get('https://example.com/file.json')
    assert not response.ok
    assert response.status_code == 404


local_get_ignore_and_visit = local_get_factory(
    ignore_urls=['https://example.com/file.json'],
    has_multi_visit=True,
)


def test_get_response_cb_ignore_url_with_visit(local_get_ignore_and_visit):
    response = requests.get('https://example.com/file.json')
    assert not response.ok
    assert response.status_code == 404

    response = requests.get('https://example.com/file.json')
    assert not response.ok
    assert response.status_code == 404


def test_data_dir(datadir):
    expected_datadir = path.join(path.abspath(path.dirname(__file__)), 'data')
    assert datadir == expected_datadir
