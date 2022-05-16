# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.core.github.utils import (
    _sanitize_github_url,
    _url_github_api,
    _url_github_html,
    get_canonical_github_origin_url,
)

KNOWN_GH_REPO = "https://github.com/user/repo"


@pytest.mark.parametrize(
    "user_repo, expected_url",
    [
        ("user/repo.git", KNOWN_GH_REPO),
        ("user/repo.git/", KNOWN_GH_REPO),
        ("user/repo/", KNOWN_GH_REPO),
        ("user/repo", KNOWN_GH_REPO),
        ("user/repo/.git", KNOWN_GH_REPO),
        # edge cases
        ("https://github.com/unknown-page", None),  # unknown gh origin returns None
        ("user/repo/with/some/deps", None),  # url kind is not dealt with for now
    ],
)
def test_get_canonical_github_origin_url(user_repo, expected_url, requests_mock):
    """It should return a canonical github origin when it exists, None otherwise"""
    html_url = _url_github_html(user_repo)
    api_url = _url_github_api(_sanitize_github_url(user_repo))

    if expected_url is not None:
        status_code = 200
        response = {"html_url": _sanitize_github_url(html_url)}
    else:
        status_code = 404
        response = {}

    requests_mock.get(api_url, [{"status_code": status_code, "json": response}])

    assert get_canonical_github_origin_url(html_url) == expected_url


def test_get_canonical_github_origin_url_not_gh_origin():
    """It should return the input url when that origin is not a github one"""
    url = "https://example.org"
    assert get_canonical_github_origin_url(url) == url
