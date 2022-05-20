# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import pytest

from swh.core.github.pytest_plugin import HTTP_GITHUB_API_URL
from swh.core.github.utils import (
    GitHubSession,
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


def test_github_session_anonymous_session():
    user_agent = ("GitHub Session Test",)
    github_session = GitHubSession(
        user_agent=user_agent,
    )
    assert github_session.anonymous is True

    actual_headers = github_session.session.headers
    assert actual_headers["Accept"] == "application/vnd.github.v3+json"
    assert actual_headers["User-Agent"] == user_agent


@pytest.mark.parametrize(
    "num_ratelimit", [1]  # return a single rate-limit response, then continue
)
def test_github_session_ratelimit_once_recovery(
    caplog,
    requests_ratelimited,
    num_ratelimit,
    monkeypatch_sleep_calls,
    github_credentials,
):
    """GitHubSession should recover from hitting the rate-limit once"""
    caplog.set_level(logging.DEBUG, "swh.core.github.utils")

    github_session = GitHubSession(
        user_agent="GitHub Session Test", credentials=github_credentials
    )

    res = github_session.request(f"{HTTP_GITHUB_API_URL}?per_page=1000&since=10")
    assert res.status_code == 200

    token_users = []
    for record in caplog.records:
        if "Using authentication token" in record.message:
            token_users.append(record.args[0])

    # check that we used one more token than we saw rate limited requests
    assert len(token_users) == 1 + num_ratelimit

    # check that we slept for one second between our token uses
    assert monkeypatch_sleep_calls == [1]


def test_github_session_authenticated_credentials(
    caplog, github_credentials, all_tokens
):
    """GitHubSession should have Authorization headers set in authenticated mode"""
    caplog.set_level(logging.DEBUG, "swh.core.github.utils")

    github_session = GitHubSession(
        "GitHub Session Test", credentials=github_credentials
    )

    assert github_session.anonymous is False
    assert github_session.token_index == 0
    assert (
        sorted(github_session.credentials, key=lambda t: t["username"])
        == github_credentials
    )
    assert github_session.session.headers["Authorization"] in [
        f"token {t}" for t in all_tokens
    ]


@pytest.mark.parametrize(
    # Do 5 successful requests, return 6 ratelimits (to exhaust the credentials) with a
    # set value for X-Ratelimit-Reset, then resume listing successfully.
    "num_before_ratelimit, num_ratelimit, ratelimit_reset",
    [(5, 6, 123456)],
)
def test_github_session_ratelimit_reset_sleep(
    caplog,
    requests_ratelimited,
    monkeypatch_sleep_calls,
    num_before_ratelimit,
    num_ratelimit,
    ratelimit_reset,
    github_credentials,
):
    """GitHubSession should handle rate-limit with authentication tokens."""
    caplog.set_level(logging.DEBUG, "swh.core.github.utils")

    github_session = GitHubSession(
        user_agent="GitHub Session Test", credentials=github_credentials
    )

    for _ in range(num_ratelimit):
        github_session.request(f"{HTTP_GITHUB_API_URL}?per_page=1000&since=10")

    # We sleep 1 second every time we change credentials, then we sleep until
    # ratelimit_reset + 1
    expected_sleep_calls = len(github_credentials) * [1] + [ratelimit_reset + 1]
    assert monkeypatch_sleep_calls == expected_sleep_calls

    found_exhaustion_message = False
    for record in caplog.records:
        if record.levelname == "INFO":
            if "Rate limits exhausted for all tokens" in record.message:
                found_exhaustion_message = True
                break

    assert found_exhaustion_message is True
