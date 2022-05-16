# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import time
from typing import Dict, Iterator, List, Optional, Union

import pytest
import requests_mock

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


def fake_time_sleep(duration: float, sleep_calls: Optional[List[float]] = None):
    """Record calls to time.sleep in the sleep_calls list"""
    if duration < 0:
        raise ValueError("Can't sleep for a negative amount of time!")
    if sleep_calls is not None:
        sleep_calls.append(duration)


def fake_time_time():
    """Return 0 when running time.time()"""
    return 0


@pytest.fixture
def monkeypatch_sleep_calls(monkeypatch) -> Iterator[List[float]]:
    """Monkeypatch `time.time` and `time.sleep`. Returns a list cumulating the arguments
    passed to time.sleep()."""
    sleeps: List[float] = []
    monkeypatch.setattr(time, "sleep", lambda d: fake_time_sleep(d, sleeps))
    monkeypatch.setattr(time, "time", fake_time_time)
    yield sleeps


@pytest.fixture()
def num_before_ratelimit() -> int:
    """Number of successful requests before the ratelimit hits"""
    return 0


@pytest.fixture()
def num_ratelimit() -> Optional[int]:
    """Number of rate-limited requests; None means infinity"""
    return None


@pytest.fixture()
def ratelimit_reset() -> Optional[int]:
    """Value of the X-Ratelimit-Reset header on ratelimited responses"""
    return None


def github_ratelimit_callback(
    request: requests_mock.request._RequestObjectProxy,
    context: requests_mock.response._Context,
    ratelimit_reset: Optional[int],
) -> Dict[str, str]:
    """Return a rate-limited GitHub API response."""
    # Check request headers
    assert request.headers["Accept"] == "application/vnd.github.v3+json"
    assert request.headers["User-Agent"] is not None
    if "Authorization" in request.headers:
        context.status_code = 429
    else:
        context.status_code = 403

    if ratelimit_reset is not None:
        context.headers["X-Ratelimit-Reset"] = str(ratelimit_reset)

    return {
        "message": "API rate limit exceeded for <IP>.",
        "documentation_url": "https://developer.github.com/v3/#rate-limiting",
    }


def github_repo(i: int) -> Dict[str, Union[int, str]]:
    """Basic repository information returned by the GitHub API"""

    repo: Dict[str, Union[int, str]] = {
        "id": i,
        "html_url": f"https://github.com/origin/{i}",
    }

    # Set the pushed_at date on one of the origins
    if i == 4321:
        repo["pushed_at"] = "2018-11-08T13:16:24Z"

    return repo


HTTP_GH_API_URL = "https://api.github.com/repositories"


def github_response_callback(
    request: requests_mock.request._RequestObjectProxy,
    context: requests_mock.response._Context,
    page_size: int = 1000,
    origin_count: int = 10000,
) -> List[Dict[str, Union[str, int]]]:
    """Return minimal GitHub API responses for the common case where the loader
    hasn't been rate-limited"""
    # Check request headers
    assert request.headers["Accept"] == "application/vnd.github.v3+json"
    assert request.headers["User-Agent"] is not None

    # Check request parameters: per_page == 1000, since = last_repo_id
    assert "per_page" in request.qs
    assert request.qs["per_page"] == [str(page_size)]
    assert "since" in request.qs

    since = int(request.qs["since"][0])

    next_page = since + page_size
    if next_page < origin_count:
        # the first id for the next page is within our origin count; add a Link
        # header to the response
        next_url = f"{HTTP_GH_API_URL}?per_page={page_size}&since={next_page}"
        context.headers["Link"] = f"<{next_url}>; rel=next"

    return [github_repo(i) for i in range(since + 1, min(next_page, origin_count) + 1)]


@pytest.fixture()
def requests_ratelimited(
    num_before_ratelimit: int,
    num_ratelimit: Optional[int],
    ratelimit_reset: Optional[int],
) -> Iterator[requests_mock.Mocker]:
    """Mock requests to the GitHub API, returning a rate-limiting status code
    after `num_before_ratelimit` requests.

    GitHub does inconsistent rate-limiting:
      - Anonymous requests return a 403 status code
      - Authenticated requests return a 429 status code, with an
        X-Ratelimit-Reset header.

    This fixture takes multiple arguments (which can be overridden with a
    :func:`pytest.mark.parametrize` parameter):
     - num_before_ratelimit: the global number of requests until the
       ratelimit triggers
     - num_ratelimit: the number of requests that return a
       rate-limited response.
     - ratelimit_reset: the timestamp returned in X-Ratelimit-Reset if the
       request is authenticated.

    The default values set in the previous fixtures make all requests return a rate
    limit response.
    """
    current_request = 0

    def response_callback(request, context):
        nonlocal current_request
        current_request += 1
        if num_before_ratelimit < current_request and (
            num_ratelimit is None
            or current_request < num_before_ratelimit + num_ratelimit + 1
        ):
            return github_ratelimit_callback(request, context, ratelimit_reset)
        else:
            return github_response_callback(request, context)

    with requests_mock.Mocker() as mock:
        mock.get(HTTP_GH_API_URL, json=response_callback)
        yield mock


@pytest.fixture
def github_credentials() -> List[Dict[str, str]]:
    """Return a static list of GitHub credentials"""
    return sorted(
        [{"username": f"swh{i:d}", "token": f"token-{i:d}"} for i in range(3)]
        + [
            {"username": f"swh-legacy{i:d}", "password": f"token-legacy-{i:d}"}
            for i in range(3)
        ],
        key=lambda c: c["username"],
    )


@pytest.fixture
def all_tokens(github_credentials) -> List[str]:
    """Return the list of tokens matching the static credential"""

    return [t.get("token", t.get("password")) for t in github_credentials]


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

    res = github_session.request(f"{HTTP_GH_API_URL}?per_page=1000&since=10")
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
        github_session.request(f"{HTTP_GH_API_URL}?per_page=1000&since=10")

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
