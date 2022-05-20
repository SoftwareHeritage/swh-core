# Copyright (C) 2020-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import time
from typing import Dict, Iterator, List, Optional, Union

import pytest
import requests_mock

HTTP_GITHUB_API_URL = "https://api.github.com/repositories"


def fake_time_sleep(duration: float, sleep_calls: Optional[List[float]] = None):
    """Record calls to time.sleep in the sleep_calls list."""
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
        next_url = f"{HTTP_GITHUB_API_URL}?per_page={page_size}&since={next_page}"
        context.headers["Link"] = f"<{next_url}>; rel=next"

    return [github_repo(i) for i in range(since + 1, min(next_page, origin_count) + 1)]


@pytest.fixture()
def requests_ratelimited(
    num_before_ratelimit: int,
    num_ratelimit: Optional[int],
    ratelimit_reset: Optional[int],
) -> Iterator[requests_mock.Mocker]:
    """Mock requests to the GitHub API, returning a rate-limiting status code after
    `num_before_ratelimit` requests.

    GitHub does inconsistent rate-limiting:

    - Anonymous requests return a 403 status code
    - Authenticated requests return a 429 status code, with an X-Ratelimit-Reset header.

    This fixture takes multiple arguments (which can be overridden with a
    :func:`pytest.mark.parametrize` parameter):

    - num_before_ratelimit: the global number of requests until the ratelimit triggers
    - num_ratelimit: the number of requests that return a rate-limited response.
    - ratelimit_reset: the timestamp returned in X-Ratelimit-Reset if the request is
      authenticated.

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
        mock.get(HTTP_GITHUB_API_URL, json=response_callback)
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
