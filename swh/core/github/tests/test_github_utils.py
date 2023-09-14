# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import itertools
import logging
from unittest.mock import call, patch

import pytest
import requests
from requests.exceptions import ChunkedEncodingError, ConnectionError
from tenacity import RetryError

from swh.core.github.pytest_plugin import HTTP_GITHUB_API_URL
from swh.core.github.utils import (
    GitHubSession,
    _sanitize_github_url,
    _url_github_api,
    get_canonical_github_origin_url,
)

KNOWN_GH_REPO = "https://github.com/user/repo"
KNOWN_GH_REPO2 = "https://github.com/user/reposit"


@patch("time.sleep", return_value=None)
@patch("swh.core.github.utils.requests.Session.get")
@pytest.mark.parametrize("exception", [ChunkedEncodingError, ConnectionError])
def test_retry(mock_requests, monkeypatch_sleep_calls, exception):
    user_repo = "test/test"
    html_input_url = f"https://github.com/{user_repo}"

    mock_requests.side_effect = exception("Request failure")

    with pytest.raises(RetryError, match=exception.__name__):
        get_canonical_github_origin_url(html_input_url)


@pytest.mark.parametrize(
    "user_repo, expected_url",
    [
        ("user/repo.git", KNOWN_GH_REPO),
        ("user/repo.git/", KNOWN_GH_REPO),
        ("user/repo/", KNOWN_GH_REPO),
        ("user/repo", KNOWN_GH_REPO),
        ("user/repo/.git", KNOWN_GH_REPO),
        ("user/reposit.git", KNOWN_GH_REPO2),
        ("user/reposit.git/", KNOWN_GH_REPO2),
        ("user/reposit/", KNOWN_GH_REPO2),
        ("user/reposit", KNOWN_GH_REPO2),
        ("user/reposit/.git", KNOWN_GH_REPO2),
        ("unknown/page", None),  # unknown gh origin returns None
        ("user/with/deps", None),  # url kind is not dealt with
    ],
)
def test_get_canonical_github_origin_url(
    user_repo, expected_url, requests_mock, github_credentials
):
    """It should return a canonical github origin when it exists, None otherwise"""
    for separator in ["/", ":"]:
        for prefix in [
            "http://",
            "https://",
            "git://",
            "ssh://",
            "//",
            "git@",
            "ssh://git@",
            "https://${env.GITHUB_TOKEN_USR}:${env.GITHUB_TOKEN_PSW}@",
            "[fetch=]git@",
        ]:
            html_input_url = f"{prefix}github.com{separator}{user_repo}"
            html_url = f"https://github.com/{user_repo}"
            api_url = _url_github_api(_sanitize_github_url(user_repo))

            if expected_url is not None:
                status_code = 200
                response = {"html_url": _sanitize_github_url(html_url)}
            else:
                status_code = 404
                response = {}

            requests_mock.get(api_url, [{"status_code": status_code, "json": response}])

            # anonymous
            assert get_canonical_github_origin_url(html_input_url) == expected_url

            # with credentials
            assert (
                get_canonical_github_origin_url(
                    html_input_url, credentials=github_credentials
                )
                == expected_url
            )

            # anonymous
            assert (
                GitHubSession(
                    user_agent="GitHub Session Test",
                ).get_canonical_url(html_input_url)
                == expected_url
            )

            # with credentials
            assert (
                GitHubSession(
                    user_agent="GitHub Session Test", credentials=github_credentials
                ).get_canonical_url(html_input_url)
                == expected_url
            )


def test_get_canonical_github_origin_url_not_gh_origin():
    """It should return the input url when that origin is not a github one"""
    url = "https://example.org"
    assert get_canonical_github_origin_url(url) == url

    assert (
        GitHubSession(
            user_agent="GitHub Session Test",
        ).get_canonical_url(url)
        == url
    )


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
    mocker,
    github_requests_ratelimited,
    num_ratelimit,
    monkeypatch_sleep_calls,
    github_credentials,
):
    """GitHubSession should recover from hitting the rate-limit once"""
    caplog.set_level(logging.DEBUG, "swh.core.github.utils")

    github_session = GitHubSession(
        user_agent="GitHub Session Test", credentials=github_credentials
    )

    statsd_report = mocker.patch.object(github_session.statsd, "_report")

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

    username0 = github_session.credentials[0]["username"]
    username1 = github_session.credentials[1]["username"]
    tags0 = {"username": username0, "http_status": 403}
    tags1 = {"username": username1, "http_status": 200}
    assert [c for c in statsd_report.mock_calls] == [
        call("requests_total", "c", 1, {"username": username0}, 1),
        call("responses_total", "c", 1, tags0, 1),
        call("remaining_requests", "g", 999, {"username": username0}, 1),
        call("rate_limited_responses_total", "c", 1, {"username": username0}, 1),
        call("sleep_seconds_total", "c", 1, None, 1),
        call("requests_total", "c", 1, {"username": username1}, 1),
        call("responses_total", "c", 1, tags1, 1),
        call("remaining_requests", "g", 998, {"username": username1}, 1),
    ]
    assert github_session.statsd.constant_tags == {
        "api_type": "github",
        "api_instance": "github",
    }


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
    mocker,
    github_requests_ratelimited,
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

    statsd_report = mocker.patch.object(github_session.statsd, "_report")

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

    username0 = github_session.credentials[0]["username"]

    def ok_request_calls(user, remaining):
        return [
            call("requests_total", "c", 1, {"username": user}, 1),
            call("responses_total", "c", 1, {"username": user, "http_status": 200}, 1),
            call("remaining_requests", "g", remaining, {"username": user}, 1),
        ]

    def ratelimited_request_calls(user):
        return [
            call("requests_total", "c", 1, {"username": user}, 1),
            call("responses_total", "c", 1, {"username": user, "http_status": 403}, 1),
            call("remaining_requests", "g", 0, {"username": user}, 1),
            call("reset_seconds", "g", ratelimit_reset, {"username": user}, 1),
            call("rate_limited_responses_total", "c", 1, {"username": user}, 1),
            call("sleep_seconds_total", "c", 1, None, 1),
        ]

    expected_calls_groups = (
        # Successful requests
        [ok_request_calls(username0, n - 1) for n in range(num_before_ratelimit, 0, -1)]
        # Then rate-limited failures, cycling through tokens
        + [
            ratelimited_request_calls(
                github_session.credentials[n % len(github_credentials)]["username"]
            )
            for n in range(num_ratelimit)
        ]
        # And finally, a long sleep and the successful request
        + [
            [call("sleep_seconds_total", "c", ratelimit_reset + 1, None, 1)],
            ok_request_calls(
                github_session.credentials[num_ratelimit % len(github_credentials)][
                    "username"
                ],
                1000 - num_ratelimit - 1,
            ),
        ]
    )
    expected_calls = list(itertools.chain.from_iterable(expected_calls_groups))
    assert [c for c in statsd_report.mock_calls] == expected_calls
    assert github_session.statsd.constant_tags == {
        "api_type": "github",
        "api_instance": "github",
    }


# Same as before, but with no credentials
@pytest.mark.parametrize(
    "num_before_ratelimit, num_ratelimit, ratelimit_reset",
    [(5, 6, 123456)],
)
def test_github_session_ratelimit_reset_sleep_anonymous(
    caplog,
    mocker,
    github_requests_ratelimited,
    monkeypatch_sleep_calls,
    num_before_ratelimit,
    num_ratelimit,
    ratelimit_reset,
):
    """GitHubSession should handle rate-limit with authentication tokens."""
    caplog.set_level(logging.DEBUG, "swh.core.github.utils")

    github_session = GitHubSession(user_agent="GitHub Session Test")

    statsd_report = mocker.patch.object(github_session.statsd, "_report")

    for _ in range(num_ratelimit):
        github_session.request(f"{HTTP_GITHUB_API_URL}?per_page=1000&since=10")

    # No credentials, so we immediately sleep for a long time
    expected_sleep_calls = [ratelimit_reset + 1] * num_ratelimit
    assert monkeypatch_sleep_calls == expected_sleep_calls

    found_exhaustion_message = False
    for record in caplog.records:
        if record.levelname == "INFO":
            if "Rate limits exhausted for all tokens" in record.message:
                found_exhaustion_message = True
                break

    assert found_exhaustion_message is True

    user = "anonymous"

    def ok_request_calls(remaining):
        return [
            call("requests_total", "c", 1, {"username": user}, 1),
            call("responses_total", "c", 1, {"username": user, "http_status": 200}, 1),
            call("remaining_requests", "g", remaining, {"username": user}, 1),
        ]

    def ratelimited_request_calls():
        return [
            call("requests_total", "c", 1, {"username": user}, 1),
            call("responses_total", "c", 1, {"username": user, "http_status": 403}, 1),
            call("remaining_requests", "g", 0, {"username": user}, 1),
            call("reset_seconds", "g", ratelimit_reset, {"username": user}, 1),
            call("rate_limited_responses_total", "c", 1, {"username": user}, 1),
            call("sleep_seconds_total", "c", ratelimit_reset + 1, None, 1),
        ]

    expected_calls_groups = (
        # Successful requests
        [ok_request_calls(n - 1) for n in range(num_before_ratelimit, 0, -1)]
        # Then rate-limited failures, each with a long sleep
        + [ratelimited_request_calls() for n in range(num_ratelimit)]
        # And finally, the successful request
        + [
            ok_request_calls(
                1000 - num_ratelimit - 1,
            ),
        ]
    )
    expected_calls = list(itertools.chain.from_iterable(expected_calls_groups))
    assert [c for c in statsd_report.mock_calls] == expected_calls
    assert github_session.statsd.constant_tags == {
        "api_type": "github",
        "api_instance": "github",
    }


def test_github_session_get_repo_metadata_success(requests_mock):
    user_repo = KNOWN_GH_REPO.replace("https://github.com/", "")
    repo_metadata = {"html_url": KNOWN_GH_REPO}
    requests_mock.get(_url_github_api(user_repo), json=repo_metadata)

    gh_session = GitHubSession(user_agent="GitHub Session Test")
    assert gh_session.get_repository_metadata(KNOWN_GH_REPO) == repo_metadata


def test_github_session_get_repo_metadata_failure(requests_mock):
    unknown_user_repo = KNOWN_GH_REPO2.replace("https://github.com/", "")
    requests_mock.get(_url_github_api(unknown_user_repo), status_code=404)

    gh_session = GitHubSession(user_agent="GitHub Session Test")
    with pytest.raises(requests.HTTPError):
        gh_session.get_repository_metadata(KNOWN_GH_REPO2)
