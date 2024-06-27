# Copyright (C) 2023-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import pytest
import requests
from requests.status_codes import codes
from tenacity.wait import wait_fixed

from swh.core.retry import MAX_NUMBER_ATTEMPTS, WAIT_EXP_BASE, http_retry

TEST_URL = "https://example.og/api/repositories"


@http_retry()
def make_request():
    response = requests.get(TEST_URL)
    response.raise_for_status()
    return response


@pytest.fixture
def mock_sleep(mocker):
    return mocker.patch("time.sleep")


def assert_sleep_calls(mocker, mock_sleep, sleep_params):
    mock_sleep.assert_has_calls([mocker.call(param) for param in sleep_params])


@pytest.mark.parametrize(
    "status_code",
    [
        codes.too_many_requests,
        codes.internal_server_error,
        codes.bad_gateway,
        codes.service_unavailable,
    ],
)
def test_http_retry(requests_mock, mocker, mock_sleep, status_code):
    data = {"result": {}}
    requests_mock.get(
        TEST_URL,
        [
            {"status_code": status_code},
            {"status_code": status_code},
            {"status_code": codes.ok, "json": data},
        ],
    )

    response = make_request()

    assert_sleep_calls(mocker, mock_sleep, [1, WAIT_EXP_BASE])

    assert response.json() == data


def test_http_retry_max_attemps(requests_mock, mocker, mock_sleep):
    requests_mock.get(
        TEST_URL,
        [{"status_code": codes.too_many_requests}] * (MAX_NUMBER_ATTEMPTS),
    )

    with pytest.raises(requests.exceptions.HTTPError) as e:
        make_request()

    assert e.value.response.status_code == codes.too_many_requests

    assert_sleep_calls(
        mocker,
        mock_sleep,
        [float(WAIT_EXP_BASE**i) for i in range(MAX_NUMBER_ATTEMPTS - 1)],
    )


@http_retry(wait=wait_fixed(WAIT_EXP_BASE))
def make_request_wait_fixed():
    response = requests.get(TEST_URL)
    response.raise_for_status()
    return response


def test_http_retry_wait_fixed(requests_mock, mocker, mock_sleep):
    requests_mock.get(
        TEST_URL,
        [
            {"status_code": codes.too_many_requests},
            {"status_code": codes.too_many_requests},
            {"status_code": codes.ok},
        ],
    )

    make_request_wait_fixed()

    assert_sleep_calls(mocker, mock_sleep, [WAIT_EXP_BASE] * 2)
