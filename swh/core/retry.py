# Copyright (C) 2023-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Callable

from requests.exceptions import ChunkedEncodingError, ConnectionError, HTTPError
from requests.status_codes import codes
from tenacity import retry as tenacity_retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential


def is_throttling_exception(e: Exception) -> bool:
    """
    Checks if an exception is a :class:`requests.exception.HTTPError` for
    a response with status code 429 (too many requests).
    """
    return (
        isinstance(e, HTTPError)
        and e.response is not None
        and e.response.status_code == codes.too_many_requests
    )


def is_retryable_exception(e: Exception) -> bool:
    """
    Checks if an exception is worth retrying (connection, throttling or a server error).
    """
    is_connection_error = isinstance(e, (ConnectionError, ChunkedEncodingError))
    is_500_error = (
        isinstance(e, HTTPError)
        and e.response is not None
        and e.response.status_code >= 500
    )

    return is_connection_error or is_throttling_exception(e) or is_500_error


def retry_if_exception(retry_state, predicate: Callable[[Exception], bool]) -> bool:
    """
    Custom tenacity retry predicate for handling exceptions with the given predicate.
    """
    attempt = retry_state.outcome
    if attempt.failed:
        exception = attempt.exception()
        return predicate(exception)
    return False


def retry_policy_generic(retry_state) -> bool:
    """
    Custom tenacity retry predicate for handling failed requests:
        - ConnectionError
        - Server errors (status >= 500)
        - Throttling errors (status == 429)

    This does not handle 403, 404 or other status codes.
    """
    return retry_if_exception(retry_state, is_retryable_exception)


WAIT_EXP_BASE = 10
MAX_NUMBER_ATTEMPTS = 5


def http_retry(
    retry=retry_policy_generic,
    wait=wait_exponential(exp_base=WAIT_EXP_BASE),
    stop=stop_after_attempt(max_attempt_number=MAX_NUMBER_ATTEMPTS),
    **retry_args,
):
    """
    Decorator based on :mod:`tenacity` for retrying a function possibly raising
    :class:`requests.exception.HTTPError` for status code 429 (too many requests)
    or >= 500.

    It provides a default configuration that should work properly in most
    cases but all :func:`tenacity.retry` parameters can also be overridden in client
    code.

    When the maximum of attempts is reached, the ``HTTPError`` exception will then
    be reraised.

    Args:
        retry: function defining request retry condition (default to 429 status code)
            https://tenacity.readthedocs.io/en/latest/#whether-to-retry

        wait: function defining wait strategy before retrying (default to exponential
            backoff) https://tenacity.readthedocs.io/en/latest/#waiting-before-retrying

        stop: function defining when to stop retrying (default after 5 attempts)
            https://tenacity.readthedocs.io/en/latest/#stopping

    """
    return tenacity_retry(retry=retry, wait=wait, stop=stop, reraise=True, **retry_args)
