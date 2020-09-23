# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import TypeVar

from swh.core.api.classes import PagedResult as CorePagedResult
from swh.core.api.classes import stream_results

T = TypeVar("T")
TestPagedResult = CorePagedResult[T, bytes]


def test_stream_results_no_result():
    def paged_results(page_token) -> TestPagedResult:
        return TestPagedResult(results=[], next_page_token=None)

    # only 1 call, no pagination
    actual_data = stream_results(paged_results)
    assert list(actual_data) == []


def test_stream_results_no_pagination():
    input_data = [
        {"url": "something"},
        {"url": "something2"},
    ]

    def paged_results(page_token) -> TestPagedResult:
        return TestPagedResult(results=input_data, next_page_token=None)

    # only 1 call, no pagination
    actual_data = stream_results(paged_results)
    assert list(actual_data) == input_data


def test_stream_results_pagination():
    input_data = [
        {"url": "something"},
        {"url": "something2"},
    ]
    input_data2 = [
        {"url": "something3"},
    ]
    input_data3 = [
        {"url": "something4"},
    ]

    def page_results2(page_token=None) -> TestPagedResult:
        result_per_token = {
            None: TestPagedResult(results=input_data, next_page_token=b"two"),
            b"two": TestPagedResult(results=input_data2, next_page_token=b"three"),
            b"three": TestPagedResult(results=input_data3, next_page_token=None),
        }
        return result_per_token[page_token]

    # multiple calls to solve the pagination calls
    actual_data = stream_results(page_results2)
    assert list(actual_data) == input_data + input_data2 + input_data3
