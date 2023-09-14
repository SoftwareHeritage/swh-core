# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.core.collections import SortedList

parametrize = pytest.mark.parametrize(
    "items",
    [
        [1, 2, 3, 4, 5, 6, 10, 100],
        [10, 100, 6, 5, 4, 3, 2, 1],
        [10, 4, 5, 6, 1, 2, 3, 100],
    ],
)


@parametrize
def test_sorted_list_iter(items):
    list1 = SortedList()
    for item in items:
        list1.add(item)
    assert list(list1) == sorted(items)

    list2 = SortedList(items)
    assert list(list2) == sorted(items)


@parametrize
def test_sorted_list_iter__key(items):
    list1 = SortedList(key=lambda item: -item)
    for item in items:
        list1.add(item)
    assert list(list1) == list(reversed(sorted(items)))

    list2 = SortedList(items, key=lambda item: -item)
    assert list(list2) == list(reversed(sorted(items)))


@parametrize
def test_sorted_list_iter_from(items):
    list_ = SortedList(items)
    for split in items:
        expected = sorted(item for item in items if item >= split)
        assert list(list_.iter_from(split)) == expected, f"split: {split}"


@parametrize
def test_sorted_list_iter_from__key(items):
    list_ = SortedList(items, key=lambda item: -item)
    for split in items:
        expected = reversed(sorted(item for item in items if item <= split))
        assert list(list_.iter_from(-split)) == list(expected), f"split: {split}"


@parametrize
def test_sorted_list_iter_after(items):
    list_ = SortedList(items)
    for split in items:
        expected = sorted(item for item in items if item > split)
        assert list(list_.iter_after(split)) == expected, f"split: {split}"


@parametrize
def test_sorted_list_iter_after__key(items):
    list_ = SortedList(items, key=lambda item: -item)
    for split in items:
        expected = reversed(sorted(item for item in items if item < split))
        assert list(list_.iter_after(-split)) == list(expected), f"split: {split}"


@parametrize
def test_contains(items):
    list_ = SortedList()
    for i in range(len(items)):
        for item in items[0:i]:
            assert item in list_
        for item in items[i:]:
            assert item not in list_

        list_.add(items[i])

    for item in items:
        assert item in list_
