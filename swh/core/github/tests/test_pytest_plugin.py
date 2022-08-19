# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import time

import pytest

from swh.core.github.pytest_plugin import fake_time_sleep, fake_time_time


@pytest.mark.parametrize("duration", [10, 20, -1])
def test_fake_time_sleep(duration):

    if duration < 0:
        with pytest.raises(ValueError, match="negative"):
            fake_time_sleep(duration, [])
    else:
        sleep_calls = []
        fake_time_sleep(duration, sleep_calls)
        assert duration in sleep_calls


def test_fake_time_time():
    assert fake_time_time() == 0


def test_monkeypatch_sleep_calls(monkeypatch_sleep_calls):

    sleeps = [10, 20, 30]
    for sleep in sleeps:
        # This adds the sleep number inside the monkeypatch_sleep_calls fixture
        time.sleep(sleep)
        assert sleep in monkeypatch_sleep_calls

    assert len(monkeypatch_sleep_calls) == len(sleeps)
    # This mocks time but adds nothing to the same fixture
    time.time()
    assert len(monkeypatch_sleep_calls) == len(sleeps)


def test_num_before_ratelimit(num_before_ratelimit):
    assert num_before_ratelimit == 0


def test_ratelimit_reset(ratelimit_reset):
    assert ratelimit_reset is None


def test_num_ratelimit(num_ratelimit):
    assert num_ratelimit is None
