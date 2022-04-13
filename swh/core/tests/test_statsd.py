# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# Initially imported from https://github.com/DataDog/datadogpy/
# at revision 62b3a3e89988dc18d78c282fe3ff5d1813917436
#
# Copyright (c) 2015, Datadog <info@datadoghq.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of Datadog nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#


import socket
import time

import pytest

from swh.core.pytest_plugin import FakeSocket
from swh.core.statsd import Statsd, TimedContextManagerDecorator


class BrokenSocket(FakeSocket):
    def send(self, payload):
        raise socket.error("Socket error")


class SlowSocket(FakeSocket):
    def send(self, payload):
        raise socket.timeout("Socket timeout")


def assert_almost_equal(a, b, delta):
    assert 0 <= abs(a - b) <= delta, f"|{a} - {b}| not within {delta}"


def test_set(statsd):
    statsd.set("set", 123)
    assert statsd.socket.recv() == "set:123|s"


def test_gauge(statsd):
    statsd.gauge("gauge", 123.4)
    assert statsd.socket.recv() == "gauge:123.4|g"


def test_counter(statsd):
    statsd.increment("page.views")
    assert statsd.socket.recv() == "page.views:1|c"

    statsd.increment("page.views", 11)
    assert statsd.socket.recv() == "page.views:11|c"

    statsd.decrement("page.views")
    assert statsd.socket.recv() == "page.views:-1|c"

    statsd.decrement("page.views", 12)
    assert statsd.socket.recv() == "page.views:-12|c"


def test_histogram(statsd):
    statsd.histogram("histo", 123.4)
    assert statsd.socket.recv() == "histo:123.4|h"


def test_tagged_gauge(statsd):
    statsd.gauge("gt", 123.4, tags={"country": "china", "age": 45})
    assert statsd.socket.recv() == "gt:123.4|g|#age:45,country:china"


def test_tagged_counter(statsd):
    statsd.increment("ct", tags={"country": "españa"})
    assert statsd.socket.recv() == "ct:1|c|#country:españa"


def test_tagged_histogram(statsd):
    statsd.histogram("h", 1, tags={"test_tag": "tag_value"})
    assert statsd.socket.recv() == "h:1|h|#test_tag:tag_value"


def test_sample_rate(statsd):
    statsd.increment("c", sample_rate=0)
    assert not statsd.socket.recv()
    for i in range(10000):
        statsd.increment("sampled_counter", sample_rate=0.3)
    assert_almost_equal(3000, len(statsd.socket.payloads), 150)
    assert statsd.socket.recv() == "sampled_counter:1|c|@0.3"


def test_tags_and_samples(statsd):
    for i in range(100):
        statsd.gauge("gst", 23, tags={"sampled": True}, sample_rate=0.9)

    assert_almost_equal(90, len(statsd.socket.payloads), 10)
    assert statsd.socket.recv() == "gst:23|g|@0.9|#sampled:True"


def test_timing(statsd):
    statsd.timing("t", 123)
    assert statsd.socket.recv() == "t:123|ms"


def test_metric_namespace(statsd):
    """
    Namespace prefixes all metric names.
    """
    statsd.namespace = "foo"
    statsd.gauge("gauge", 123.4)
    assert statsd.socket.recv() == "foo.gauge:123.4|g"


# Test Client level constant tags
def test_gauge_constant_tags(statsd):
    statsd.constant_tags = {
        "bar": "baz",
    }
    statsd.gauge("gauge", 123.4)
    assert statsd.socket.recv() == "gauge:123.4|g|#bar:baz"


def test_counter_constant_tag_with_metric_level_tags(statsd):
    statsd.constant_tags = {
        "bar": "baz",
        "foo": True,
    }
    statsd.increment("page.views", tags={"extra": "extra"})
    assert statsd.socket.recv() == "page.views:1|c|#bar:baz,extra:extra,foo:True"


def test_gauge_constant_tags_with_metric_level_tags_twice(statsd):
    metric_level_tag = {"foo": "bar"}
    statsd.constant_tags = {"bar": "baz"}
    statsd.gauge("gauge", 123.4, tags=metric_level_tag)
    assert statsd.socket.recv() == "gauge:123.4|g|#bar:baz,foo:bar"

    # sending metrics multiple times with same metric-level tags
    # should not duplicate the tags being sent
    statsd.gauge("gauge", 123.4, tags=metric_level_tag)
    assert statsd.socket.recv() == "gauge:123.4|g|#bar:baz,foo:bar"


def test_socket_error(statsd):
    statsd._socket = BrokenSocket()
    statsd.gauge("no error", 1)
    assert True, "success"


def test_socket_timeout(statsd):
    statsd._socket = SlowSocket()
    statsd.gauge("no error", 1)
    assert True, "success"


def test_timed(statsd):
    """
    Measure the distribution of a function's run time.
    """

    @statsd.timed("timed.test")
    def func(a, b, c=1, d=1):
        """docstring"""
        time.sleep(0.5)
        return (a, b, c, d)

    assert func.__name__ == "func"
    assert func.__doc__ == "docstring"

    result = func(1, 2, d=3)
    # Assert it handles args and kwargs correctly.
    assert result, (1, 2, 1 == 3)

    packet = statsd.socket.recv()
    name_value, type_ = packet.split("|")
    name, value = name_value.split(":")

    assert type_ == "ms"
    assert name == "timed.test"
    assert_almost_equal(500, float(value), 100)


def test_timed_exception(statsd):
    """
    Exception bubble out of the decorator and is reported
    to statsd as a dedicated counter.
    """

    @statsd.timed("timed.test")
    def func(a, b, c=1, d=1):
        """docstring"""
        time.sleep(0.5)
        return (a / b, c, d)

    assert func.__name__ == "func"
    assert func.__doc__ == "docstring"

    with pytest.raises(ZeroDivisionError):
        func(1, 0)

    packet = statsd.socket.recv()
    name_value, type_, tags = packet.split("|")
    name, value = name_value.split(":")

    assert type_ == "c"
    assert name == "timed.test_error_count"
    assert int(value) == 1
    assert tags == "#error_type:ZeroDivisionError"


def test_timed_no_metric(statsd):
    """
    Test using a decorator without providing a metric.
    """

    @statsd.timed()
    def func(a, b, c=1, d=1):
        """docstring"""
        time.sleep(0.5)
        return (a, b, c, d)

    assert func.__name__ == "func"
    assert func.__doc__ == "docstring"

    result = func(1, 2, d=3)
    # Assert it handles args and kwargs correctly.
    assert result, (1, 2, 1 == 3)

    packet = statsd.socket.recv()
    name_value, type_ = packet.split("|")
    name, value = name_value.split(":")

    assert type_ == "ms"
    assert name == "swh.core.tests.test_statsd.func"
    assert_almost_equal(500, float(value), 100)


def test_timed_coroutine(statsd):
    """
    Measure the distribution of a coroutine function's run time.

    Warning: Python >= 3.5 only.
    """
    import asyncio

    @statsd.timed("timed.test")
    @asyncio.coroutine
    def print_foo():
        """docstring"""
        time.sleep(0.5)
        print("foo")

    loop = asyncio.new_event_loop()
    loop.run_until_complete(print_foo())
    loop.close()

    # Assert
    packet = statsd.socket.recv()
    name_value, type_ = packet.split("|")
    name, value = name_value.split(":")

    assert type_ == "ms"
    assert name == "timed.test"
    assert_almost_equal(500, float(value), 100)


def test_timed_context(statsd):
    """
    Measure the distribution of a context's run time.
    """
    # In milliseconds
    with statsd.timed("timed_context.test") as timer:
        assert isinstance(timer, TimedContextManagerDecorator)
        time.sleep(0.5)

    packet = statsd.socket.recv()
    name_value, type_ = packet.split("|")
    name, value = name_value.split(":")

    assert type_ == "ms"
    assert name == "timed_context.test"
    assert_almost_equal(500, float(value), 100)
    assert_almost_equal(500, timer.elapsed, 100)


def test_timed_context_exception(statsd):
    """
    Exception bubbles out of the `timed` context manager and is
    reported to statsd as a dedicated counter.
    """

    class ContextException(Exception):
        pass

    def func(statsd):
        with statsd.timed("timed_context.test"):
            time.sleep(0.5)
            raise ContextException()

    # Ensure the exception was raised.
    with pytest.raises(ContextException):
        func(statsd)

    # Ensure the timing was recorded.
    packet = statsd.socket.recv()
    name_value, type_, tags = packet.split("|")
    name, value = name_value.split(":")

    assert type_ == "c"
    assert name == "timed_context.test_error_count"
    assert int(value) == 1
    assert tags == "#error_type:ContextException"


def test_timed_context_no_metric_name_exception(statsd):
    """Test that an exception occurs if using a context manager without a
    metric name.
    """

    def func(statsd):
        with statsd.timed():
            time.sleep(0.5)

    # Ensure the exception was raised.
    with pytest.raises(TypeError):
        func(statsd)

    # Ensure the timing was recorded.
    packet = statsd.socket.recv()
    assert packet is None


def test_timed_start_stop_calls(statsd):
    timer = statsd.timed("timed_context.test")
    timer.start()
    time.sleep(0.5)
    timer.stop()

    packet = statsd.socket.recv()
    name_value, type_ = packet.split("|")
    name, value = name_value.split(":")

    assert type_ == "ms"
    assert name == "timed_context.test"
    assert_almost_equal(500, float(value), 100)


def test_batched(statsd):
    statsd.open_buffer()
    statsd.gauge("page.views", 123)
    statsd.timing("timer", 123)
    statsd.close_buffer()

    assert statsd.socket.recv() == "page.views:123|g\ntimer:123|ms"


def test_context_manager():
    fake_socket = FakeSocket()
    with Statsd() as statsd:
        statsd._socket = fake_socket
        statsd.gauge("page.views", 123)
        statsd.timing("timer", 123)

    assert fake_socket.recv() == "page.views:123|g\ntimer:123|ms"


def test_batched_buffer_autoflush():
    fake_socket = FakeSocket()
    with Statsd() as statsd:
        statsd._socket = fake_socket
        for i in range(51):
            statsd.increment("mycounter")
        assert "\n".join(["mycounter:1|c" for i in range(50)]) == fake_socket.recv()

    assert fake_socket.recv() == "mycounter:1|c"


def test_module_level_instance(statsd):
    from swh.core.statsd import statsd

    assert isinstance(statsd, Statsd)


def test_instantiating_does_not_connect():
    local_statsd = Statsd()
    assert local_statsd._socket is None


def test_accessing_socket_opens_socket():
    local_statsd = Statsd()
    try:
        assert local_statsd.socket is not None
    finally:
        local_statsd.close_socket()


def test_accessing_socket_multiple_times_returns_same_socket():
    local_statsd = Statsd()
    fresh_socket = FakeSocket()
    local_statsd._socket = fresh_socket
    assert fresh_socket == local_statsd.socket
    assert FakeSocket() != local_statsd.socket


def test_tags_from_environment(monkeypatch):
    monkeypatch.setenv("STATSD_TAGS", "country:china,age:45")
    statsd = Statsd()
    statsd._socket = FakeSocket()
    statsd.gauge("gt", 123.4)
    assert statsd.socket.recv() == "gt:123.4|g|#age:45,country:china"


def test_tags_from_environment_with_substitution(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "sweethome")
    monkeypatch.setenv("PORT", "42")
    monkeypatch.setenv(
        "STATSD_TAGS", "country:china,age:45,host:$HOSTNAME,port:${PORT}"
    )
    statsd = Statsd()
    statsd._socket = FakeSocket()
    statsd.gauge("gt", 123.4)
    assert (
        statsd.socket.recv()
        == "gt:123.4|g|#age:45,country:china,host:sweethome,port:42"
    )


def test_tags_from_environment_and_constant(monkeypatch):
    monkeypatch.setenv("STATSD_TAGS", "country:china,age:45")
    statsd = Statsd(constant_tags={"country": "canada"})
    statsd._socket = FakeSocket()
    statsd.gauge("gt", 123.4)
    assert statsd.socket.recv() == "gt:123.4|g|#age:45,country:canada"


def test_tags_from_environment_warning(monkeypatch):
    monkeypatch.setenv("STATSD_TAGS", "valid:tag,invalid_tag")
    with pytest.warns(UserWarning) as record:
        statsd = Statsd()

    assert len(record) == 1
    assert "invalid_tag" in record[0].message.args[0]
    assert "valid:tag" not in record[0].message.args[0]
    assert statsd.constant_tags == {"valid": "tag"}


def test_gauge_doesnt_send_none(statsd):
    statsd.gauge("metric", None)
    assert statsd.socket.recv() is None


def test_increment_doesnt_send_none(statsd):
    statsd.increment("metric", None)
    assert statsd.socket.recv() is None


def test_decrement_doesnt_send_none(statsd):
    statsd.decrement("metric", None)
    assert statsd.socket.recv() is None


def test_timing_doesnt_send_none(statsd):
    statsd.timing("metric", None)
    assert statsd.socket.recv() is None


def test_histogram_doesnt_send_none(statsd):
    statsd.histogram("metric", None)
    assert statsd.socket.recv() is None


def test_param_host(monkeypatch):
    monkeypatch.setenv("STATSD_HOST", "test-value")
    monkeypatch.setenv("STATSD_PORT", "")
    local_statsd = Statsd(host="actual-test-value")

    assert local_statsd.host == "actual-test-value"
    assert local_statsd.port == 8125


def test_param_port(monkeypatch):
    monkeypatch.setenv("STATSD_HOST", "")
    monkeypatch.setenv("STATSD_PORT", "12345")
    local_statsd = Statsd(port=4321)
    assert local_statsd.host == "localhost"
    assert local_statsd.port == 4321


def test_envvar_host(monkeypatch):
    monkeypatch.setenv("STATSD_HOST", "test-value")
    monkeypatch.setenv("STATSD_PORT", "")
    local_statsd = Statsd()
    assert local_statsd.host == "test-value"
    assert local_statsd.port == 8125


def test_envvar_port(monkeypatch):
    monkeypatch.setenv("STATSD_HOST", "")
    monkeypatch.setenv("STATSD_PORT", "12345")
    local_statsd = Statsd()

    assert local_statsd.host == "localhost"
    assert local_statsd.port == 12345


def test_namespace_added():
    local_statsd = Statsd(namespace="test-namespace")
    local_statsd._socket = FakeSocket()

    local_statsd.gauge("gauge", 123.4)
    assert local_statsd.socket.recv() == "test-namespace.gauge:123.4|g"


def test_contextmanager_empty(statsd):
    with statsd:
        assert True, "success"


def test_contextmanager_buffering(statsd):
    with statsd as s:
        s.gauge("gauge", 123.4)
        s.gauge("gauge_other", 456.78)
        assert s.socket.recv() is None

    assert statsd.socket.recv() == "gauge:123.4|g\ngauge_other:456.78|g"


def test_timed_elapsed(statsd):
    with statsd.timed("test_timer") as t:
        pass

    assert t.elapsed >= 0
    assert statsd.socket.recv() == "test_timer:%s|ms" % t.elapsed


def test_status_gauge(statsd):
    with statsd.status_gauge("test_status_gauge", ["s1", "s2", "s3"]) as set_status:
        set_status("s1")
        set_status("s2")
        set_status("s3")

    # enter the context manager: initialisation of gauges for listed statuses
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s1"
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s2"
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s3"
    # set_status("s1")
    assert statsd.socket.recv() == "test_status_gauge:1|g|#status:s1"
    # set_status("s2")
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s1"
    assert statsd.socket.recv() == "test_status_gauge:1|g|#status:s2"
    # set_status("s3")
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s2"
    assert statsd.socket.recv() == "test_status_gauge:1|g|#status:s3"
    # exit the context manager: cleanup gauges
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s1"
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s2"
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s3"


def test_status_gauge_error(statsd):
    with statsd.status_gauge("test_status_gauge", ["s1", "s2", "s3"]) as set_status:
        with pytest.raises(ValueError):
            set_status("s4")


def test_status_gauge_repeated(statsd):
    with statsd.status_gauge("test_status_gauge", ["s1", "s2", "s3"]) as set_status:
        set_status("s1")
        set_status("s1")
        set_status("s1")

    # enter the context manager: initialisation of gauges for listed statuses
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s1"
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s2"
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s3"
    # set_status("s1")
    assert statsd.socket.recv() == "test_status_gauge:1|g|#status:s1"
    # set_status("s1")
    assert statsd.socket.recv() == "test_status_gauge:1|g|#status:s1"
    # set_status("s1")
    assert statsd.socket.recv() == "test_status_gauge:1|g|#status:s1"
    # exit the context manager: cleanup gauges
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s1"
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s2"
    assert statsd.socket.recv() == "test_status_gauge:0|g|#status:s3"
