# Copyright (C) 2018  The Software Heritage developers
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


from collections import deque
from contextlib import contextmanager
import os
import socket
import time
import unittest

import pytest

from swh.core.statsd import Statsd, TimedContextManagerDecorator


@contextmanager
def preserve_envvars(*envvars):
    """Context manager preserving the value of environment variables"""
    preserved = {}
    to_delete = object()

    for var in envvars:
        preserved[var] = os.environ.get(var, to_delete)

    yield

    for var in envvars:
        old = preserved[var]
        if old is not to_delete:
            os.environ[var] = old
        else:
            del os.environ[var]


class FakeSocket(object):
    """ A fake socket for testing. """

    def __init__(self):
        self.payloads = deque()

    def send(self, payload):
        assert type(payload) == bytes
        self.payloads.append(payload)

    def recv(self):
        try:
            return self.payloads.popleft().decode('utf-8')
        except IndexError:
            return None

    def close(self):
        pass

    def __repr__(self):
        return str(self.payloads)


class BrokenSocket(FakeSocket):
    def send(self, payload):
        raise socket.error("Socket error")


class SlowSocket(FakeSocket):
    def send(self, payload):
        raise socket.timeout("Socket timeout")


class TestStatsd(unittest.TestCase):

    def setUp(self):
        """
        Set up a default Statsd instance and mock the socket.
        """
        #
        self.statsd = Statsd()
        self.statsd.socket = FakeSocket()

    def recv(self):
        return self.statsd.socket.recv()

    def test_set(self):
        self.statsd.set('set', 123)
        assert self.recv() == 'set:123|s'

    def test_gauge(self):
        self.statsd.gauge('gauge', 123.4)
        assert self.recv() == 'gauge:123.4|g'

    def test_counter(self):
        self.statsd.increment('page.views')
        self.assertEqual('page.views:1|c', self.recv())

        self.statsd.increment('page.views', 11)
        self.assertEqual('page.views:11|c', self.recv())

        self.statsd.decrement('page.views')
        self.assertEqual('page.views:-1|c', self.recv())

        self.statsd.decrement('page.views', 12)
        self.assertEqual('page.views:-12|c', self.recv())

    def test_histogram(self):
        self.statsd.histogram('histo', 123.4)
        self.assertEqual('histo:123.4|h', self.recv())

    def test_tagged_gauge(self):
        self.statsd.gauge('gt', 123.4, tags={'country': 'china', 'age': 45})
        self.assertEqual('gt:123.4|g|#age:45,country:china', self.recv())

    def test_tagged_counter(self):
        self.statsd.increment('ct', tags={'country': 'españa'})
        self.assertEqual('ct:1|c|#country:españa', self.recv())

    def test_tagged_histogram(self):
        self.statsd.histogram('h', 1, tags={'test_tag': 'tag_value'})
        self.assertEqual('h:1|h|#test_tag:tag_value', self.recv())

    def test_sample_rate(self):
        self.statsd.increment('c', sample_rate=0)
        assert not self.recv()
        for i in range(10000):
            self.statsd.increment('sampled_counter', sample_rate=0.3)
        self.assert_almost_equal(3000, len(self.statsd.socket.payloads), 150)
        self.assertEqual('sampled_counter:1|c|@0.3', self.recv())

    def test_tags_and_samples(self):
        for i in range(100):
            self.statsd.gauge('gst', 23, tags={"sampled": True},
                              sample_rate=0.9)

        self.assert_almost_equal(90, len(self.statsd.socket.payloads), 10)
        self.assertEqual('gst:23|g|@0.9|#sampled:True', self.recv())

    def test_timing(self):
        self.statsd.timing('t', 123)
        self.assertEqual('t:123|ms', self.recv())

    def test_metric_namespace(self):
        """
        Namespace prefixes all metric names.
        """
        self.statsd.namespace = "foo"
        self.statsd.gauge('gauge', 123.4)
        self.assertEqual('foo.gauge:123.4|g', self.recv())

    # Test Client level contant tags
    def test_gauge_constant_tags(self):
        self.statsd.constant_tags = {
            'bar': 'baz',
        }
        self.statsd.gauge('gauge', 123.4)
        assert self.recv() == 'gauge:123.4|g|#bar:baz'

    def test_counter_constant_tag_with_metric_level_tags(self):
        self.statsd.constant_tags = {
            'bar': 'baz',
            'foo': True,
        }
        self.statsd.increment('page.views', tags={'extra': 'extra'})
        self.assertEqual(
            'page.views:1|c|#bar:baz,extra:extra,foo:True',
            self.recv(),
        )

    def test_gauge_constant_tags_with_metric_level_tags_twice(self):
        metric_level_tag = {'foo': 'bar'}
        self.statsd.constant_tags = {'bar': 'baz'}
        self.statsd.gauge('gauge', 123.4, tags=metric_level_tag)
        assert self.recv() == 'gauge:123.4|g|#bar:baz,foo:bar'

        # sending metrics multiple times with same metric-level tags
        # should not duplicate the tags being sent
        self.statsd.gauge('gauge', 123.4, tags=metric_level_tag)
        assert self.recv() == 'gauge:123.4|g|#bar:baz,foo:bar'

    def assert_almost_equal(self, a, b, delta):
        self.assertTrue(
            0 <= abs(a - b) <= delta,
            "%s - %s not within %s" % (a, b, delta)
        )

    def test_socket_error(self):
        self.statsd.socket = BrokenSocket()
        self.statsd.gauge('no error', 1)
        assert True, 'success'

    def test_socket_timeout(self):
        self.statsd.socket = SlowSocket()
        self.statsd.gauge('no error', 1)
        assert True, 'success'

    def test_timed(self):
        """
        Measure the distribution of a function's run time.
        """
        @self.statsd.timed('timed.test')
        def func(a, b, c=1, d=1):
            """docstring"""
            time.sleep(0.5)
            return (a, b, c, d)

        self.assertEqual('func', func.__name__)
        self.assertEqual('docstring', func.__doc__)

        result = func(1, 2, d=3)
        # Assert it handles args and kwargs correctly.
        self.assertEqual(result, (1, 2, 1, 3))

        packet = self.recv()
        name_value, type_ = packet.split('|')
        name, value = name_value.split(':')

        self.assertEqual('ms', type_)
        self.assertEqual('timed.test', name)
        self.assert_almost_equal(500, float(value), 100)

    def test_timed_exception(self):
        """
        Exception bubble out of the decorator and is reported
        to statsd as a dedicated counter.
        """
        @self.statsd.timed('timed.test')
        def func(a, b, c=1, d=1):
            """docstring"""
            time.sleep(0.5)
            return (a / b, c, d)

        self.assertEqual('func', func.__name__)
        self.assertEqual('docstring', func.__doc__)

        with self.assertRaises(ZeroDivisionError):
            func(1, 0)

        packet = self.recv()
        name_value, type_ = packet.split('|')
        name, value = name_value.split(':')

        self.assertEqual('c', type_)
        self.assertEqual('timed.test_error_count', name)
        self.assertEqual(int(value), 1)

    def test_timed_no_metric(self, ):
        """
        Test using a decorator without providing a metric.
        """

        @self.statsd.timed()
        def func(a, b, c=1, d=1):
            """docstring"""
            time.sleep(0.5)
            return (a, b, c, d)

        self.assertEqual('func', func.__name__)
        self.assertEqual('docstring', func.__doc__)

        result = func(1, 2, d=3)
        # Assert it handles args and kwargs correctly.
        self.assertEqual(result, (1, 2, 1, 3))

        packet = self.recv()
        name_value, type_ = packet.split('|')
        name, value = name_value.split(':')

        self.assertEqual('ms', type_)
        self.assertEqual('swh.core.tests.test_statsd.func', name)
        self.assert_almost_equal(500, float(value), 100)

    def test_timed_coroutine(self):
        """
        Measure the distribution of a coroutine function's run time.

        Warning: Python >= 3.5 only.
        """
        import asyncio

        @self.statsd.timed('timed.test')
        @asyncio.coroutine
        def print_foo():
            """docstring"""
            time.sleep(0.5)
            print("foo")

        loop = asyncio.get_event_loop()
        loop.run_until_complete(print_foo())
        loop.close()

        # Assert
        packet = self.recv()
        name_value, type_ = packet.split('|')
        name, value = name_value.split(':')

        self.assertEqual('ms', type_)
        self.assertEqual('timed.test', name)
        self.assert_almost_equal(500, float(value), 100)

    def test_timed_context(self):
        """
        Measure the distribution of a context's run time.
        """
        # In milliseconds
        with self.statsd.timed('timed_context.test') as timer:
            self.assertIsInstance(timer, TimedContextManagerDecorator)
            time.sleep(0.5)

        packet = self.recv()
        name_value, type_ = packet.split('|')
        name, value = name_value.split(':')

        self.assertEqual('ms', type_)
        self.assertEqual('timed_context.test', name)
        self.assert_almost_equal(500, float(value), 100)
        self.assert_almost_equal(500, timer.elapsed, 100)

    def test_timed_context_exception(self):
        """
        Exception bubbles out of the `timed` context manager and is
        reported to statsd as a dedicated counter.
        """
        class ContextException(Exception):
            pass

        def func(self):
            with self.statsd.timed('timed_context.test'):
                time.sleep(0.5)
                raise ContextException()

        # Ensure the exception was raised.
        self.assertRaises(ContextException, func, self)

        # Ensure the timing was recorded.
        packet = self.recv()
        name_value, type_ = packet.split('|')
        name, value = name_value.split(':')

        self.assertEqual('c', type_)
        self.assertEqual('timed_context.test_error_count', name)
        self.assertEqual(int(value), 1)

    def test_timed_context_no_metric_name_exception(self):
        """Test that an exception occurs if using a context manager without a
        metric name.
        """

        def func(self):
            with self.statsd.timed():
                time.sleep(0.5)

        # Ensure the exception was raised.
        self.assertRaises(TypeError, func, self)

        # Ensure the timing was recorded.
        packet = self.recv()
        self.assertEqual(packet, None)

    def test_timed_start_stop_calls(self):
        timer = self.statsd.timed('timed_context.test')
        timer.start()
        time.sleep(0.5)
        timer.stop()

        packet = self.recv()
        name_value, type_ = packet.split('|')
        name, value = name_value.split(':')

        self.assertEqual('ms', type_)
        self.assertEqual('timed_context.test', name)
        self.assert_almost_equal(500, float(value), 100)

    def test_batched(self):
        self.statsd.open_buffer()
        self.statsd.gauge('page.views', 123)
        self.statsd.timing('timer', 123)
        self.statsd.close_buffer()

        self.assertEqual('page.views:123|g\ntimer:123|ms', self.recv())

    def test_context_manager(self):
        fake_socket = FakeSocket()
        with Statsd() as statsd:
            statsd.socket = fake_socket
            statsd.gauge('page.views', 123)
            statsd.timing('timer', 123)

        self.assertEqual('page.views:123|g\ntimer:123|ms', fake_socket.recv())

    def test_batched_buffer_autoflush(self):
        fake_socket = FakeSocket()
        with Statsd() as statsd:
            statsd.socket = fake_socket
            for i in range(51):
                statsd.increment('mycounter')
            self.assertEqual(
                '\n'.join(['mycounter:1|c' for i in range(50)]),
                fake_socket.recv(),
            )

        self.assertEqual('mycounter:1|c', fake_socket.recv())

    def test_module_level_instance(self):
        from swh.core.statsd import statsd
        self.assertTrue(isinstance(statsd, Statsd))

    def test_instantiating_does_not_connect(self):
        local_statsd = Statsd()
        self.assertEqual(None, local_statsd.socket)

    def test_accessing_socket_opens_socket(self):
        local_statsd = Statsd()
        try:
            self.assertIsNotNone(local_statsd.get_socket())
        finally:
            local_statsd.socket.close()

    def test_accessing_socket_multiple_times_returns_same_socket(self):
        local_statsd = Statsd()
        fresh_socket = FakeSocket()
        local_statsd.socket = fresh_socket
        self.assertEqual(fresh_socket, local_statsd.get_socket())
        self.assertNotEqual(FakeSocket(), local_statsd.get_socket())

    def test_tags_from_environment(self):
        with preserve_envvars('STATSD_TAGS'):
            os.environ['STATSD_TAGS'] = 'country:china,age:45'
            statsd = Statsd()

        statsd.socket = FakeSocket()
        statsd.gauge('gt', 123.4)
        self.assertEqual('gt:123.4|g|#age:45,country:china',
                         statsd.socket.recv())

    def test_tags_from_environment_and_constant(self):
        with preserve_envvars('STATSD_TAGS'):
            os.environ['STATSD_TAGS'] = 'country:china,age:45'
            statsd = Statsd(constant_tags={'country': 'canada'})
        statsd.socket = FakeSocket()
        statsd.gauge('gt', 123.4)
        self.assertEqual('gt:123.4|g|#age:45,country:canada',
                         statsd.socket.recv())

    def test_tags_from_environment_warning(self):
        with preserve_envvars('STATSD_TAGS'):
            os.environ['STATSD_TAGS'] = 'valid:tag,invalid_tag'
            with pytest.warns(UserWarning) as record:
                statsd = Statsd()

        assert len(record) == 1
        assert 'invalid_tag' in record[0].message.args[0]
        assert 'valid:tag' not in record[0].message.args[0]
        assert statsd.constant_tags == {'valid': 'tag'}

    def test_gauge_doesnt_send_none(self):
        self.statsd.gauge('metric', None)
        assert self.recv() is None

    def test_increment_doesnt_send_none(self):
        self.statsd.increment('metric', None)
        assert self.recv() is None

    def test_decrement_doesnt_send_none(self):
        self.statsd.decrement('metric', None)
        assert self.recv() is None

    def test_timing_doesnt_send_none(self):
        self.statsd.timing('metric', None)
        assert self.recv() is None

    def test_histogram_doesnt_send_none(self):
        self.statsd.histogram('metric', None)
        assert self.recv() is None

    def test_param_host(self):
        with preserve_envvars('STATSD_HOST', 'STATSD_PORT'):
            os.environ['STATSD_HOST'] = 'test-value'
            os.environ['STATSD_PORT'] = ''
            local_statsd = Statsd(host='actual-test-value')

        self.assertEqual(local_statsd.host, 'actual-test-value')
        self.assertEqual(local_statsd.port, 8125)

    def test_param_port(self):
        with preserve_envvars('STATSD_HOST', 'STATSD_PORT'):
            os.environ['STATSD_HOST'] = ''
            os.environ['STATSD_PORT'] = '12345'
            local_statsd = Statsd(port=4321)

        self.assertEqual(local_statsd.host, 'localhost')
        self.assertEqual(local_statsd.port, 4321)

    def test_envvar_host(self):
        with preserve_envvars('STATSD_HOST', 'STATSD_PORT'):
            os.environ['STATSD_HOST'] = 'test-value'
            os.environ['STATSD_PORT'] = ''
            local_statsd = Statsd()

        self.assertEqual(local_statsd.host, 'test-value')
        self.assertEqual(local_statsd.port, 8125)

    def test_envvar_port(self):
        with preserve_envvars('STATSD_HOST', 'STATSD_PORT'):
            os.environ['STATSD_HOST'] = ''
            os.environ['STATSD_PORT'] = '12345'
            local_statsd = Statsd()

        self.assertEqual(local_statsd.host, 'localhost')
        self.assertEqual(local_statsd.port, 12345)

    def test_namespace_added(self):
        local_statsd = Statsd(namespace='test-namespace')
        local_statsd.socket = FakeSocket()

        local_statsd.gauge('gauge', 123.4)
        assert local_statsd.socket.recv() == 'test-namespace.gauge:123.4|g'

    def test_contextmanager_empty(self):
        with self.statsd:
            assert True, 'success'

    def test_contextmanager_buffering(self):
        with self.statsd as s:
            s.gauge('gauge', 123.4)
            s.gauge('gauge_other', 456.78)
            self.assertIsNone(s.socket.recv())

        self.assertEqual(self.recv(), 'gauge:123.4|g\ngauge_other:456.78|g')

    def test_timed_elapsed(self):
        with self.statsd.timed('test_timer') as t:
            pass

        self.assertGreaterEqual(t.elapsed, 0)
        self.assertEqual(self.recv(), 'test_timer:%s|ms' % t.elapsed)
