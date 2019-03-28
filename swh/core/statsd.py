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
#
# Vastly adapted for integration in swh.core:
#
# - Removed python < 3.5 compat code
# - trimmed the imports down to be a single module
# - adjust some options:
#   - drop unix socket connection option
#   - add environment variable support for setting the statsd host and
#     port (pulled the idea from the main python statsd module)
#   - only send timer metrics in milliseconds (that's what
#     prometheus-statsd-exporter expects)
#   - drop DataDog-specific metric types (that are unsupported in
#     prometheus-statsd-exporter)
# - made the tags a dict instead of a list (prometheus-statsd-exporter only
#   supports tags with a value, mirroring prometheus)
# - switch from time.time to time.monotonic
# - improve unit test coverage
# - documentation cleanup


from asyncio import iscoroutinefunction
from functools import wraps
from random import random
from time import monotonic
import itertools
import logging
import os
import socket
import warnings


log = logging.getLogger('swh.core.statsd')


class TimedContextManagerDecorator(object):
    """
    A context manager and a decorator which will report the elapsed time in
    the context OR in a function call.

    Attributes:
      elapsed (float): the elapsed time at the point of completion
    """
    def __init__(self, statsd, metric=None, error_metric=None,
                 tags=None, sample_rate=1):
        self.statsd = statsd
        self.metric = metric
        self.error_metric = error_metric
        self.tags = tags
        self.sample_rate = sample_rate
        self.elapsed = None  # this is for testing purpose

    def __call__(self, func):
        """
        Decorator which returns the elapsed time of the function call.

        Default to the function name if metric was not provided.
        """
        if not self.metric:
            self.metric = '%s.%s' % (func.__module__, func.__name__)

        # Coroutines
        if iscoroutinefunction(func):
            @wraps(func)
            async def wrapped_co(*args, **kwargs):
                start = monotonic()
                try:
                    result = await func(*args, **kwargs)
                except:  # noqa
                    self._send_error()
                    raise
                self._send(start)
                return result
            return wrapped_co

        # Others
        @wraps(func)
        def wrapped(*args, **kwargs):
            start = monotonic()
            try:
                result = func(*args, **kwargs)
            except:  # noqa
                self._send_error()
                raise
            self._send(start)
            return result
        return wrapped

    def __enter__(self):
        if not self.metric:
            raise TypeError("Cannot used timed without a metric!")
        self._start = monotonic()
        return self

    def __exit__(self, type, value, traceback):
        # Report the elapsed time of the context manager if no error.
        if type is None:
            self._send(self._start)
        else:
            self._send_error()

    def _send(self, start):
        elapsed = (monotonic() - start) * 1000
        self.statsd.timing(self.metric, elapsed,
                           tags=self.tags, sample_rate=self.sample_rate)
        self.elapsed = elapsed

    def _send_error(self):
        if self.error_metric is None:
            self.error_metric = self.metric + '_error_count'
        self.statsd.increment(self.error_metric, tags=self.tags)

    def start(self):
        """Start the timer"""
        self.__enter__()

    def stop(self):
        """Stop the timer, send the metric value"""
        self.__exit__(None, None, None)


class Statsd(object):
    """Initialize a client to send metrics to a StatsD server.

    Arguments:
      host (str): the host of the StatsD server. Defaults to localhost.
      port (int): the port of the StatsD server. Defaults to 8125.

      max_buffer_size (int): Maximum number of metrics to buffer before
        sending to the server if sending metrics in batch

      namespace (str): Namespace to prefix all metric names

      constant_tags (Dict[str, str]): Tags to attach to all metrics

    Note:
      This class also supports the following environment variables:

      STATSD_HOST
        Override the default host of the statsd server
      STATSD_PORT
        Override the default port of the statsd server
      STATSD_TAGS
        Tags to attach to every metric reported. Example value:

        "label:value,other_label:other_value"
    """

    def __init__(self, host=None, port=None, max_buffer_size=50,
                 namespace=None, constant_tags=None):
        # Connection
        if host is None:
            host = os.environ.get('STATSD_HOST') or 'localhost'
        self.host = host

        if port is None:
            port = os.environ.get('STATSD_PORT') or 8125
        self.port = int(port)

        # Socket
        self.socket = None
        self.max_buffer_size = max_buffer_size
        self._send = self._send_to_server
        self.encoding = 'utf-8'

        # Tags
        self.constant_tags = {}
        tags_envvar = os.environ.get('STATSD_TAGS', '')
        for tag in tags_envvar.split(','):
            if not tag:
                continue
            if ':' not in tag:
                warnings.warn(
                    'STATSD_TAGS needs to be in key:value format, '
                    '%s invalid' % tag,
                    UserWarning,
                )
                continue
            k, v = tag.split(':', 1)
            self.constant_tags[k] = v

        if constant_tags:
            self.constant_tags.update({
                str(k): str(v)
                for k, v in constant_tags.items()
            })

        # Namespace
        if namespace is not None:
            namespace = str(namespace)
        self.namespace = namespace

    def __enter__(self):
        self.open_buffer(self.max_buffer_size)
        return self

    def __exit__(self, type, value, traceback):
        self.close_buffer()

    def gauge(self, metric, value, tags=None, sample_rate=1):
        """
        Record the value of a gauge, optionally setting a list of tags and a
        sample rate.

        >>> statsd.gauge('users.online', 123)
        >>> statsd.gauge('active.connections', 1001, tags={"protocol": "http"})
        """
        return self._report(metric, 'g', value, tags, sample_rate)

    def increment(self, metric, value=1, tags=None, sample_rate=1):
        """
        Increment a counter, optionally setting a value, tags and a sample
        rate.

        >>> statsd.increment('page.views')
        >>> statsd.increment('files.transferred', 124)
        """
        self._report(metric, 'c', value, tags, sample_rate)

    def decrement(self, metric, value=1, tags=None, sample_rate=1):
        """
        Decrement a counter, optionally setting a value, tags and a sample
        rate.

        >>> statsd.decrement('files.remaining')
        >>> statsd.decrement('active.connections', 2)
        """
        metric_value = -value if value else value
        self._report(metric, 'c', metric_value, tags, sample_rate)

    def histogram(self, metric, value, tags=None, sample_rate=1):
        """
        Sample a histogram value, optionally setting tags and a sample rate.

        >>> statsd.histogram('uploaded.file.size', 1445)
        >>> statsd.histogram('file.count', 26, tags={"filetype": "python"})
        """
        self._report(metric, 'h', value, tags, sample_rate)

    def timing(self, metric, value, tags=None, sample_rate=1):
        """
        Record a timing, optionally setting tags and a sample rate.

        >>> statsd.timing("query.response.time", 1234)
        """
        self._report(metric, 'ms', value, tags, sample_rate)

    def timed(self, metric=None, error_metric=None, tags=None, sample_rate=1):
        """
        A decorator or context manager that will measure the distribution of a
        function's/context's run time. Optionally specify a list of tags or a
        sample rate. If the metric is not defined as a decorator, the module
        name and function name will be used. The metric is required as a
        context manager.
        ::

            @statsd.timed('user.query.time', sample_rate=0.5)
            def get_user(user_id):
                # Do what you need to ...
                pass

            # Is equivalent to ...
            with statsd.timed('user.query.time', sample_rate=0.5):
                # Do what you need to ...
                pass

            # Is equivalent to ...
            start = time.monotonic()
            try:
                get_user(user_id)
            finally:
                statsd.timing('user.query.time', time.monotonic() - start)
        """
        return TimedContextManagerDecorator(
            statsd=self, metric=metric,
            error_metric=error_metric,
            tags=tags, sample_rate=sample_rate)

    def set(self, metric, value, tags=None, sample_rate=1):
        """
        Sample a set value.

        >>> statsd.set('visitors.uniques', 999)
        """
        self._report(metric, 's', value, tags, sample_rate)

    def get_socket(self):
        """
        Return a connected socket.

        Note: connect the socket before assigning it to the class instance to
        avoid bad thread race conditions.
        """
        if not self.socket:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect((self.host, self.port))
            self.socket = sock

        return self.socket

    def open_buffer(self, max_buffer_size=50):
        """
        Open a buffer to send a batch of metrics in one packet.

        You can also use this as a context manager.

        >>> with Statsd() as batch:
        ...     batch.gauge('users.online', 123)
        ...     batch.gauge('active.connections', 1001)
        """
        self.max_buffer_size = max_buffer_size
        self.buffer = []
        self._send = self._send_to_buffer

    def close_buffer(self):
        """
        Flush the buffer and switch back to single metric packets.
        """
        self._send = self._send_to_server

        if self.buffer:
            # Only send packets if there are packets to send
            self._flush_buffer()

    def close_socket(self):
        """
        Closes connected socket if connected.
        """
        if self.socket:
            self.socket.close()
            self.socket = None

    def _report(self, metric, metric_type, value, tags, sample_rate):
        """
        Create a metric packet and send it.
        """
        if value is None:
            return

        if sample_rate != 1 and random() > sample_rate:
            return

        # Resolve the full tag list
        tags = self._add_constant_tags(tags)

        # Create/format the metric packet
        payload = "%s%s:%s|%s%s%s" % (
            (self.namespace + ".") if self.namespace else "",
            metric,
            value,
            metric_type,
            ("|@" + str(sample_rate)) if sample_rate != 1 else "",
            ("|#" + ",".join(
                "%s:%s" % (k, v)
                for (k, v) in sorted(tags.items())
            )) if tags else "",
        )
        # Send it
        self._send(payload)

    def _send_to_server(self, packet):
        try:
            # If set, use socket directly
            (self.socket or self.get_socket()).send(packet.encode('utf-8'))
        except socket.timeout:
            return
        except socket.error:
            log.debug(
                "Error submitting statsd packet."
                " Dropping the packet and closing the socket."
            )
            self.close_socket()

    def _send_to_buffer(self, packet):
        self.buffer.append(packet)
        if len(self.buffer) >= self.max_buffer_size:
            self._flush_buffer()

    def _flush_buffer(self):
        self._send_to_server("\n".join(self.buffer))
        self.buffer = []

    def _add_constant_tags(self, tags):
        return {
            str(k): str(v)
            for k, v in itertools.chain(
                    self.constant_tags.items(),
                    (tags if tags else {}).items(),
            )
        }


statsd = Statsd()
