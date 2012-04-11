#
#  Copyright (C) 2010-2012 Canonical Ltd. All Rights Reserved
#
#  This file is part of txzookeeper.
#
#  Authors:
#   Kapil Thangavelu
#
#  txzookeeper is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  txzookeeper is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with txzookeeper.  If not, see <http://www.gnu.org/licenses/>.
#

import json
import time
import zookeeper

from twisted.internet.defer import inlineCallbacks, fail, succeed, Deferred

from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import (
    RetryClient, retry, retry_watch,
    check_retryable, is_retryable, get_delay, sleep)

from txzookeeper import retry as retry_module


from txzookeeper.utils import retry_change

from txzookeeper.tests import ZookeeperTestCase, utils
from txzookeeper.tests.proxy import ProxyFactory
from txzookeeper.tests import test_client


class RetryCoreTests(ZookeeperTestCase):
    """Test the retry functions in isolation.
    """
    timeout = 5

    def test_is_retryable(self):
        self.assertEqual(
            is_retryable(zookeeper.SessionExpiredException()), False)
        self.assertEqual(
            is_retryable(zookeeper.ConnectionLossException()), True)
        self.assertEqual(
            is_retryable(zookeeper.OperationTimeoutException()), True)
        self.assertEqual(
            is_retryable(TypeError()), False)

    def setup_always_retryable(self):
        def check_retry(*args):
            return True

        self.patch(retry_module, "check_retryable", check_retry)

    @inlineCallbacks
    def test_sleep(self):
        t = time.time()
        yield sleep(0.5)
        self.assertTrue(time.time() - t > 0.5)

    @inlineCallbacks
    def test_retry_function(self):
        """The retry wrapper can be used for a function."""
        self.setup_always_retryable()

        results = [fail(zookeeper.ConnectionLossException()),
                   fail(zookeeper.ConnectionLossException()),
                   succeed(21)]

        def original(zebra):
            """Hello World"""
            return results.pop(0)

        result = yield retry(ZookeeperClient(), original, "magic")
        self.assertEqual(result, 21)

    @inlineCallbacks
    def test_retry_method(self):
        """The retry wrapper can be used for a method."""
        self.setup_always_retryable()

        results = [fail(zookeeper.ConnectionLossException()),
                   fail(zookeeper.ConnectionLossException()),
                   succeed(21)]

        class Foobar(object):

            def original(self, zebra):
                """Hello World"""
                return results.pop(0)

        client = ZookeeperClient()
        foo = Foobar()
        result = yield retry(client, foo.original, "magic")
        self.assertEqual(result, 21)

    @inlineCallbacks
    def test_retry_watch_function(self):
        self.setup_always_retryable()

        results = [(fail(zookeeper.ConnectionLossException()), Deferred()),
                   (fail(zookeeper.ConnectionLossException()), Deferred()),
                   (succeed(21), succeed(22))]

        def original(zebra):
            """Hello World"""
            return results.pop(0)

        client = ZookeeperClient()
        value_d, watch_d = retry_watch(client, original, "magic")
        self.assertEqual((yield value_d), 21)
        self.assertEqual((yield watch_d), 22)

    def test_check_retryable(self):
        unrecoverable_errors = [
            zookeeper.ApiErrorException(),
            zookeeper.NoAuthException(),
            zookeeper.NodeExistsException(),
            zookeeper.SessionExpiredException(),
            ]

        class _Conn(object):
            def __init__(self, **kw):
                self.__dict__.update(kw)

        error = zookeeper.ConnectionLossException()
        conn = _Conn(connected=True, unrecoverable=False)
        max_time = time.time() + 10

        for e in unrecoverable_errors:
            self.assertFalse(check_retryable(_Conn(), max_time, e))

        self.assertTrue(check_retryable(conn, max_time, error))
        self.assertFalse(check_retryable(conn, time.time() - 10, error))
        self.assertFalse(check_retryable(
            _Conn(connected=False, unrecoverable=False), max_time, error))
        self.assertFalse(check_retryable(
            _Conn(connected=True, unrecoverable=True), max_time, error))

    def test_get_delay(self):
        # Verify max value is respected
        self.assertEqual(get_delay(600 * 1000, 10), 10)
        # Verify normal calculation
        self.assertEqual(get_delay(600, 10, 30), 0.02)


class RetryClientTests(test_client.ClientTests):
    """Run the full client test suite against the retry facade.
    """
    def setUp(self):
        super(RetryClientTests, self).setUp()
        self.client = RetryClient(ZookeeperClient("127.0.0.1:2181", 3000))
        self.client2 = None

    def tearDown(self):
        if self.client.connected:
            utils.deleteTree(handle=self.client.handle)
            self.client.close()

        if self.client2 and self.client2.connected:
            self.client2.close()

        super(RetryClientTests, self).tearDown()

    def test_wb_connect_after_timeout(self):
        """white box tests disabled for retryclient."""

    def test_wb_reconnect_after_timeout_and_close(self):
        """white box tests disabled for retryclient."""


class RetryClientConnectionLossTest(ZookeeperTestCase):

    def setUp(self):
        super(RetryClientConnectionLossTest, self).setUp()

        from twisted.internet import reactor
        self.proxy = ProxyFactory("127.0.0.1", 2181)
        self.proxy_port = reactor.listenTCP(0, self.proxy)
        host = self.proxy_port.getHost()
        self.proxied_client = RetryClient(ZookeeperClient(
            "%s:%s" % (host.host, host.port)))
        self.direct_client = ZookeeperClient("127.0.0.1:2181", 3000)
        self.session_events = []

        def session_event_collector(conn, event):
            self.session_events.append(event)

        self.proxied_client.set_session_callback(session_event_collector)
        return self.direct_client.connect()

    @inlineCallbacks
    def tearDown(self):
        import zookeeper
        zookeeper.set_debug_level(0)
        if self.proxied_client.connected:
            yield self.proxied_client.close()
        if not self.direct_client.connected:
            yield self.direct_client.connect()
        utils.deleteTree(handle=self.direct_client.handle)
        yield self.direct_client.close()
        self.proxy.lose_connection()
        yield self.proxy_port.stopListening()

    @inlineCallbacks
    def test_get_children_and_watch(self):
        yield self.proxied_client.connect()

        # Setup tree
        cpath = "/test-tree"
        yield self.direct_client.create(cpath)

        # Block the request (drops all packets.)
        self.proxy.set_blocked(True)
        child_d, watch_d = self.proxied_client.get_children_and_watch(cpath)

        # Unblock and disconnect
        self.proxy.set_blocked(False)
        self.proxy.lose_connection()

        # Call goes through
        self.assertEqual((yield child_d), [])
        self.assertEqual(len(self.session_events), 2)

        # And we have reconnect events
        self.assertEqual(self.session_events[-1].state_name, "connected")

        yield self.direct_client.create(cpath + "/abc")

        # The original watch is still active
        yield watch_d

    @inlineCallbacks
    def test_exists_and_watch(self):
        yield self.proxied_client.connect()

        cpath = "/test-tree"

        # Block the request
        self.proxy.set_blocked(True)
        exists_d, watch_d = self.proxied_client.exists_and_watch(cpath)

        # Create the node
        yield self.direct_client.create(cpath)

        # Unblock and disconnect
        self.proxy.set_blocked(False)
        self.proxy.lose_connection()

        # Call gets retried, see the latest state
        self.assertTrue((yield exists_d))
        self.assertEqual(len(self.session_events), 2)

        # And we have reconnect events
        self.assertEqual(self.session_events[-1].state_name, "connected")

        yield self.direct_client.delete(cpath)

        # The original watch is still active
        yield watch_d

    @inlineCallbacks
    def test_get_and_watch(self):
        yield self.proxied_client.connect()

        # Setup tree
        cpath = "/test-tree"
        yield self.direct_client.create(cpath)

        # Block the request (drops all packets.)
        self.proxy.set_blocked(True)
        get_d, watch_d = self.proxied_client.get_and_watch(cpath)

        # Unblock and disconnect
        self.proxy.set_blocked(False)
        self.proxy.lose_connection()

        # Call goes through
        content, stat = yield get_d
        self.assertEqual(content, '')
        self.assertEqual(len(self.session_events), 2)

        # And we have reconnect events
        self.assertEqual(self.session_events[-1].state_name, "connected")

        yield self.direct_client.delete(cpath)

        # The original watch is still active
        yield watch_d

    @inlineCallbacks
    def test_set(self):
        yield self.proxied_client.connect()

        # Setup tree
        cpath = "/test-tree"
        yield self.direct_client.create(cpath, json.dumps({"a": 1, "c": 2}))

        def update_node(content, stat):
            data = json.loads(content)
            data["a"] += 1
            data["b"] = 0
            return json.dumps(data)

        # Block the request (drops all packets.)
        self.proxy.set_blocked(True)
        mod_d = retry_change(self.proxied_client, cpath, update_node)

        # Unblock and disconnect
        self.proxy.set_blocked(False)
        self.proxy.lose_connection()

        # Call goes through, contents verified.
        yield mod_d
        content, stat = yield self.direct_client.get(cpath)
        self.assertEqual(json.loads(content),
                         {"a": 2, "b": 0, "c": 2})
