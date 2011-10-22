#
#  Copyright (C) 2010, 2011 Canonical Ltd. All Rights Reserved
#
#  This file is part of txzookeeper.
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

from twisted.internet.defer import inlineCallbacks

from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient
from txzookeeper.utils import retry_change

from txzookeeper.tests import ZookeeperTestCase, utils
from txzookeeper.tests.proxy import ProxyFactory
from txzookeeper.tests import test_client


class RetryCoreTests(ZookeeperTestCase):
    """Test the retry functions in isolation.
    """

    def test_passmethod(self):
        pass

    def test_passproperty(self):
        pass

    def test_is_retryable(self):
        pass

    def test_retry_watch(self):
        pass


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
        self.proxy.loose_connection()
        yield self.proxy_port.stopListening()

    @inlineCallbacks
    def test_get_children_and_watch(self):
        yield self.proxied_client.connect()

        # Setup tree
        cpath = "/test-tree"
        yield self.direct_client.create(cpath)

        # Block the request
        self.proxy.set_blocked(True)
        child_d, watch_d = self.proxied_client.get_children_and_watch(cpath)

        # Unblock and disconnect
        self.proxy.set_blocked(False)
        self.proxy.loose_connection()

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
        self.proxy.loose_connection()

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

        # Block the request
        self.proxy.set_blocked(True)
        get_d, watch_d = self.proxied_client.get_and_watch(cpath)

        # Unblock and disconnect
        self.proxy.set_blocked(False)
        self.proxy.loose_connection()

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
        import zookeeper
        zookeeper.set_debug_level(zookeeper.LOG_LEVEL_DEBUG)
        yield self.proxied_client.connect()

        # Setup tree
        cpath = "/test-tree"
        yield self.direct_client.create(cpath, json.dumps({"a": 1, "c": 2}))

        def update_node(content, stat):
            data = json.loads(content)
            print "data", data
            data["a"] += 1
            data["b"] = 0
            return json.dumps(data)

        # Block the request
        self.proxy.set_blocked(True)
        mod_d = retry_change(self.proxied_client, cpath, update_node)

        # Unblock and disconnect
        self.proxy.set_blocked(False)
        self.proxy.loose_connection()

        # Call goes through, contents verified.
        yield mod_d
        content, stat = yield self.direct_client.get(cpath)
        self.assertEqual(json.loads(content),
                         {"a": 2, "b": 0, "c": 2})
