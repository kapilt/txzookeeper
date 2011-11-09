#
#  Copyright (C) 2010-2011 Canonical Ltd. All Rights Reserved
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

import zookeeper

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from txzookeeper import ZookeeperClient
from txzookeeper.tests import ZookeeperTestCase, utils
from txzookeeper.tests.proxy import ProxyFactory


class WatchDeliveryConnectionFailedTest(ZookeeperTestCase):
    """Watches are still sent on reconnect.
    """

    def setUp(self):
        super(WatchDeliveryConnectionFailedTest, self).setUp()

        self.proxy = ProxyFactory("127.0.0.1", 2181)
        self.proxy_port = reactor.listenTCP(0, self.proxy)
        host = self.proxy_port.getHost()
        self.proxied_client = ZookeeperClient(
            "%s:%s" % (host.host, host.port))
        self.direct_client = ZookeeperClient("127.0.0.1:2181", 3000)
        self.session_events = []

        def session_event_collector(conn, event):
            self.session_events.append(event)

        self.proxied_client.set_session_callback(session_event_collector)
        return self.direct_client.connect()

    @inlineCallbacks
    def tearDown(self):
        zookeeper.set_debug_level(0)
        if self.proxied_client.connected:
            yield self.proxied_client.close()
        if not self.direct_client.connected:
            yield self.direct_client.connect()
        utils.deleteTree(handle=self.direct_client.handle)
        yield self.direct_client.close()
        self.proxy.lose_connection()
        yield self.proxy_port.stopListening()

    def verify_events(self, events, expected):
        """Verify the state of the session events encountered.
        """
        for value, state in zip([e.state_name for e in events], expected):
            self.assertEqual(value, state)

    @inlineCallbacks
    def test_child_watch_fires_upon_reconnect(self):
        yield self.proxied_client.connect()

        # Setup tree
        cpath = "/test-tree"
        yield self.direct_client.create(cpath)

        # Setup watch
        child_d, watch_d = self.proxied_client.get_children_and_watch(cpath)

        self.assertEqual((yield child_d), [])

        # Kill the connection and fire the watch
        self.proxy.lose_connection()
        yield self.direct_client.create(
            cpath + "/abc", flags=zookeeper.SEQUENCE)

        # We should still get the child event.
        yield watch_d

        # We get two pairs of (connecting, connected) for the conn and watch
        self.assertEqual(len(self.session_events), 4)
        self.verify_events(
            self.session_events,
            ("connecting", "connecting", "connected", "connected"))

    @inlineCallbacks
    def test_exists_watch_fires_upon_reconnect(self):
        yield self.proxied_client.connect()
        cpath = "/test"

        # Setup watch
        exists_d, watch_d = self.proxied_client.exists_and_watch(cpath)

        self.assertEqual((yield exists_d), None)

        # Kill the connection and fire the watch
        self.proxy.lose_connection()
        yield self.direct_client.create(cpath)

        # We should still get the exists event.
        yield watch_d

        # We get two pairs of (connecting, connected) for the conn and watch
        self.assertEqual(len(self.session_events), 4)
        self.verify_events(
            self.session_events,
            ("connecting", "connecting", "connected", "connected"))

    @inlineCallbacks
    def test_get_watch_fires_upon_reconnect(self):
        yield self.proxied_client.connect()
        # Setup tree
        cpath = "/test"
        yield self.direct_client.create(cpath, "abc")

        # Setup watch
        get_d, watch_d = self.proxied_client.get_and_watch(cpath)
        content, stat = yield get_d
        self.assertEqual(content, "abc")

        # Kill the connection and fire the watch
        self.proxy.lose_connection()
        yield self.direct_client.set(cpath, "xyz")

        # We should still get the exists event.
        yield watch_d

        # We also two pairs of (connecting, connected) for the conn and watch
        self.assertEqual(len(self.session_events), 4)
        self.verify_events(
            self.session_events,
            ("connecting", "connecting", "connected", "connected"))

    @inlineCallbacks
    def test_watch_delivery_failure_resends(self):
        """Simulate a network failure for the watch delivery

        The zk server effectively sends the watch delivery to the client,
        but the client never recieves it.
        """
        yield self.proxied_client.connect()
        cpath = "/test"

        # Setup watch
        exists_d, watch_d = self.proxied_client.exists_and_watch(cpath)

        self.assertEqual((yield exists_d), None)

        # Pause the connection fire the watch, and blackhole the data.
        self.proxy.set_blocked(True)
        yield self.direct_client.create(cpath)
        self.proxy.set_blocked(False)
        self.proxy.lose_connection()

        # We should still get the exists event.
        yield watch_d

    @inlineCallbacks
    def test_session_exception(self):
        """Test session expiration.

        On a single server, the best way to produce a session expiration is
        timing out the client.
        """
        yield self.proxied_client.connect()
        data = yield self.proxied_client.exists("/")
        self.assertTrue(data)
        self.proxy.set_blocked(True)
        # Wait for session expiration, on a single server options are limited
        yield self.sleep(15)
        # Unblock the proxy for next connect, and then drop the connection.
        self.proxy.set_blocked(False)
        self.proxy.lose_connection()
        # Wait for a reconnect (see below why we can't just use a watch here)
        yield self.sleep(2)
        yield self.assertFailure(
            self.proxied_client.get("/a"),
            zookeeper.SessionExpiredException)
        self.assertEqual(self.session_events[-1].state_name, "expired")

    @inlineCallbacks
    def xtest_binding_bug_session_exception(self):
        """This test triggers an exception in the python-zookeeper binding.

        File "txzookeeper/client.py", line 491, in create
           self.handle, path, data, acls, flags, callback)
        exceptions.SystemError: error return without exception set
        """
        yield self.proxied_client.connect()
        data_d, watch_d = yield self.proxied_client.exists_and_watch("/")
        self.assertTrue((yield data_d))
        self.proxy.set_blocked(True)
        # Wait for session expiration, on a single server options are limited
        yield self.sleep(15)
        # Unblock the proxy for next connect, and then drop the connection.
        self.proxy.set_blocked(False)
        self.proxy.lose_connection()
        # Wait for a reconnect
        yield self.assertFailure(watch_d, zookeeper.SessionExpiredException)
        # Leads to bindings bug failure
        yield self.assertFailure(
            self.proxied_client.get("/a"),
            zookeeper.SessionExpiredException)
        self.assertEqual(self.session_events[-1].state_name, "expired")
