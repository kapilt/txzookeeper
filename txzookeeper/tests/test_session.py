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

import atexit
import os

import zookeeper

from twisted.internet.defer import inlineCallbacks, Deferred, returnValue

from txzookeeper import ZookeeperClient
from txzookeeper.client import NotConnectedException, ConnectionException

from txzookeeper.tests.common import ZookeeperCluster
from txzookeeper.tests import ZookeeperTestCase


ZK_HOME = os.environ.get("ZOOKEEPER_PATH")
assert ZK_HOME, (
    "ZOOKEEPER_PATH environment variable must be defined.\n "
    "For deb package installations this is /usr/share/java")

CLUSTER = ZookeeperCluster(ZK_HOME)
atexit.register(lambda cluster: cluster.terminate(), CLUSTER)


class ClientSessionTests(ZookeeperTestCase):

    def setUp(self):
        super(ClientSessionTests, self).setUp()
        self.cluster.start()
        self.client = None
        self.client2 = None
        zookeeper.deterministic_conn_order(True)
        zookeeper.set_debug_level(0)

    @property
    def cluster(self):
        return CLUSTER

    def tearDown(self):
        super(ClientSessionTests, self).tearDown()
        self.cluster.reset()

    @inlineCallbacks
    def test_client_session_migration(self):
        """A client will automatically rotate servers to ensure a connection.

        A client connected to multiple servers, will transparently
        migrate amongst them, as individual servers can no longer be
        reached. A client's session will be maintined.
        """
        # Connect to the Zookeeper Cluster
        servers = ",".join([s.address for s in self.cluster])
        self.client = ZookeeperClient(servers)
        yield self.client.connect()
        yield self.client.create("/hello", flags=zookeeper.EPHEMERAL)

        # Shutdown the server the client is connected to
        self.cluster[0].stop()

        # Wait for the shutdown and cycle, if we don't wait we'll
        # get a zookeeper connectionloss exception on occassion.
        yield self.sleep(0.1)

        self.assertTrue(self.client.connected)
        exists = yield self.client.exists("/hello")
        self.assertTrue(exists)

    @inlineCallbacks
    def test_client_watch_migration(self):
        """On server rotation, extant watches are still active.

        A client connected to multiple servers, will transparently
        migrate amongst them, as individual servers can no longer be
        reached. Watch deferreds issued from the same client instance will
        continue to function as the session is maintained.
        """
        session_events = []

        def session_event_callback(connection, e):
            session_events.append(e)

        # Connect to the Zookeeper Cluster
        servers = ",".join([s.address for s in self.cluster])
        self.client = ZookeeperClient(servers)
        self.client.set_session_callback(session_event_callback)
        yield self.client.connect()

        # Setup a watch
        yield self.client.create("/hello")
        exists_d, watch_d = self.client.exists_and_watch("/hello")
        yield exists_d

        # Shutdown the server the client is connected to
        self.cluster[0].stop()

        # Wait for the shutdown and cycle, if we don't wait we'll
        # get occasionally get a  zookeeper connectionloss exception.
        yield self.sleep(0.1)

        # The session events that would have been ignored are sent
        # to the session event callback.
        self.assertTrue(session_events)
        self.assertTrue(self.client.connected)

        # If we delete the node, we'll see the watch fire.
        yield self.client.delete("/hello")
        event = yield watch_d
        self.assertEqual(event.type_name, "deleted")
        self.assertEqual(event.path, "/hello")

    @inlineCallbacks
    def test_connection_error_handler(self):
        """A callback can be specified for connection errors.

        We can specify a callback for connection errors, that
        can perform recovery for a disconnected client, restablishing
        """
        @inlineCallbacks
        def connection_error_handler(connection, error):
            # Moved management of this connection attribute out of the
            # default behavior for a connection exception, to support
            # the retry facade. Under the hood libzk is going to be
            # trying to transparently reconnect
            connection.connected = False

            # On loss of the connection, reconnect the client w/ same session.

            yield connection.connect(
                self.cluster[1].address, client_id=connection.client_id)
            returnValue(23)

        self.client = ZookeeperClient(self.cluster[0].address)
        self.client.set_connection_error_callback(connection_error_handler)
        yield self.client.connect()

        yield self.client.create("/hello")
        exists_d, watch_d = self.client.exists_and_watch("/hello")
        yield exists_d

        # Shutdown the server the client is connected to
        self.cluster[0].stop()
        yield self.sleep(0.1)

        # Results in connection loss exception, and invoking of error handler.
        result = yield self.client.exists("/hello")

        # The result of the error handler is returned to the api
        self.assertEqual(result, 23)

        exists = yield self.client.exists("/hello")
        self.assertTrue(exists)

    @inlineCallbacks
    def test_client_session_expiration_event(self):
        """A client which recieves a session expiration event.
        """
        session_events = []
        events_received = Deferred()

        def session_event_callback(connection, e):
            session_events.append(e)
            if len(session_events) == 8:
                events_received.callback(True)

        # Connect to a node in the cluster and establish a watch
        self.client = ZookeeperClient(self.cluster[0].address)
        self.client.set_session_callback(session_event_callback)
        yield self.client.connect()

        # Setup some watches to verify they are cleaned out on expiration.
        d, e_watch_d = self.client.exists_and_watch("/")
        yield d

        d, g_watch_d = self.client.get_and_watch("/")
        yield d

        d, c_watch_d = self.client.get_children_and_watch("/")
        yield d

        # Connect a client to the same session on a different node.
        self.client2 = ZookeeperClient(self.cluster[0].address)
        yield self.client2.connect(client_id=self.client.client_id)

        # Close the new client and wait for the event propogation
        yield self.client2.close()

        # It can take some time for this to propagate
        yield events_received
        self.assertEqual(len(session_events), 8)
        # The last four (conn + 3 watches) are all expired
        for evt in session_events[4:]:
            self.assertEqual(evt.state_name, "expired")

        # The connection is dead without reconnecting.
        yield self.assertFailure(
            self.client.exists("/"),
            NotConnectedException, ConnectionException)

        self.assertTrue(self.client.unrecoverable)
        yield self.assertFailure(e_watch_d, zookeeper.SessionExpiredException)
        yield self.assertFailure(g_watch_d, zookeeper.SessionExpiredException)
        yield self.assertFailure(c_watch_d, zookeeper.SessionExpiredException)

        # If a reconnect attempt is made with a dead session id
        #yield self.client.connect(client_id=self.client.client_id)

    test_client_session_expiration_event.timeout = 10

    @inlineCallbacks
    def test_client_reconnect_session_on_different_server(self):
        """On connection failure, An application can choose to use a
        new connection with which to reconnect to a different member
        of the zookeeper cluster, reacquiring the extant session.

        A large obvious caveat to using a new client instance rather
        than reconnecting the existing client, is that even though the
        session has outstanding watches, the watch callbacks/deferreds
        won't be active unless the client instance used to create them
        is connected.
        """
        session_events = []

        def session_event_callback(connection, e):
            session_events.append(e)

        # Connect to a node in the cluster and establish a watch
        self.client = ZookeeperClient(self.cluster[2].address)
        self.client.set_session_callback(session_event_callback)
        yield self.client.connect()

        yield self.client.create("/hello", flags=zookeeper.EPHEMERAL)
        exists_d, watch_d = self.client.exists_and_watch("/hello")
        yield exists_d

        # Shutdown the server the client is connected to
        self.cluster[2].stop()
        yield self.sleep(0.1)

        # Verify we got a session event regarding the down server
        self.assertTrue(session_events)

        # Open up a new connection to a different server with same session
        self.client2 = ZookeeperClient(self.cluster[0].address)
        yield self.client2.connect(client_id=self.client.client_id)

        # Close the old disconnected client
        self.client.close()

        # Verify the ephemeral still exists
        exists = yield self.client2.exists("/hello")
        self.assertTrue(exists)

        # Destroy the session and reconnect
        self.client2.close()
        yield self.client.connect(self.cluster[0].address)

        # Ephemeral is destroyed when the session closed.
        exists = yield self.client.exists("/hello")
        self.assertFalse(exists)
