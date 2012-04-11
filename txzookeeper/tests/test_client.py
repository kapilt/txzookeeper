#
#  Copyright (C) 2010-2011, 2011 Canonical Ltd. All Rights Reserved
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


import base64
import hashlib

from twisted.internet.defer import Deferred
from twisted.internet.base import DelayedCall
from twisted.python.failure import Failure

import zookeeper

from mocker import ANY, MATCH
from txzookeeper.tests import ZookeeperTestCase, utils
from txzookeeper.client import (
    ZookeeperClient, ZOO_OPEN_ACL_UNSAFE, ConnectionTimeoutException,
    ConnectionException, NotConnectedException, ClientEvent)

PUBLIC_ACL = ZOO_OPEN_ACL_UNSAFE


def match_deferred(arg):
    return isinstance(arg, Deferred)

DEFERRED_MATCH = MATCH(match_deferred)


class ClientTests(ZookeeperTestCase):

    def setUp(self):
        super(ClientTests, self).setUp()
        self.client = ZookeeperClient("127.0.0.1:2181", 3000)
        self.client2 = None

    def tearDown(self):
        if self.client.connected:
            utils.deleteTree(handle=self.client.handle)
            self.client.close()

        if self.client2 and self.client2.connected:
            self.client2.close()

        super(ClientTests, self).tearDown()

    def test_wb_connect_after_timeout(self):
        """
        Test an odd error scenario. If the zookeeper client succeeds in
        connecting after a timeout, the connection should be closed, as
        the connect deferred has already fired.
        """
        mock_client = self.mocker.patch(self.client)
        mock_client.close()

        def close_state():
            # Ensure the client state variable is correct after the close call.
            self.client.connected = False

        self.mocker.call(close_state)
        self.mocker.replay()

        task = DelayedCall(1, lambda: 1, None, None, None, None)
        task.called = True

        d = Deferred()
        d.errback(ConnectionTimeoutException())

        self.client._cb_connected(
            task, d, None, zookeeper.CONNECTED_STATE, "/")

        self.failUnlessFailure(d, ConnectionTimeoutException)
        return d

    def test_wb_reconnect_after_timeout_and_close(self):
        """
        Another odd error scenario, if a client instance has has
        connect and closed methods invoked in succession multiple
        times, and a previous callback connect timeouts, the callback
        of a previous connect can be invoked by a subsequent connect,
        with a CONNECTING_STATE. Verify this does not attempt to
        invoke the connect deferred again.
        """
        d = Deferred()
        d.callback(True)

        task = DelayedCall(1, lambda: 1, None, None, None, None)
        task.called = True

        self.assertEqual(
            self.client._cb_connected(
                task, d, None, zookeeper.CONNECTING_STATE, ""),
            None)

    def test_connect(self):
        """
        The client can connect to a zookeeper instance.
        """
        d = self.client.connect()

        def check_connected(client):
            self.assertEquals(client.connected, True)
            self.assertEquals(client.state, zookeeper.CONNECTED_STATE)
        d.addCallback(check_connected)
        return d

    def test_client_event_repr(self):
        event = ClientEvent(zookeeper.SESSION_EVENT,
                            zookeeper.EXPIRED_SESSION_STATE, '')
        self.assertEqual(repr(event),
                         "<ClientEvent session at '' state: expired>")

    def test_client_event_attributes(self):
        event = ClientEvent(4, 'state', 'path')
        self.assertEqual(event.type, 4)
        self.assertEqual(event.connection_state, 'state')
        self.assertEqual(event.path, 'path')
        self.assertEqual(event, (4, 'state', 'path'))

    def test_client_use_while_disconnected_returns_failure(self):
        return self.assertFailure(
            self.client.exists("/"), NotConnectedException)

    def test_create_ephemeral_node_and_close_connection(self):
        """
        The client can create transient nodes that are destroyed when the
        client is closed and the session is destroyed on the zookeeper servers.
        """
        d = self.client.connect()

        def test_create_ephemeral_node(client):
            d = self.client.create(
                "/foobar-transient", "rabbit", flags=zookeeper.EPHEMERAL)
            return d

        def check_node_path(path):
            self.assertEqual(path, "/foobar-transient")
            return path

        def close_connection(path):
            return self.client.close()

        def new_connection(close_result):
            self.client2 = new_client = ZookeeperClient("127.0.0.1:2181")
            return new_client.connect()

        def check_node_doesnt_exist(connected):
            self.assertRaises(
                zookeeper.NoNodeException,
                zookeeper.get,
                connected.handle,
                "/foobar-transient")
            self.client2.close()

        d.addCallback(test_create_ephemeral_node)
        d.addCallback(check_node_path)
        d.addCallback(close_connection)
        d.addCallback(new_connection)
        d.addCallback(check_node_doesnt_exist)
        return d

    def test_create_node(self):
        """
        We can create a node in zookeeper, with a given path
        """
        d = self.client.connect()

        def create_ephemeral_node(connected):
            d = self.client.create(
                "/foobar", "rabbit", flags=zookeeper.EPHEMERAL)
            return d

        def verify_node_path_and_content(path):
            self.assertEqual(path, "/foobar")
            self.assertNotEqual(
                zookeeper.exists(self.client.handle, path), None)
            data, stat = zookeeper.get(self.client.handle, path)
            self.assertEqual(data, "rabbit")

        d.addCallback(create_ephemeral_node)
        d.addCallback(verify_node_path_and_content)
        return d

    def test_create_persistent_node_and_close(self):
        """
        The client creates persistent nodes by default that exist independently
        of the client session.
        """
        d = self.client.connect()

        def test_create_ephemeral_node(client):
            d = self.client.create(
                "/foobar-persistent", "rabbit")
            return d

        def check_node_path(path):
            self.assertEqual(path, "/foobar-persistent")
            self.assertNotEqual(
                zookeeper.exists(self.client.handle, path), None)
            return path

        def close_connection(path):
            self.client.close()
            self.client2 = new_client = ZookeeperClient("127.0.0.1:2181")
            return new_client.connect()

        def check_node_exists(client):
            data, stat = zookeeper.get(client.handle, "/foobar-persistent")
            self.assertEqual(data, "rabbit")

        d.addCallback(test_create_ephemeral_node)
        d.addCallback(check_node_path)
        d.addCallback(close_connection)
        d.addCallback(check_node_exists)
        return d

    def test_get(self):
        """
        The client can retrieve a node's data via its get method.
        """
        d = self.client.connect()

        def create_node(client):
            d = self.client.create(
                "/foobar-transient", "rabbit", flags=zookeeper.EPHEMERAL)
            return d

        def get_contents(path):
            return self.client.get(path)

        def verify_contents((data, stat)):
            self.assertEqual(data, "rabbit")

        d.addCallback(create_node)
        d.addCallback(get_contents)
        d.addCallback(verify_contents)
        return d

    def test_get_with_error(self):
        """
        On get error the deferred's errback is raised.
        """
        d = self.client.connect()

        def get_contents(client):
            return client.get("/foobar-transient")

        def verify_failure(failure):
            self.assertTrue(
                isinstance(failure.value, zookeeper.NoNodeException))

        def assert_failure(extra):
            self.fail("get should have failed")

        d.addCallback(get_contents)
        d.addCallback(verify_failure)
        d.addErrback(verify_failure)
        return d

    def test_get_with_watcher(self):
        """
        The client can specify a callable watcher when invoking get. The
        watcher will be called back when the client path is modified in
        another session.
        """
        d = self.client.connect()
        watch_deferred = Deferred()

        def create_node(client):
            return self.client.create("/foobar-watched", "rabbit")

        def get_node(path):
            data, watch = self.client.get_and_watch(path)
            watch.chainDeferred(watch_deferred)
            return data

        def new_connection(data):
            self.client2 = ZookeeperClient("127.0.0.1:2181")
            return self.client2.connect()

        def trigger_watch(client):
            zookeeper.set(self.client2.handle, "/foobar-watched", "abc")
            return watch_deferred

        def verify_watch(event):
            self.assertEqual(event.path, "/foobar-watched")
            self.assertEqual(event.type, zookeeper.CHANGED_EVENT)

        d.addCallback(create_node)
        d.addCallback(get_node)
        d.addCallback(new_connection)
        d.addCallback(trigger_watch)
        d.addCallback(verify_watch)
        return d

    def test_get_with_watcher_and_delete(self):
        """
        The client can specify a callable watcher when invoking get. The
        watcher will be called back when the client path is modified in
        another session.
        """
        d = self.client.connect()

        def create_node(client):
            return self.client.create("/foobar-watched", "rabbit")

        def get_node(path):
            data, watch = self.client.get_and_watch(path)
            return data.addCallback(lambda x: (watch,))

        def new_connection((watch,)):
            self.client2 = ZookeeperClient("127.0.0.1:2181")
            return self.client2.connect().addCallback(
                lambda x, y=None, z=None: (x, watch))

        def trigger_watch((client, watch)):
            zookeeper.delete(self.client2.handle, "/foobar-watched")
            self.client2.close()
            return watch

        def verify_watch(event):
            self.assertEqual(event.path, "/foobar-watched")
            self.assertEqual(event.type, zookeeper.DELETED_EVENT)

        d.addCallback(create_node)
        d.addCallback(get_node)
        d.addCallback(new_connection)
        d.addCallback(trigger_watch)
        d.addCallback(verify_watch)
        return d

    def test_delete(self):
        """
        The client can delete a node via its delete method.
        """
        d = self.client.connect()

        def create_node(client):
            return self.client.create(
                "/foobar-transient", "rabbit", flags=zookeeper.EPHEMERAL)

        def verify_exists(path):
            self.assertNotEqual(
                zookeeper.exists(self.client.handle, path), None)
            return path

        def delete_node(path):
            return self.client.delete(path)

        def verify_not_exists(*args):
            self.assertEqual(
                zookeeper.exists(self.client.handle, "/foobar-transient"),
                None)

        d.addCallback(create_node)
        d.addCallback(verify_exists)
        d.addCallback(delete_node)
        d.addCallback(verify_not_exists)
        return d

    def test_exists_with_existing(self):
        """
        The exists method returns node stat information for an existing node.
        """
        d = self.client.connect()

        def create_node(client):
            return self.client.create(
                "/foobar-transient", "rabbit", flags=zookeeper.EPHEMERAL)

        def check_exists(path):
            return self.client.exists(path)

        def verify_exists(node_stat):
            self.assertEqual(node_stat["dataLength"], 6)
            self.assertEqual(node_stat["version"], 0)

        d.addCallback(create_node)
        d.addCallback(check_exists)
        d.addCallback(verify_exists)
        return d

    def test_exists_with_error(self):
        """
        On error exists invokes the errback with the exception.
        """
        d = self.client.connect()

        def inject_error(result_code, d, extra_codes=None):
            error = SyntaxError()
            d.errback(error)
            return error

        def check_exists(client):
            mock_client = self.mocker.patch(client)
            mock_client._check_result(
                ANY, DEFERRED_MATCH, extra_codes=(zookeeper.NONODE,))
            self.mocker.call(inject_error)
            self.mocker.replay()
            return client.exists("/zebra-moon")

        def verify_failure(failure):
            self.assertTrue(isinstance(failure.value, SyntaxError))

        d.addCallback(check_exists)
        d.addErrback(verify_failure)
        return d

    def test_exists_with_nonexistant(self):
        """
        The exists method returns None when the value node doesn't exist.
        """
        d = self.client.connect()

        def check_exists(client):
            return self.client.exists("/abcdefg")

        def verify_exists(node_stat):
            self.assertEqual(node_stat, None)

        d.addCallback(check_exists)
        d.addCallback(verify_exists)
        return d

    def test_exist_watch_with_node_change(self):
        """
        Setting an exist watches on existing node will also respond to
        node changes.
        """
        d = self.client.connect()

        def create_node(client):
            return client.create("/rome")

        def check_exists(path):
            existsd, w = self.client.exists_and_watch(path)
            w.addCallback(node_watcher)
            return existsd

        def node_watcher(event):
            self.assertEqual(event.type_name, "changed")

        def verify_exists(node_stat):
            self.assertTrue(node_stat)
            return self.client.set("/rome", "magic")

        d.addCallback(create_node)
        d.addCallback(check_exists)
        d.addCallback(verify_exists)
        return d

    def test_exists_with_watcher_and_close(self):
        """
        Closing a connection with an watch outstanding behaves correctly.
        """
        d = self.client.connect()

        def node_watcher(event):
            client = getattr(self, "client", None)
            if client is not None and client.connected:
                self.fail("Client should be disconnected")

        def create_node(client):
            return client.create("/syracuse")

        def check_exists(path):
            # shouldn't fire till unit test cleanup
            d, w = self.client.exists_and_watch(path)
            w.addCallback(node_watcher)
            return d

        def verify_exists(result):
            self.assertTrue(result)

        d.addCallback(create_node)
        d.addCallback(check_exists)
        d.addCallback(verify_exists)
        return d

    def test_exists_with_nonexistant_watcher(self):
        """
        The exists method can also be used to set an optional watcher on a
        node. The watch can be set on a node that does not yet exist.
        """
        d = self.client.connect()
        node_path = "/animals"
        watcher_deferred = Deferred()

        def create_container(path):
            return self.client.create(node_path, "")

        def check_exists(path):
            exists, watch = self.client.exists_and_watch(
                "%s/wooly-mammoth" % node_path)
            watch.chainDeferred(watcher_deferred)
            return exists

        def new_connection(node_stat):
            self.assertFalse(node_stat)
            self.client2 = ZookeeperClient("127.0.0.1:2181")
            return self.client2.connect()

        def create_node(client):
            self.assertEqual(client.connected, True)
            return self.client2.create(
                "%s/wooly-mammoth" % node_path, "extinct")

        def shim(path):
            return watcher_deferred

        def verify_watch(event):
            self.assertEqual(event.path, "%s/wooly-mammoth" % node_path)
            self.assertEqual(event.type, zookeeper.CREATED_EVENT)

        d.addCallback(create_container)
        d.addCallback(check_exists)
        d.addCallback(new_connection)
        d.addCallback(create_node)
        d.addCallback(shim)
        d.addCallback(verify_watch)

        return d

    def test_create_sequence_node(self):
        """
        The client can create a monotonically increasing sequence nodes.
        """
        d = self.client.connect()

        def create_node(client):
            return self.client.create("/seq-a")

        def create_seq_node(path):
            return self.client.create(
                "/seq-a/seq-", flags=zookeeper.EPHEMERAL | zookeeper.SEQUENCE)

        def get_children(path):
            return self.client.get_children("/seq-a")

        def verify_children(children):
            self.assertEqual(children, ["seq-0000000000", "seq-0000000001"])

        d.addCallback(create_node)
        d.addCallback(create_seq_node)
        d.addCallback(create_seq_node)
        d.addCallback(get_children)
        d.addCallback(verify_children)
        return d

    def test_create_duplicate_node(self):
        """
        Attempting to create a node that already exists results in a failure.
        """
        d = self.client.connect()

        def create_node(client):
            return self.client.create("/abc")

        def create_duplicate(path):
            return self.client.create("/abc")

        def verify_fails(*args):
            self.fail("Invoked Callback")

        def verify_succeeds(failure):
            self.assertTrue(failure)
            self.assertEqual(failure.value.args, ("node exists",))

        d.addCallback(create_node)
        d.addCallback(create_duplicate)
        d.addCallback(verify_fails)
        d.addErrback(verify_succeeds)
        return d

    def test_delete_nonexistant_node(self):
        """
        Attempting to delete a node that already exists results in a failure.
        """
        d = self.client.connect()

        def delete_node(client):
            return client.delete("/abcd")

        def verify_fails(*args):
            self.fail("Invoked Callback")

        def verify_succeeds(failure):
            self.assertTrue(failure)
            self.assertEqual(failure.value.args, ("no node",))

        d.addCallback(delete_node)
        d.addCallback(verify_fails)
        d.addErrback(verify_succeeds)
        return d

    def test_set(self):
        """
        The client can be used to set contents of a node.
        """
        d = self.client.connect()

        def create_node(client):
            return client.create("/zebra", "horse")

        def set_node(path):
            return self.client.set("/zebra", "mammal")

        def verify_contents(junk):
            self.assertEqual(zookeeper.get(self.client.handle, "/zebra")[0],
                             "mammal")

        d.addCallback(create_node)
        d.addCallback(set_node)
        d.addCallback(verify_contents)
        return d

    def test_set_nonexistant(self):
        """
        if the client is used to set the contents of a nonexistant node
        an error is raised.
        """
        d = self.client.connect()

        def set_node(client):
            return client.set("/xy1")

        def verify_fails(*args):
            self.fail("Invoked Callback")

        def verify_succeeds(failure):
            self.assertTrue(failure)
            self.assertEqual(failure.value.args, ("no node",))

        d.addCallback(set_node)
        d.addCallback(verify_fails)
        d.addErrback(verify_succeeds)
        return d

    def test_get_children(self):

        d = self.client.connect()

        def create_nodes(client):
            zookeeper.create(
                self.client.handle, "/tower", "", [PUBLIC_ACL], 0)
            zookeeper.create(
                self.client.handle, "/tower/london", "", [PUBLIC_ACL], 0)
            zookeeper.create(
                self.client.handle, "/tower/paris", "", [PUBLIC_ACL], 0)

            return client

        def get_children(client):
            return client.get_children("/tower")

        def verify_children(children):
            self.assertEqual(children, ["paris", "london"])

        d.addCallback(create_nodes)
        d.addCallback(get_children)
        d.addCallback(verify_children)
        return d

    def test_get_children_with_error(self):
        """If the result of an api call is an error, its propgated.
        """
        d = self.client.connect()

        def get_children(client):
            # Get the children of a nonexistant node
            return client.get_children("/tower")

        def verify_failure(failure):
            self.assertTrue(isinstance(failure, Failure))
            self.assertTrue(
                isinstance(failure.value, zookeeper.NoNodeException))

        d.addCallback(get_children)
        d.addBoth(verify_failure)
        return d

    # seems to be a segfault on this one, must be running latest zk
    def test_get_children_with_watch(self):
        """
        The get_children method optionally takes a watcher callable which will
        be notified when the node is modified, or a child deleted or added.
        """
        d = self.client.connect()
        watch_deferred = Deferred()

        def create_node(client):
            return client.create("/jupiter")

        def get_children(path):
            ids, watch = self.client.get_children_and_watch(path)
            watch.chainDeferred(watch_deferred)
            return ids

        def new_connection(children):
            self.assertFalse(children)
            self.client2 = ZookeeperClient("127.0.0.1:2181")
            return self.client2.connect()

        def trigger_watch(client):
            zookeeper.create(
                self.client2.handle, "/jupiter/io", "", [PUBLIC_ACL], 0)
            return watch_deferred

        def verify_observed(data):
            self.assertTrue(data)

        d.addCallback(create_node)
        d.addCallback(get_children)
        d.addCallback(new_connection)
        d.addCallback(trigger_watch)
        d.addCallback(verify_observed)
        return d

    def test_get_children_with_watch_container_deleted(self):
        """
        Establishing a child watch on a path, and then deleting the path,
        will fire a child event watch on the container. This seems a little
        counterintutive, but zookeeper docs state they do this as a signal
        the container will never have any children. And logically you'd
        would want to fire, so that in case the container node gets recreated
        later and the watch fires, you don't want to the watch to fire then,
        as its a technically a different container.
        """

        d = self.client.connect()
        watch_deferred = Deferred()

        def create_node(client):
            return self.client.create("/prison")

        def get_children(path):
            childd, w = self.client.get_children_and_watch(path)
            w.addCallback(verify_watch)
            return childd

        def delete_node(children):
            return self.client.delete("/prison")

        def verify_watch(event):
            self.assertTrue(event.type_name, "child")
            watch_deferred.callback(None)

        d.addCallback(create_node)
        d.addCallback(get_children)
        d.addCallback(delete_node)

        return watch_deferred

    test_get_children_with_watch_container_deleted.timeout = 5

    def test_get_no_children(self):
        """
        Getting children of a node without any children returns an empty list.
        """
        d = self.client.connect()

        def create_node(client):
            return self.client.create("/tower")

        def get_children(path):
            return self.client.get_children(path)

        def verify_children(children):
            self.assertEqual(children, [])

        d.addCallback(create_node)
        d.addCallback(get_children)
        d.addCallback(verify_children)
        return d

    def test_get_children_nonexistant(self):
        """
        Getting children of a nonexistant node raises a no node exception.
        """
        d = self.client.connect()

        def get_children(client):
            return client.get_children("/tower")

        d.addCallback(get_children)
        self.failUnlessFailure(d, zookeeper.NoNodeException)
        return d

    def test_add_auth(self):
        """
        The connection can have zero or more authentication infos. This
        authentication infos are used when accessing nodes to veriy access
        against the node's acl.
        """
        d = self.client.connect()

        credentials = "mary:apples"
        user, password = credentials.split(":")
        identity = "%s:%s" % (
            user,
            base64.b64encode(hashlib.new('sha1', credentials).digest()))
        acl = {'id': identity, 'scheme': 'digest', 'perms': zookeeper.PERM_ALL}
        failed = []

        def add_auth_one(client):
            d = client.add_auth("digest", "bob:martini")
            # a little hack to avoid slowness around adding auth
            # see https://issues.apache.org/jira/browse/ZOOKEEPER-770
            # by pushing an additional message send/response cycle
            # we don't have to wait for the io thread to timeout
            # on the socket.
            client.exists("/orchard")
            return d

        def create_node(client):
            return client.create("/orchard", "apple trees", acls=[acl])

        def try_node_access(path):
            return self.client.set("/orchard", "bar")

        def node_access_failed(failure):
            self.assertEqual(failure.value.args, ("not authenticated",))
            failed.append(True)
            return

        def add_auth_two(result):
            d = self.client.add_auth("digest", credentials)
            # a little hack to avoid slowness around adding auth
            # see https://issues.apache.org/jira/browse/ZOOKEEPER-770
            self.client.get_children("/orchard")
            return d

        def verify_node_access(stat):
            self.assertEqual(stat['version'], 1)
            self.assertEqual(stat['dataLength'], 3)
            self.assertTrue(failed)  # we should have hit the errback

        d.addCallback(add_auth_one)
        d.addCallback(create_node)
        d.addCallback(try_node_access)
        d.addErrback(node_access_failed)
        d.addCallback(add_auth_two)
        d.addCallback(try_node_access)
        d.addCallback(verify_node_access)

        return d

    def test_add_auth_with_error(self):
        """
        On add_auth error the deferred errback is invoked with the exception.
        """
        d = self.client.connect()

        def _fake_auth(handle, scheme, identity, callback):
            callback(0, zookeeper.AUTHFAILED)
            return 0

        mock_auth = self.mocker.replace("zookeeper.add_auth")
        mock_auth(ANY, ANY, ANY, ANY)
        self.mocker.call(_fake_auth)
        self.mocker.replay()

        def add_auth(client):
            d = self.client.add_auth("digest", "mary:lamb")
            return d

        def verify_failure(failure):
            self.assertTrue(
                isinstance(failure.value, zookeeper.AuthFailedException))

        def assert_failed(result):
            self.fail("should not get here")

        d.addCallback(add_auth)
        d.addCallback(assert_failed)
        d.addErrback(verify_failure)
        return d

    def test_set_acl(self):
        """
        The client can be used to set an ACL on a node.
        """

        d = self.client.connect()

        acl = [PUBLIC_ACL,
               dict(scheme="digest",
                    id="zebra:moon",
                    perms=zookeeper.PERM_ALL)]

        def create_node(client):
            return client.create("/moose")

        def set_acl(path):
            return self.client.set_acl(path, acl)

        def verify_acl(junk):
            self.assertEqual(
                zookeeper.get_acl(self.client.handle, "/moose")[1],
                acl)

        d.addCallback(create_node)
        d.addCallback(set_acl)
        d.addCallback(verify_acl)
        return d

    def test_set_acl_with_error(self):
        """
        on error set_acl invokes the deferred's errback with an exception.
        """
        d = self.client.connect()

        acl = dict(scheme="digest", id="a:b", perms=zookeeper.PERM_ALL)

        def set_acl(client):
            return client.set_acl("/zebra-moon22", [acl])

        def verify_failure(failure):
            self.assertTrue(
                isinstance(failure.value, zookeeper.NoNodeException))

        d.addCallback(set_acl)
        d.addErrback(verify_failure)
        return d

    def test_get_acl(self):
        """
        The client can be used to get an ACL on a node.
        """
        d = self.client.connect()

        def create_node(client):
            return client.create("/moose")

        def get_acl(path):
            return self.client.get_acl(path)

        def verify_acl((acls, stat)):
            self.assertEqual(acls, [PUBLIC_ACL])

        d.addCallback(create_node)
        d.addCallback(get_acl)
        d.addCallback(verify_acl)
        return d

    def test_get_acl_error(self):
        """
        On error the acl callback invokes the deferred errback with the
        exception.
        """
        d = self.client.connect()

        def inject_error(result, d):
            error = zookeeper.ZooKeeperException()
            d.errback(error)
            return error

        def get_acl(path):
            # Get the ACL of a nonexistant node
            return self.client.get_acl("/moose")

        def verify_failure(failure):
            self.assertTrue(isinstance(failure, Failure))
            self.assertTrue(
                isinstance(failure.value, zookeeper.ZooKeeperException))

        d.addCallback(get_acl)
        d.addBoth(verify_failure)
        return d

    def test_client_id(self):
        """
        The client exposes a client id which is useful when examining
        the server logs.
        """
        # if we're not connected returns none
        self.assertEqual(self.client.client_id, None)
        d = self.client.connect()

        def verify_client_id(client):
            self.assertTrue(isinstance(self.client.client_id, tuple))
            self.assertTrue(isinstance(self.client.client_id[0], long))
            self.assertTrue(isinstance(self.client.client_id[1], str))

        d.addCallback(verify_client_id)
        return d

    def test_sync(self):
        """
        The sync method on the client flushes the connection to leader.

        In practice this seems hard to test functionally, but we at
        least verify the method executes without issue.
        """
        d = self.client.connect()

        def create_node(client):
            return client.create("/abc")

        def client_sync(path):
            return self.client.sync(path)

        def verify_sync(result):
            self.assertTrue(
                zookeeper.exists(self.client.handle, "/abc"))

        d.addCallback(create_node)
        d.addCallback(client_sync)
        d.addCallback(verify_sync)
        return d

    def test_property_servers(self):
        """
        The servers property of the client, shows which if any servers
        it might be connected, else it returns.
        """
        self.assertEqual(self.client.servers, None)
        d = self.client.connect()

        def verify_servers(client):
            self.assertEqual(client.servers, "127.0.0.1:2181")

        d.addCallback(verify_servers)
        return d

    def test_property_session_timeout(self):
        """
        The negotiated session timeout is available as a property on the
        client. If the client isn't connected, the value is None.
        """
        self.assertEqual(self.client.session_timeout, None)
        d = self.client.connect()

        def verify_session_timeout(client):
            self.assertIn(client.session_timeout, (4000, 10000))

        d.addCallback(verify_session_timeout)
        return d

    def test_property_unrecoverable(self):
        """
        The unrecoverable property specifies whether the connection can be
        recovered or must be discarded.
        """
        d = self.client.connect()

        def verify_recoverable(client):
            self.assertEqual(client.unrecoverable, False)
            return client

        d.addCallback(verify_recoverable)
        return d

    def test_invalid_watcher(self):
        """
        Setting an invalid watcher raises a syntaxerror.
        """
        d = self.client.connect()

        def set_invalid_watcher(client):
            return client.set_connection_watcher(1)

        def verify_invalid(failure):
            self.assertEqual(failure.value.args, ("Invalid Watcher 1",))
            self.assertTrue(isinstance(failure.value, SyntaxError))

        d.addCallback(set_invalid_watcher)
        d.addErrback(verify_invalid)
        return d

    def test_connect_with_server(self):
        """
        A client's servers can be specified in the connect method.
        """
        d = self.client.connect("127.0.0.1:2181")

        def verify_connected(client):
            self.assertTrue(client.connected)

        d.addCallback(verify_connected)
        return d

    def test_connect_with_error(self):
        """
        An error in the connect invokes the deferred errback with exception.
        """

        def _fake_init(handle, callback, timeout):
            callback(0, 0, zookeeper.ASSOCIATING_STATE, "")
            return 0
        mock_init = self.mocker.replace("zookeeper.init")
        mock_init(ANY, ANY, ANY)
        self.mocker.call(_fake_init)
        self.mocker.replay()

        d = self.client.connect()

        def verify_error(failure):
            self.assertFalse(self.client.connected)
            self.assertTrue(isinstance(failure.value, ConnectionException))
            self.assertEqual(failure.value.args[0], "connection error")

        def assert_failed(any):
            self.fail("should not be invoked")

        d.addCallback(assert_failed)
        d.addErrback(verify_error)
        return d

    test_connect_with_error.timeout = 5

    def test_connect_timeout(self):
        """
        A timeout in seconds can be specified on connect, if the client hasn't
        connected before then, then an errback is invoked with a timeout
        exception.
        """
        mock_init = self.mocker.replace("zookeeper.init")
        mock_init(ANY, ANY, ANY)
        self.mocker.result(0)
        self.mocker.replay()

        d = self.client.connect(timeout=0.1)

        def verify_timeout(failure):
            self.assertTrue(
                isinstance(failure.value, ConnectionTimeoutException))

        def assert_failure(any):
            self.fail("should not be reached")

        d.addCallback(assert_failure)
        d.addErrback(verify_timeout)
        return d

    def test_connect_ensured(self):
        """
        All of the client apis (with the exception of connect) attempt
        to ensure the client is connected before executing an operation.
        """
        self.assertFailure(
            self.client.get_children("/abc"), zookeeper.ZooKeeperException)

        self.assertFailure(
            self.client.create("/abc"), zookeeper.ZooKeeperException)

        self.assertFailure(
            self.client.set("/abc", "123"), zookeeper.ZooKeeperException)

    def test_connect_multiple_raises(self):
        """
        Attempting to connect on a client that is already connected raises
        an exception.
        """
        d = self.client.connect()

        def connect_again(client):
            d = client.connect()
            self.failUnlessFailure(d, zookeeper.ZooKeeperException)
            return d

        d.addCallback(connect_again)
        return d

    def test_bad_result_raises_error(self):
        """
        A not OK return from zookeeper api method result raises an exception.
        """
        mock_acreate = self.mocker.replace("zookeeper.acreate")
        mock_acreate(ANY, ANY, ANY, ANY, ANY, ANY)
        self.mocker.result(-100)
        self.mocker.replay()

        d = self.client.connect()

        def verify_failure(client):
            d = client.create("/abc")
            self.failUnlessFailure(d, zookeeper.ZooKeeperException)

        d.addCallback(verify_failure)
        return d

    def test_connection_watcher(self):
        """
        A connection watcher can be set that receives notices on when
        the connection state changes. Technically zookeeper would also
        use this as a global watcher for node watches, but zkpython
        doesn't expose that api, as its mostly considered legacy.

        its out of scope to simulate a connection level event within unit tests
        such as the server restarting.
        """
        d = self.client.connect()

        observed = []

        def watch(*args):
            observed.append(args)

        def set_global_watcher(client):
            client.set_connection_watcher(watch)
            return client

        def close_connection(client):
            return client.close()

        def verify_observed(stat):
            self.assertFalse(observed)

        d.addCallback(set_global_watcher)
        d.addCallback(close_connection)
        d.addCallback(verify_observed)

        return d

    def test_close_not_connected(self):
        """
        If the client is not connected, closing returns None.
        """
        self.assertEqual(self.client.close(), None)

    def test_invalid_connection_error_callback(self):
        self.assertRaises(TypeError,
                          self.client.set_connection_error_callback,
                          None)

    def test_invalid_session_callback(self):
        self.assertRaises(TypeError,
                          self.client.set_session_callback,
                          None)
