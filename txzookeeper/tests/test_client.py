
import hashlib
import base64

import zookeeper

from mocker import ANY

from twisted.internet.defer import Deferred

from txzookeeper.tests import ZookeeperTestCase
from txzookeeper.client import (
    ZookeeperClient, ZOO_OPEN_ACL_UNSAFE)

PUBLIC_ACL = ZOO_OPEN_ACL_UNSAFE


class ClientTests(ZookeeperTestCase):

    def setUp(self):
        super(ClientTests, self).setUp()
        self.client = ZookeeperClient("127.0.0.1:2181", 3000)
        self.client2 = None

    def tearDown(self):
        if self.client.connected:
            self._deleteTree(handle=self.client.handle)
            self.client.close()
        del self.client
        if self.client2 and self.client2.connected:
            self.client2.close()
        super(ClientTests, self).tearDown()

    def _deleteTree(self, path="/", handle=1):
        """
        Destroy all the nodes in zookeeper.
        """
        for child in zookeeper.get_children(handle, path):
            if child == "zookeeper": # skip the metadata node
                continue
            child_path = "/"+("%s/%s"%(path, child)).strip("/")
            try:
                self._deleteTree(child_path, handle)
                zookeeper.delete(handle, child_path, -1)
            except zookeeper.ZooKeeperException, e:
                print "Error on path", child_path, e

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

    def test_get_with_watcher(self):
        """
        The client can specify a callable watcher when invoking get. The
        watcher will be called back when the client path is modified in
        another session.
        """
        d = self.client.connect()
        watch_deferred = Deferred()

        def node_watch(type, state, path):
            watch_deferred.callback((type, path))

        def create_node(client):
            return self.client.create("/foobar-watched", "rabbit")

        def get_node(path):
            return self.client.get(path, node_watch)

        def new_connection(data):
            self.client2 = ZookeeperClient("127.0.0.1:2181")
            return self.client2.connect()

        def trigger_watch(client):
            zookeeper.set(self.client2.handle, "/foobar-watched", "abc")
            return watch_deferred

        def verify_watch((event_type, path)):
            self.assertEqual(path, "/foobar-watched")
            self.assertEqual(event_type, zookeeper.CHANGED_EVENT)

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

    def test_exists_with_nonexistant_watcher(self):
        """
        The exists method can also be used to set an optional watcher on a
        node. The watch can be set on a node that does not yet exist.
        """
        d = self.client.connect()
        node_path = "/animals"
        watcher_deferred = Deferred()

        def node_watcher(event_type, state, path):
            watcher_deferred.callback((event_type, path))

        def create_container(path):
            return self.client.create(node_path, "")

        def check_exists(path):
            return self.client.exists(
                "%s/wooly-mammoth"%node_path, node_watcher)

        def new_connection(node_stat):
            self.assertFalse(node_stat)
            self.client2 = ZookeeperClient("127.0.0.1:2181")
            return self.client2.connect()

        def create_node(client):
            self.assertEqual(client.connected, True)
            return self.client2.create("%s/wooly-mammoth"%node_path, "extinct")

        def shim(path):
            return watcher_deferred

        def verify_watch((event_type, path)):
            self.assertEqual(path, "%s/wooly-mammoth"%node_path)
            self.assertEqual(event_type, zookeeper.CREATED_EVENT)

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

    def test_get_children_with_watch(self):
        """
        The get_children method optionally takes a watcher callable which will
        be notified when the node is modified, or a child deleted or added.
        """

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
        Getting children of a nonexistant node also returns an empty list
        """
        d = self.client.connect()

        def get_children(client):
            return client.get_children("/tower")

        def verify_children(children):
            self.assertEqual(children, [])

        d.addCallback(get_children)
        d.addCallback(verify_children)
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
            client.get_children("/orchard")
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
            self.assertTrue(failed) # we should have hit the errback

        d.addCallback(add_auth_one)
        d.addCallback(create_node)
        d.addCallback(try_node_access)
        d.addErrback(node_access_failed)
        d.addCallback(add_auth_two)
        d.addCallback(try_node_access)
        d.addCallback(verify_node_access)

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

        def create_node(client):
            return client.create("/moose")

        def get_acl(path):
            mock_client = self.mocker.patch(self.client)
            mock_client._check_result(ANY, True)
            self.mocker.result(zookeeper.ZooKeeperException("foobar"))
            self.mocker.replay()
            return self.client.get_acl(path)

        def verify_failure(failure):
            self.assertTrue(
                isinstance(failure.value, zookeeper.ZooKeeperException))

        d.addCallback(create_node)
        d.addCallback(get_acl)
        d.addErrback(verify_failure)
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

    def test_sync_error(self):
        """
        On error the sync callback returns a an exception/failure.
        """

        d = self.client.connect()

        def create_node(client):
            return client.create("/abc")

        def client_sync(path):
            mock_client = self.mocker.patch(self.client)
            mock_client._check_result(ANY, True)
            self.mocker.result(zookeeper.ZooKeeperException("foobar"))
            self.mocker.replay()
            return self.client.sync(path)

        def verify_failure(failure):
            self.assertTrue(
                isinstance(failure.value, zookeeper.ZooKeeperException))

        def assert_failed(extra):
            self.fail("Should have gone to errback")

        d.addCallback(create_node)
        d.addCallback(client_sync)
        d.addCallback(assert_failed)
        d.addErrback(verify_failure)
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
        self.assertEqual(self.client.session_timeout, None)
        d = self.client.connect()

        def verify_session_timeout(client):
            self.assertEqual(client.session_timeout, 4000)

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
            self.assertEqual(failure.value.args, ("invalid watcher",))
            self.assertTrue(isinstance(failure.value, SyntaxError))

        d.addCallback(set_invalid_watcher)
        d.addErrback(verify_invalid)
        return d

    def test_connect_ensured(self):
        """
        All of the client apis (with the exception of connect) attempt
        to ensure the client is connected before executing an operation.
        """
        self.assertRaises(
            zookeeper.ZooKeeperException, self.client.get_children, "/abc")

        self.assertRaises(
            zookeeper.ZooKeeperException, self.client.create, "/abc")

        self.assertRaises(
            zookeeper.ZooKeeperException, self.client.set, "/abc", "123")

    def test_connect_multiple_raises(self):
        """
        Attempting to connect on a client that is already connected raises
        an exception.
        """
        d = self.client.connect()

        def connect_again(client):
            self.assertRaises(
                zookeeper.ZooKeeperException, client.connect)

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
            self.assertRaises(
                zookeeper.ZooKeeperException, client.create, "/abc")

        d.addCallback(verify_failure)
        return d

    def test_connection_watcher(self):
        """
        A connection watcher can be set that recieves notices on when the
        connection state changes. Technically zookeeper would also use this as
        a global watcher for node state changes, but zkpython doesn't expose
        that api, as its mostly considered legacy.
        """
        d = self.client.connect()

        observed = []
        zookeeper.set_debug_level(zookeeper.LOG_LEVEL_DEBUG)

        def watch(*args):
            print "watch", observed
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
