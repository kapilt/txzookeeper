import zookeeper

from twisted.internet.defer import Deferred
from txzookeeper.tests import ZookeeperTestCase
from txzookeeper.client import (
    ZookeeperClient, ZOO_OPEN_ACL_UNSAFE)

PUBLIC_ACL = ZOO_OPEN_ACL_UNSAFE


class ClientTests(ZookeeperTestCase):

    def setUp(self):
        super(ClientTests, self).setUp()
        self.client = ZookeeperClient("127.0.0.1:2181")
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

        def node_watch(type, path):
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

        def errback(failure):
            print failure

        d.addCallback(check_exists)
        d.addCallback(verify_exists)
        d.addErrback(errback)
        return d

    def test_exists_with_nonexistant_watcher(self):
        """
        The exists method can also be used to set an optional watcher on a
        node. The watch can be set on a node that does not yet exist.
        """
        d = self.client.connect()
        node_path = "/animals"
        watcher_deferred = Deferred()

        def node_watcher(event_type, path):
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

    def test_invalid_watcher(self):
        """
        Setting an invalid watcher raises a syntaxerror.
        """

    def test_authenticate(self):
        """
        The connection can be authenticated.
        """

    def test_sync(self):
        """
        Calling sync flushes the zooke
        """

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

        def verify_acl(acls):
            self.assertEqual(acls, [PUBLIC_ACL])

        d.addCallback(create_node)

        return d
