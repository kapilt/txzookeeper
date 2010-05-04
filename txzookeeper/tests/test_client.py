import time
import zookeeper

from twisted.internet.defer import Deferred
from txzookeeper.tests import ZookeeperTestCase
from txzookeeper.client import ZookeeperClient


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

        def verify_contents(data):
            self.assertEqual(data, "rabbit")

        d.addCallback(create_node)
        d.addCallback(get_contents)
        d.addCallback(verify_contents)

    def xtest_get_with_watcher_same_session(self):
        """
        The client can specify a callable watcher when invoking get. If the
        watcher is created from the same session that created the node, it
        will not be notified.

        The
        watcher will be called back when the client path is modified
        """
        d = self.client.connect()
        values = []

        def node_watch(*args, **kw):
            values.append(True)

        def create_node(client):
            return self.client.create("/foobar-watched", "rabbit")

        def get_node(path):
            return self.client.get(path, node_watch)

        def trigger_watch(data):
            zookeeper.delete(self.client.handle, "/foobar-watched")
            return data

        def verify_watch(*args):
            self.assertTrue(values)

        d.addCallback(create_node)
        d.addCallback(get_node)
        d.addCallback(trigger_watch)
        d.addCallback(verify_watch)

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
        The client can delete a ndoe via its delete method.
        """
        d = self.client.connect()

        def create_node(client):
            d = self.client.create(
                "/foobar-transient", "rabbit", flags=zookeeper.EPHEMERAL)
            return d

        def verify_exists(path):
            self.assertNotEqual(
                zookeeper.exists(self.client.handle, path), None)
            return path

        def delete_node(path):
            return self.client.delete(path)

        def verify_not_exists(args):
            print "H"*20
            print args
            self.assertEqual(
                zookeeper.exists(self.client.handle, "/foobar-transient"),
                None)

        d.addCallback(create_node)
        d.addCallback(verify_exists)
        d.addCallback(delete_node)
        d.addCallback(verify_not_exists)

    def test_exists_with_watcher(self):
        pass

    def test_create_sequence_node(self):
        """
        The client can create a sequence node that
        """

    def test_create_duplicate_node(self):
        """
        Attempting to create a node that already exists results in
        a failure.
        """
