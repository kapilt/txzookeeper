import time
import zookeeper

from txzookeeper.tests import ZookeeperTestCase
from txzookeeper.client import ZookeeperClient, Wrapper

zookeeper = Wrapper(zookeeper)


class ClientTests(ZookeeperTestCase):

    def setUp(self):
        super(ClientTests, self).setUp()
        self.client = ZookeeperClient("127.0.0.1:2181")
        self.client2 = None

    def tearDown(self):
        if self.client.connected:
            self._deleteTree(handle=self.client.handle)
            d = self.client.close()
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

        def check_connected(connected):
            self.assertEquals(connected.state, zookeeper.CONNECTED_STATE)

        d.addCallback(check_connected)
        return d

    def test_create_ephemeral_node_and_close_connection(self):
        """
        The client can create transient nodes that are destroyed when the
        client is closed and the session is destroyed on the zookeeper servers.
        """
        d = self.client.connect()

        def test_create_ephemeral_node(connected):
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

        def test_create_ephemeral_node(connected):
            d = self.client.create(
                "/foobar", "rabbit", flags=zookeeper.EPHEMERAL)
            return d

        def check_node_path(path):
            self.assertEqual(path, "/foobar")
            return path

        def get_node_content(path):
            return self.client.get(path)

        def check_node_content(value):
            self.assertEqual(value[0], "rabbit")
            print zookeeper.state(self.client.handle)
            #time.sleep(0.1)
            return self.client.close()

        d.addCallback(test_create_ephemeral_node)
        d.addCallback(check_node_path)
        d.addCallback(get_node_content)
        d.addCallback(check_node_content)

        return d

    def xtest_create_persistent_node(self):
        """
        The client creates persistent nodes by default that exist
        independently of the session.
        """
        d = self.client.connect()

        def test_create_ephemeral_node(connected):
            d = self.client.create(
                "/foobar-transient", "rabbit")
            return d

        def check_node_path(path):
            self.assertEqual(path, "/foobar-transient")
            return path

        def close_connection(path):
            self.client.close()
            new_client = ZookeeperClient("127.0.0.1:2181")
            return new_client.connect()

        def check_node_exists(connected):
            data, stat = zookeeper.get(connected.handle, "/foobar-transient")
            self.assertEqual(data, "rabbit")
            zookeeper.close(connected.handle)

        d.addCallback(test_create_ephemeral_node)
        d.addCallback(check_node_path)
        d.addCallback(close_connection)
        d.addCallback(check_node_exists)

        return d

    def xtest_create_sequence_node(self):
        """
        The client can create a sequence node that
        """

    def xtest_create_duplicate_node(self):
        """
        Attempting to create a node that already exists results in
        a failure.
        """
