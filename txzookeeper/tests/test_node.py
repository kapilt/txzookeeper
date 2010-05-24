import zookeeper
import hashlib
import base64

from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.python.failure import Failure
from txzookeeper.node import ZNode
from txzookeeper.tests import TestCase
from txzookeeper.tests.utils import deleteTree
from txzookeeper import ZookeeperClient


class NodeTest(TestCase):

    def setUp(self):
        super(NodeTest, self).setUp()
        zookeeper.set_debug_level(zookeeper.LOG_LEVEL_ERROR)
        self.client = ZookeeperClient("127.0.0.1:2181", 2000)
        d = self.client.connect()
        self.client2 = None

        def create_zoo(client):
            client.create("/zoo")
        d.addCallback(create_zoo)
        return d

    def tearDown(self):
        super(NodeTest, self).tearDown()
        deleteTree(handle=self.client.handle)
        if self.client.connected:
            self.client.close()
        if self.client2 and self.client2.connected:
            self.client2.close()
        zookeeper.set_debug_level(zookeeper.LOG_LEVEL_DEBUG)

    def _make_digest_identity(self, credentials):
        user, password = credentials.split(":")
        digest = hashlib.new("sha1", credentials).digest()
        return "%s:%s"%(user, base64.b64encode(digest))

    def test_node_name_and_path(self):
        """
        Each node has name and path.
        """
        node = ZNode("/zoo/rabbit", self.client)
        self.assertEqual(node.name, "rabbit")
        self.assertEqual(node.path, "/zoo/rabbit")

    @inlineCallbacks
    def test_node_exists_nonexistant(self):
        """
        A node knows whether it exists or not.
        """
        node = ZNode("/zoo/rabbit", self.client)
        exists = yield node.exists()
        self.assertFalse(exists)

    @inlineCallbacks
    def test_node_set_data_create(self):
        """
        Data can be set on a node, if the node doesn't exist,
        setting its data creates the node.
        """
        node = ZNode("/zoo/rabbit", self.client)
        yield node.set_data("big furry ears")
        exists = yield self.client.exists("/zoo/rabbit")
        self.assertTrue(exists)

    @inlineCallbacks
    def test_node_get_data(self):
        """
        Data can be fetched from a node.
        """
        yield self.client.create("/zoo/giraffe", "mouse")
        data = yield ZNode("/zoo/giraffe", self.client).get_data()
        self.assertEqual(data, "mouse")

    @inlineCallbacks
    def test_node_get_data_nonexistant(self):
        """
        Attempting to fetch data from a nonexistant node returns
        None.
        """
        data = yield ZNode("/zoo/giraffe", self.client).get_data()
        self.assertEqual(data, None)

    @inlineCallbacks
    def test_node_get_acl(self):
        """
        The ACL for a node can be retrieved.
        """
        yield self.client.create("/zoo/giraffe")
        acl = yield ZNode("/zoo/giraffe", self.client).get_acl()
        self.assertEqual(len(acl), 1)
        self.assertEqual(acl[0]['scheme'], 'world')

    def test_node_get_acl_nonexistant(self):
        """
        The fetching the ACL for a non-existant node results in an error.
        """
        node = ZNode("/zoo/giraffe", self.client)

        def assert_failed(failed):
            if not isinstance(failed, Failure):
                self.fail("Should have failed")
            self.assertTrue(
                isinstance(failed.value, zookeeper.NoNodeException))
        d = node.get_acl()
        d.addBoth(assert_failed)
        return d

    @inlineCallbacks
    def test_node_set_acl(self):
        """
        The ACL for a node can be modified.
        """
        path = yield self.client.create("/zoo/giraffe")
        credentials = "zebra:moon"
        acl = [{"id": self._make_digest_identity(credentials),
                "scheme": "digest",
                "perms":zookeeper.PERM_ALL}]
        node = ZNode(path, self.client)
        # little hack around slow auth issue 770 zookeeper
        d = self.client.add_auth("digest", credentials)
        yield node.set_acl(acl)
        yield d
        node_acl, stat = yield self.client.get_acl(path)
        self.assertEqual(node_acl, acl)

    @inlineCallbacks
    def test_node_set_data_update_with_cached_exists(self):
        """
        Data can be set on an existing node, updating it
        in place.
        """
        node = ZNode("/zoo/monkey", self.client)
        yield self.client.create("/zoo/monkey", "stripes")
        exists = yield node.exists()
        self.assertTrue(exists)
        yield node.set_data("banana")
        data, stat = yield self.client.get("/zoo/monkey")
        self.assertEqual(data, "banana")

    @inlineCallbacks
    def test_node_set_data_update_with_invalid_cached_exists(self):
        """
        Data can be set on an existing node, updating it
        in place.
        """
        node = ZNode("/zoo/monkey", self.client)
        yield self.client.create("/zoo/monkey", "stripes")
        exists = yield node.exists()
        self.assertTrue(exists)
        yield self.client.delete("/zoo/monkey")
        yield node.set_data("banana")
        data, stat = yield self.client.get("/zoo/monkey")
        self.assertEqual(data, "banana")

    @inlineCallbacks
    def test_node_set_data_update_with_exists(self):
        """
        Data can be set on an existing node, updating it
        in place.
        """
        node = ZNode("/zoo/monkey", self.client)
        yield self.client.create("/zoo/monkey", "stripes")
        yield node.set_data("banana")
        data, stat = yield self.client.get("/zoo/monkey")
        self.assertEqual(data, "banana")

    @inlineCallbacks
    def test_node_exists_with_watch_nonexistant(self):
        """
        The node's existance can be checked with the exist_watch api
        a deferred will be returned and any node level events,
        created, deleted, modified invoke the callback. You can
        get these create event callbacks for non existant nodes.
        """
        node = ZNode("/zoo/elephant", self.client)
        exists, watch = yield node.exists_with_watch()

        self.client.create("/zoo/elephant")
        event, state, path = yield watch
        self.assertEqual(event, zookeeper.CREATED_EVENT)

    @inlineCallbacks
    def test_node_get_data_with_watch_on_update(self):
        """
        Subscribing to a node will get node update events.
        """
        yield self.client.create("/zoo/elephant")

        node = ZNode("/zoo/elephant", self.client)
        data, watch = yield node.get_data_and_watch()
        yield self.client.set("/zoo/elephant")
        event, state, path = yield watch
        self.assertEqual(event, zookeeper.CHANGED_EVENT)
        self.assertEqual(path, "/zoo/elephant")

    @inlineCallbacks
    def test_node_get_data_with_watch_on_delete(self):
        """
        Subscribing to a node will get node deletion events.
        """
        yield self.client.create("/zoo/elephant")

        node = ZNode("/zoo/elephant", self.client)
        data, watch = yield node.get_data_and_watch()

        yield self.client.delete("/zoo/elephant")
        event, state, path = yield watch
        self.assertEqual(event, zookeeper.DELETED_EVENT)
        self.assertEqual(path, "/zoo/elephant")

    @inlineCallbacks
    def test_node_children(self):
        """
        A node's children can be introspected.
        """
        node = ZNode("/zoo", self.client)
        node_path_a = yield self.client.create("/zoo/lion")
        node_path_b = yield self.client.create("/zoo/tiger")
        children = yield node.get_children()
        children.sort()
        self.assertEqual(children[0].path, node_path_a)
        self.assertEqual(children[1].path, node_path_b)

    @inlineCallbacks
    def test_node_children_by_prefix(self):
        """
        A node's children can be introspected optionally with a prefix.
        """
        node = ZNode("/zoo", self.client)
        node_path_a = yield self.client.create("/zoo/lion")
        yield self.client.create("/zoo/tiger")
        children = yield node.get_children("lion")
        children.sort()
        self.assertEqual(children[0].path, node_path_a)
        self.assertEqual(len(children), 1)

    @inlineCallbacks
    def test_node_get_children_with_watch_create(self):
        """
        A node's children can explicitly be watched to given existance
        events for node creation and destruction.
        """
        node = ZNode("/zoo", self.client)
        children, watch = yield node.get_children_and_watch()
        yield self.client.create("/zoo/lion")
        event, state, path = yield watch
        self.assertEqual(path, "/zoo")
        self.assertEqual(event, zookeeper.CHILD_EVENT)

    @inlineCallbacks
    def test_node_get_children_with_watch_delete(self):
        """
        A node's children can explicitly be watched to given existance
        events for node creation and destruction.
        """
        node = ZNode("/zoo", self.client)
        yield self.client.create("/zoo/lion")
        children, watch = yield node.get_children_and_watch()
        yield self.client.delete("/zoo/lion")
        event, state, path = yield watch
        self.assertEqual(path, "/zoo")
        self.assertEqual(event, zookeeper.CHILD_EVENT)

    @inlineCallbacks
    def test_bad_version_error(self):
        """
        The node captures the node version on any read operations, which
        it utilizes for write operations. On a concurrent modification error
        the node return a bad version error, this also clears the cached
        state so subsequent modifications will be against the latest version,
        unless the cache is seeded again by a read operation.
        """
        node = ZNode("/zoo/lion", self.client)

        self.client2 = ZookeeperClient("127.0.0.1:2181")
        yield self.client2.connect()

        yield self.client.create("/zoo/lion", "mouse")
        yield node.get_data()
        yield self.client2.set("/zoo/lion", "den2")
        data = yield self.client.exists("/zoo/lion")
        self.assertEqual(data['version'], 1)
        d = node.set_data("zebra")
        self.failUnlessFailure(d, zookeeper.BadVersionException)
        yield d

        # after failure the cache is deleted, and a set proceeds
        yield node.set_data("zebra")
        data = yield node.get_data()
        self.assertEqual(data, "zebra")
