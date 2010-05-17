import zookeeper
import hashlib
import base64

from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.python.failure import Failure
from ensemble.lib.testing import TestCase
from ensemble.storage.node import ZNode
from txzookeeper.tests.utils import deleteTree
from txzookeeper import ZookeeperClient


class NodeTest(TestCase):

    def setUp(self):
        super(NodeTest, self).setUp()
        zookeeper.set_debug_level(zookeeper.LOG_LEVEL_ERROR)
        self.client = ZookeeperClient("127.0.0.1:2181", 2000)
        d = self.client.connect()

        def create_zoo(client):
            client.create("/zoo")
        d.addCallback(create_zoo)
        return d

    def tearDown(self):
        super(NodeTest, self).tearDown()
        deleteTree(handle=self.client.handle)
        if self.client.connected:
            self.client.close()
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
    def test_node_subscribe_nonexistant(self):
        """
        The node can be subscribed to, any node level events,
        created, deleted, modified will be sent to the subscriber.
        You can subscribe to non existant nodes.
        """
        subscriber_deferred = Deferred()

        def notify_subscriber(event, state, path):
            subscriber_deferred.callback((event, state, path))

        node = ZNode("/zoo/elephant", self.client)
        yield node.subscribe(notify_subscriber)

        self.client.create("/zoo/elephant")
        event, state, path = yield subscriber_deferred
        self.assertEqual(event, zookeeper.CREATED_EVENT)

    @inlineCallbacks
    def test_node_subscribe_update_event(self):
        """
        Subscribing to a node will get node update events.
        """
        subscriber_deferred = Deferred()

        def notify_subscriber(event, state, path):
            subscriber_deferred.callback((event, state, path))

        yield self.client.create("/zoo/elephant")

        node = ZNode("/zoo/elephant", self.client)
        yield node.subscribe(notify_subscriber)

        yield self.client.set("/zoo/elephant")
        event, state, path = yield subscriber_deferred
        self.assertEqual(event, zookeeper.CHANGED_EVENT)
        self.assertEqual(path, "/zoo/elephant")

    @inlineCallbacks
    def test_node_subscribe_delete_event(self):
        """
        Subscribing to a node will get node deletion events.
        """
        subscriber_deferred = Deferred()

        def notify_subscriber(event, state, path):
            subscriber_deferred.callback((event, state, path))

        yield self.client.create("/zoo/elephant")

        node = ZNode("/zoo/elephant", self.client)
        yield node.subscribe(notify_subscriber)

        yield self.client.set("/zoo/elephant")
        event, state, path = yield subscriber_deferred
        self.assertEqual(event, zookeeper.CHANGED_EVENT)
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
    def test_node_subscribe_children_create(self):
        """
        A node's children can explicitly subscribed to given existnace
        events for node creation and destruction.
        """
        subscriber_deferred = Deferred()

        def notify_subscriber(event, state, path):
            subscriber_deferred.callback((event, state, path))

        node = ZNode("/zoo", self.client)
        yield node.subscribe_children(notify_subscriber)
        yield self.client.create("/zoo/lion")
        event, state, path = yield subscriber_deferred
        self.assertEqual(path, "/zoo")
        self.assertEqual(event, zookeeper.CHILD_EVENT)

    @inlineCallbacks
    def test_node_subscribe_children_delete(self):
        """
        A node's children can explicitly subscribed to given existnace
        events for node creation and destruction.
        """
        subscriber_deferred = Deferred()

        def notify_subscriber(event, state, path):
            subscriber_deferred.callback((event, state, path))

        node = ZNode("/zoo", self.client)
        yield self.client.create("/zoo/lion")
        yield node.subscribe_children(notify_subscriber)
        yield self.client.delete("/zoo/lion")
        event, state, path = yield subscriber_deferred
        self.assertEqual(path, "/zoo")
        self.assertEqual(event, zookeeper.CHILD_EVENT)

    @inlineCallbacks
    def test_node_subscribe_children_modify(self):
        """
        Subscribing for child events, does not notify on child modification.
        """
        from twisted.internet import reactor
        subscriber_deferred = Deferred()

        # so we need to assert that we're not being called back. we
        # register the subscriber, and a timed function which invokes
        # the errback on the subscriber deferred.
        def notify_subscriber(event, state, path):
            if subscriber_deferred.called:
                return
            self.fail("should not be invoked (%s, %s, %s)"%(
                event, state, path))

        def timeout(*args):
            subscriber_deferred.callback((None, None, None))

        node = ZNode("/zoo", self.client)
        yield self.client.create("/zoo/lion")
        yield node.subscribe_children(notify_subscriber)
        yield self.client.exists("/zoo/lion")
        reactor.callLater(0.2, timeout)
        event, state, path = yield subscriber_deferred
        self.assertEqual(path, None)
        self.assertEqual(event, None)
