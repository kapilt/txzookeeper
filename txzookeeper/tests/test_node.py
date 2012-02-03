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


import base64
import hashlib

import zookeeper

from twisted.internet.defer import inlineCallbacks
from twisted.python.failure import Failure

from txzookeeper.node import ZNode
from txzookeeper.node import NodeEvent
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
        return "%s:%s" % (user, base64.b64encode(digest))

    def test_node_name_and_path(self):
        """
        Each node has name and path.
        """
        node = ZNode("/zoo/rabbit", self.client)
        self.assertEqual(node.name, "rabbit")
        self.assertEqual(node.path, "/zoo/rabbit")

    def test_node_event_repr(self):
        """
        Node events have a human-readable representation.
        """
        node = ZNode("/zoo", self.client)
        event = NodeEvent(4, None, node)
        self.assertEqual(repr(event), "<NodeEvent child at '/zoo'>")

    @inlineCallbacks
    def test_node_exists_nonexistant(self):
        """
        A node knows whether it exists or not.
        """
        node = ZNode("/zoo/rabbit", self.client)
        exists = yield node.exists()
        self.assertFalse(exists)

    @inlineCallbacks
    def test_node_set_data_on_nonexistant(self):
        """
        Setting data on a non existant node raises a no node exception.
        """
        node = ZNode("/zoo/rabbit", self.client)
        d = node.set_data("big furry ears")
        self.failUnlessFailure(d, zookeeper.NoNodeException)
        yield d

    @inlineCallbacks
    def test_node_create_set_data(self):
        """
        A node can be created and have its data set.
        """
        node = ZNode("/zoo/rabbit", self.client)
        data = "big furry ears"
        yield node.create(data)
        exists = yield self.client.exists("/zoo/rabbit")
        self.assertTrue(exists)
        node_data = yield node.get_data()
        self.assertEqual(data, node_data)
        data = data*2

        yield node.set_data(data)
        node_data = yield node.get_data()
        self.assertEqual(data, node_data)

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
        a non existant error.
        """
        d = ZNode("/zoo/giraffe", self.client).get_data()
        self.failUnlessFailure(d, zookeeper.NoNodeException)
        yield d

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
        If a node is deleted, attempting to set data on it
        raises a no node exception.
        """
        node = ZNode("/zoo/monkey", self.client)
        yield self.client.create("/zoo/monkey", "stripes")
        exists = yield node.exists()
        self.assertTrue(exists)
        yield self.client.delete("/zoo/monkey")
        d = node.set_data("banana")
        self.failUnlessFailure(d, zookeeper.NoNodeException)
        yield d

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
        exists, watch = yield node.exists_and_watch()

        self.client.create("/zoo/elephant")
        event = yield watch
        self.assertEqual(event.type, zookeeper.CREATED_EVENT)
        self.assertEqual(event.path, node.path)

    @inlineCallbacks
    def test_node_get_data_with_watch_on_update(self):
        """
        Subscribing to a node will get node update events.
        """
        yield self.client.create("/zoo/elephant")

        node = ZNode("/zoo/elephant", self.client)
        data, watch = yield node.get_data_and_watch()
        yield self.client.set("/zoo/elephant")
        event = yield watch
        self.assertEqual(event.type, zookeeper.CHANGED_EVENT)
        self.assertEqual(event.path, "/zoo/elephant")

    @inlineCallbacks
    def test_node_get_data_with_watch_on_delete(self):
        """
        Subscribing to a node will get node deletion events.
        """
        yield self.client.create("/zoo/elephant")

        node = ZNode("/zoo/elephant", self.client)
        data, watch = yield node.get_data_and_watch()

        yield self.client.delete("/zoo/elephant")
        event = yield watch
        self.assertEqual(event.type, zookeeper.DELETED_EVENT)
        self.assertEqual(event.path, "/zoo/elephant")

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
        event = yield watch
        self.assertEqual(event.path, "/zoo")
        self.assertEqual(event.type, zookeeper.CHILD_EVENT)
        self.assertEqual(event.type_name, "child")

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
        event = yield watch
        self.assertEqual(event.path, "/zoo")
        self.assertEqual(event.type, zookeeper.CHILD_EVENT)

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
