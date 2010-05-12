import hashlib
import base64

import zookeeper

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.python.failure import Failure

from txzookeeper.tests import ZookeeperTestCase, utils
from txzookeeper.client import (ZookeeperClient, ZOO_OPEN_ACL_UNSAFE)

PUBLIC_ACL = ZOO_OPEN_ACL_UNSAFE


class SecurityTests(ZookeeperTestCase):

    ident_bob = "bob:bob"
    ident_alice = "alice:alice"
    ident_eve = "eve:eve"
    ident_chuck = "chuck:chuck"

    ident_unittest = "unittest:unittest"

    def setUp(self):
        super(SecurityTests, self).setUp()
        self.clients = []

        self.test_cleanup_connection = ZookeeperClient("127.0.0.1:2181", 2000)
        self.test_cleanup_access_control = self.make_ac(
            self.ident_unittest, all=True, admin=True)
        return self.open_and_authenticate(
            self.test_cleanup_connection, self.ident_unittest)

    def tearDown(self):
        utils.deleteTree(handle=self.test_cleanup_connection.handle)
        for client in self.clients:
            if client.connected:
                client.close()
        self.test_cleanup_connection.close()

    @inlineCallbacks
    def open_and_authenticate(self, client, credentials):
        """authentication so the test always has access to clean up the
        zookeeper node tree. synchronous auth to avoid using deferred
        during setup."""
        yield client.connect()
        d = client.add_auth("digest", credentials)
        # hack to keep auth fast
        yield client.exists("/")
        yield d
        returnValue(client)

    @inlineCallbacks
    def connect_users(self, *users):
        clients = []
        for name in users:
            ident_user = getattr(self, "ident_%s"%(name), None)
            if ident_user is None:
                raise AttributeError("Invalid User %s"%(name))
            client = ZookeeperClient("127.0.0.1:2181", 3000)
            clients.append(client)
            yield self.open_and_authenticate(client, ident_user)
        self.clients.extend(clients)
        returnValue(clients)

    @inlineCallbacks
    def sync_clients(self, *clients):
        for client in clients:
            yield client.sync()

    def ensure_auth_failure(self, result):
        if isinstance(result, Failure):
            self.assertTrue(isinstance(
                result.value, zookeeper.NoAuthException))
            return
        self.fail("should have raised auth exception")

    def make_acl(self, *ac):
        ac = list(ac)
        ac.append(self.test_cleanup_access_control)
        return ac

    def make_ac(self, credentials, **kw):
        user, password = credentials.split(":")
        identity = "%s:%s" % (
            user,
            base64.b64encode(hashlib.new('sha1', credentials).digest()))

        permissions = None

        for name, perm in (('read', zookeeper.PERM_READ),
                           ('write', zookeeper.PERM_WRITE),
                           ('delete', zookeeper.PERM_DELETE),
                           ('create', zookeeper.PERM_CREATE),
                           ('admin', zookeeper.PERM_ADMIN),
                           ('all', zookeeper.PERM_ALL)):
            if name not in kw:
                continue

            if permissions is None:
                permissions = perm
            else:
                permissions = permissions | perm
        if permissions is None:
            raise SyntaxError("No permissions specified")
        acl = {'id': identity, 'scheme': 'digest', 'perms': permissions}
        return acl

    @inlineCallbacks
    def test_bob_message_for_alice_with_eve_reading(self):
        """
        If bob creates a message for alice to read, eve cannot read
        it.
        """
        bob, alice, eve = yield self.connect_users(
            "bob", "alice", "eve")
        yield bob.create(
            "/message_inbox", "message for alice",
            self.make_acl(
                self.make_ac(self.ident_bob, write=True, read=True),
                self.make_ac(self.ident_alice, read=True)))

        message_content, message_stat = yield alice.get("/message_inbox")
        self.assertEqual(message_content, "message for alice")

        d = eve.get("/message_inbox")
        d.addBoth(self.ensure_auth_failure)

        yield d

    @inlineCallbacks
    def test_alice_message_box_for_bob_with_eve_deleting(self):
        """
        If alice makes a folder to drop off messages to bob,
        neither bob nor eve can write to it, and bob can only
        read, and delete the messages. The permission for deleting
        is set on the container node. Even if eve has permission
        to delete the message node, without the container permission
        it will not succeed.
        """
        bob, alice, eve = yield self.connect_users("bob", "alice", "eve")

        yield alice.create(
            "/from_alice", "messages from alice",
            self.make_acl(
                self.make_ac(self.ident_alice, create=True, write=True),
                self.make_ac(self.ident_bob, read=True, delete=True))),

        # make sure all the clients have a consistent view
        yield self.sync_clients(alice, bob, eve)

        # bob can't create messages in the mailbox
        d = bob.create("/from_alice/love_letter", "test")
        d.addBoth(self.ensure_auth_failure)

        # alice's message can only be read by bob.
        path = yield alice.create(
            "/from_alice/appreciate_letter", "great",
            self.make_acl(
                self.make_ac(self.ident_eve, delete=True),
                self.make_ac(self.ident_bob, read=True),
                self.make_ac(self.ident_alice, create=True, write=True)))

        message_content, node_stat = yield bob.get(path)
        self.assertEqual(message_content, "great")

        # make sure all the clients have a consistent view
        yield self.sync_clients(alice, bob, eve)

        # eve can neither read nor delete
        d = eve.get(path)
        d.addBoth(self.ensure_auth_failure)
        yield d

        d = eve.delete(path)
        d.addBoth(self.ensure_auth_failure)
        yield d

        # bob can delete the message when he's done reading.
        yield bob.delete(path)

    def test_eve_can_discover_node_path(self):
        """
        One weakness of the zookeeper security model, is that it enables
        discovery of a node existance and its acl to any inquiring party.
        The acl is read off the node and then used as enforcement to any
        policy. Ideally it should validate exists and get_acl against
        the read permission on the node.

        Here bob creates a node that only he can read or write to, but
        eve can still infer the nodes existance.
        """
        bob, eve = yield self.connect_users("bob", "eve")
        yield bob.create("/bobsafeplace", "",
                         self.make_acl(self.make_ac(self.ident_bob, all=True)))

        yield bob.create("/bobsafeplace/secret-a", "supersecret",
                         self.make_acl(self.make_ac(self.ident_bob, all=True)))

        self.sync_clients(bob, eve)
        d = eve.exists("/bobsafeplace")

        def verify_node_stat(node_stat):
            self.assertEqual(node_stat["dataLength"], len("supersecret"))
            self.assertEqual(node_stat["version"], 0)

        d.addCallback(verify_node_stat)
        yield d

    def test_eve_can_discover_node_acl_(self):
        """
        One weakness of the zookeeper security model, is that it enables
        discovery of a node existance, its node stats, and its acl to
        any inquiring party.

        The acl is read off the node and then used as enforcement to any
        policy. Ideally it should validate exists and get_acl against
        the read permission on the node.

        Here bob creates a node that only he can read or write to, but
        eve can still infer the nodes existance. and reads its acl.
        """
        bob, eve = yield self.connect_users("bob", "eve")
        yield bob.create("/bobsafeplace", "",
                         self.make_acl(self.make_ac(self.ident_bob, all=True)))

        yield bob.create("/bobsafeplace/secret-a", "supersecret",
                         self.make_acl(self.make_ac(self.ident_bob, all=True)))

        self.sync_clients(bob, eve)
        d = eve.get_acl("/bobsafeplace/secret-a")

        def verify_node_stat_and_acl((acl, node_stat)):
            self.assertEqual(node_stat["dataLength"], len("supersecret"))
            self.assertEqual(node_stat["version"], 0)

        d.addCallback(verify_node_stat_and_acl)
        yield d
