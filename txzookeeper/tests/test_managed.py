#
#  Copyright (C) 2010-2012 Canonical Ltd. All Rights Reserved
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

import zookeeper

from twisted.internet.defer import inlineCallbacks, Deferred, DeferredList

from txzookeeper.client import ZookeeperClient, ClientEvent
from txzookeeper import managed
from txzookeeper.tests import ZookeeperTestCase, utils
from txzookeeper.tests import test_client


class WatchTest(ZookeeperTestCase):
    """Watch manager and watch tests"""

    def setUp(self):
        self.watches = managed.WatchManager()

    def tearDown(self):
        self.watches.clear()
        del self.watches

    def test_add_remove(self):

        w = self.watches.add("/foobar", "child", lambda x: 1)
        self.assertIn(
            "<Watcher child /foobar",
            str(w))
        self.assertIn(w, self.watches._watches)
        self.watches.remove(w)
        self.assertNotIn(w, self.watches._watches)
        self.watches.remove(w)

    @inlineCallbacks
    def test_watch_fire_removes(self):
        """Firing the watch removes it from the manager.
        """
        w = self.watches.add("/foobar", "child", lambda x: 1)
        yield w("a")
        self.assertNotIn(w, self.watches._watches)

    @inlineCallbacks
    def test_watch_fire_with_error_removes(self):
        """Firing the watch removes it from the manager.
        """
        d = Deferred()

        @inlineCallbacks
        def cb_error(e):
            yield d
            raise ValueError("a")

        w = self.watches.add("/foobar", "child", lambda x: 1)
        try:
            wd = w("a")
            d.callback(True)
            yield wd
        except ValueError:
            pass
        self.assertNotIn(w, self.watches._watches)

    @inlineCallbacks
    def test_reset_with_error(self):
        """A callback firing an error on reset is ignored.
        """
        output = self.capture_log("txzk.managed")
        d = Deferred()
        results = []

        @inlineCallbacks
        def callback(*args, **kw):
            results.append((args, kw))
            yield d
            raise ValueError("a")

        w = self.watches.add("/foobar", "child", callback)
        reset_done = self.watches.reset()

        e, _ = results.pop()
        self.assertEqual(
            str(ClientEvent(*e)),
            "<ClientEvent session at '/foobar' state: connected>")
        d.callback(True)
        yield reset_done
        self.assertNotIn(w, self.watches._watches)
        self.assertIn("Error reseting watch", output.getvalue())

    @inlineCallbacks
    def test_reset(self):
        """Reset fires a synthentic client event, and clears watches.
        """
        d = Deferred()
        results = []

        def callback(*args, **kw):
            results.append((args, kw))
            return d

        w = self.watches.add("/foobar", "child", callback)
        reset_done = self.watches.reset()

        e, _ = results.pop()
        self.assertEqual(
            str(ClientEvent(*e)),
            "<ClientEvent session at '/foobar' state: connected>")
        d.callback(True)
        yield reset_done
        self.assertNotIn(w, self.watches._watches)


class SessionClientTests(test_client.ClientTests):
    """Run through basic operations with SessionClient."""
    timeout = 5

    def setUp(self):
        super(SessionClientTests, self).setUp()
        self.client = managed.SessionClient("127.0.0.1:2181")

    def test_client_use_while_disconnected_returns_failure(self):
        # managed session client reconnects here.
        return True


class SessionClientExpireTests(ZookeeperTestCase):
    """Verify expiration behavior."""

    def setUp(self):
        super(SessionClientExpireTests, self).setUp()
        self.client = managed.ManagedClient("127.0.0.1:2181", 3000)
        self.client2 = None
        return self.client.connect()

    @inlineCallbacks
    def tearDown(self):
        self.client.close()

        self.client2 = ZookeeperClient("127.0.0.1:2181")
        yield self.client2.connect()
        utils.deleteTree(handle=self.client2.handle)
        yield self.client2.close()
        super(SessionClientExpireTests, self).tearDown()

    @inlineCallbacks
    def expire_session(self):
        assert self.client.connected
        self.client2 = ZookeeperClient(self.client.servers)
        yield self.client2.connect(client_id=self.client.client_id)
        yield self.client2.close()
        # It takes some time to propagate (1/3 session time as ping)
        yield self.sleep(2)

    @inlineCallbacks
    def test_session_expiration_conn(self):
        session_id = self.client.client_id[0]
        yield self.client.create("/fo-1", "abc")
        yield self.expire_session()
        yield self.client.exists("/")
        self.assertNotEqual(session_id, self.client.client_id[0])

    @inlineCallbacks
    def test_session_expiration_notification(self):
        session_id = self.client.client_id[0]
        c_d, w_d = self.client.get_and_watch("/")
        yield c_d
        d = self.client.subscribe_new_session()
        self.assertFalse(d.called)
        yield self.expire_session()
        yield d
        yield w_d
        self.assertNotEqual(session_id, self.client.client_id[0])

    @inlineCallbacks
    def test_session_expiration_conn_watch(self):
        session_id = self.client.client_id[0]
        yield self.client.create("/fo-1", "abc")
        yield self.expire_session()
        yield self.client.exists("/")
        self.assertNotEqual(session_id, self.client.client_id[0])

    @inlineCallbacks
    def test_invoked_watch_gc(self):
        c_d, w_d = yield self.client.get_children_and_watch("/")
        yield c_d
        yield self.client.create("/foo")
        yield w_d
        yield self.expire_session()
        yield self.client.create("/foo2")
        # Nothing should blow up
        yield self.sleep(0.2)

    @inlineCallbacks
    def test_ephemeral_and_watch_recreate(self):
        # Create some ephemeral nodes
        yield self.client.create("/fo-1", "abc", flags=zookeeper.EPHEMERAL)
        yield self.client.create("/fo-2", "def", flags=zookeeper.EPHEMERAL)

        # Create some watches
        g_d, g_w_d = self.client.get_and_watch("/fo-1")
        yield g_d

        c_d, c_w_d = self.client.get_children_and_watch("/")
        yield g_d

        e_d, e_w_d = self.client.get_children_and_watch("/fo-2")
        yield e_d

        # Expire the session
        yield self.expire_session()

        # Poof

        # Ephemerals back
        c, s = yield self.client.get("/fo-1")
        self.assertEqual(c, "abc")

        c, s = yield self.client.get("/fo-2")
        self.assertEqual(c, "def")

        # Watches triggered
        yield DeferredList(
            [g_w_d, c_w_d, e_w_d],
            fireOnOneErrback=True, consumeErrors=True)

        self.assertEqual(
            [str(d.result) for d in (g_w_d, c_w_d, e_w_d)],
            ["<ClientEvent session at '/fo-1' state: connected>",
             "<ClientEvent session at '/' state: connected>",
             "<ClientEvent session at '/fo-2' state: connected>"])

    @inlineCallbacks
    def test_ephemeral_no_track_sequence_nodes(self):
        """ Ephemeral tracking ignores sequence nodes.
        """
        yield self.client.create("/music", "abc")
        yield self.client.create(
            "/music/u2-", "abc",
            flags=zookeeper.EPHEMERAL | zookeeper.SEQUENCE)
        yield self.expire_session()
        children = yield self.client.get_children("/music")
        self.assertEqual(children, [])

    @inlineCallbacks
    def test_ephemeral_content_modification(self):
        yield self.client.create("/fo-1", "abc", flags=zookeeper.EPHEMERAL)
        yield self.client.set("/fo-1", "def")
        yield self.expire_session()
        c, s = yield self.client.get("/fo-1")
        self.assertEqual(c, "def")

    @inlineCallbacks
    def test_ephemeral_acl_modification(self):
        yield self.client.create("/fo-1", "abc", flags=zookeeper.EPHEMERAL)
        acl = [test_client.PUBLIC_ACL,
               dict(scheme="digest",
                    id="zebra:moon",
                    perms=zookeeper.PERM_ALL)]
        yield self.client.set_acl("/fo-1", acl)
        yield self.expire_session()
        n_acl, stat = yield self.client.get_acl("/fo-1")
        self.assertEqual(acl, n_acl)

    @inlineCallbacks
    def test_ephemeral_deletion(self):
        yield self.client.create("/fo-1", "abc", flags=zookeeper.EPHEMERAL)
        yield self.client.delete("/fo-1")
        yield self.expire_session()
        self.assertFalse((yield self.client.exists("/fo-1")))
