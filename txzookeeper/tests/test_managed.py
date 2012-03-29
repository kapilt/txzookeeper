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

#import json
#import time
import zookeeper

from twisted.internet.defer import inlineCallbacks, Deferred

from txzookeeper import managed


#from txzookeeper.utils import retry_change

from txzookeeper.tests import ZookeeperTestCase, utils
#from txzookeeper.tests.proxy import ProxyFactory
from txzookeeper.tests import test_client


class WatchTest(ZookeeperTestCase):

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

        (e,), _ = results.pop()
        self.assertEqual(
            str(e), "<ClientEvent session at '/foobar' state: connected>")
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

        (e,), _ = results.pop()
        self.assertEqual(
            str(e), "<ClientEvent session at '/foobar' state: connected>")
        d.callback(True)
        yield reset_done
        self.assertNotIn(w, self.watches._watches)


class SessionClientTests(test_client.ClientTests):
    timeout = 5

    def setUp(self):
        super(SessionClientTests, self).setUp()
        self.client = managed.SessionClient("127.0.0.1:2181")


class ManagedClientTests(test_client.ClientTests):
    timeout = 5

    def setUp(self):
        super(ManagedClientTests, self).setUp()
        self.client = managed.ManagedClient("127.0.0.1:2181")

    def test_wb_connect_after_timeout(self):
        """white box tests disabled for retryclient."""

    def test_wb_reconnect_after_timeout_and_close(self):
        """white box tests disabled for retryclient."""


class SessionClientExpireTests(ZookeeperTestCase):

    def setUp(self):
        super(SessionClientExpireTests, self).setUp()
        self.client = managed.SessionClient("127.0.0.1:2181", 3000)
        self.client2 = None

    def tearDown(self):
        if self.client.connected:
            utils.deleteTree(handle=self.client.handle)
            self.client.close()

        if self.client2 and self.client2.connected:
            self.client2.close()
        super(SessionClientExpireTests, self).tearDown()

    def xtest_ephemeral_and_watch_recreate(self):
        pass
