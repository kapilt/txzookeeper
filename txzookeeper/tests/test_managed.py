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

from twisted.internet.defer import inlineCallbacks  # , fail, succeed, Deferred

from txzookeeper import managed


#from txzookeeper.utils import retry_change

from txzookeeper.tests import ZookeeperTestCase, utils
#from txzookeeper.tests.proxy import ProxyFactory
from txzookeeper.tests import test_client


class ManagedClientCoreTests(ZookeeperTestCase):
    timeout = 5

    def setUp(self):
#        super(ZookeeperTestCase, self).setUp()
        super(test_client.ClientTests, self).setUp()
        zookeeper.set_debug_level(0)
        self.client = managed.ManagedClient("127.0.0.1:2181")
        self.cient2 = None

    def tearDown(self):
        super(ZookeeperTestCase, self).tearDown()
        if self.client.connected:
            utils.deleteTree(handle=self.client.handle)
            self.client.close()
        if self.client2 and self.client2.connected:
            self.client2.close()

    @inlineCallbacks
    def xtest_ephemeral_tracking(self):
        yield self.client.create("/foobar")
        self.assertFalse(self.client._ephemerals)
        self.client.create("/foobar/abc", flags=zookeeper.EPHEMERAL)

    @inlineCallbacks
    def xtest_child_watch_tracking(self):
        pass
