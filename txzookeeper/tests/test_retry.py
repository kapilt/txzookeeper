#
#  Copyright (C) 2010, 2011 Canonical Ltd. All Rights Reserved
#
#  This file is part of txzookeeper.
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

from twisted.internet.defer import Deferred
from twisted.internet.base import DelayedCall
from twisted.python.failure import Failure

import zookeeper

from mocker import ANY, MATCH
from txzookeeper.tests import ZookeeperTestCase, utils
from txzookeeper.client import (
    ZookeeperClient, ZOO_OPEN_ACL_UNSAFE, ConnectionTimeoutException,
    ConnectionException, NotConnectedException, ClientEvent)

from txzookeeper.retry import RetryClient
from txzookeeper.tests.test_client import ClientTests

PUBLIC_ACL = ZOO_OPEN_ACL_UNSAFE


def match_deferred(arg):
    return isinstance(arg, Deferred)

DEFERRED_MATCH = MATCH(match_deferred)


class RetryClientTests(ClientTests):

    def setUp(self):
        super(RetryClientTests, self).setUp()
        self.client = RetryClient(ZookeeperClient("127.0.0.1:2181", 3000))
        self.client2 = None

    def tearDown(self):
        if self.client.connected:
            utils.deleteTree(handle=self.client.handle)
            self.client.close()

        if self.client2 and self.client2.connected:
            self.client2.close()

        super(RetryClientTests, self).tearDown()

    def test_wb_connect_after_timeout(self):
        """white box tests disabled for retryclient."""

    def test_wb_reconnect_after_timeout_and_close(self):
        """white box tests disabled for retryclient."""
