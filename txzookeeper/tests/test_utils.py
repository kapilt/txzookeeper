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

import zookeeper

from twisted.internet.defer import inlineCallbacks, fail, succeed

from txzookeeper import ZookeeperClient
from txzookeeper.utils import retry_change
from txzookeeper.tests.mocker import MATCH
from txzookeeper.tests import ZookeeperTestCase, utils

MATCH_STAT = MATCH(lambda x: isinstance(x, dict))


class RetryChangeTest(ZookeeperTestCase):

    def update_function_increment(self, content, stat):
        if not content:
            return str(0)
        return str(int(content) + 1)

    def setUp(self):
        super(RetryChangeTest, self).setUp()
        self.client = ZookeeperClient("127.0.0.1:2181")
        return self.client.connect()

    def tearDown(self):
        utils.deleteTree("/", self.client.handle)
        self.client.close()

    @inlineCallbacks
    def test_node_create(self):
        """
        retry_change will create a node if one does not exist.
        """
        #use a mock to ensure the change function is only invoked once
        func = self.mocker.mock()
        func(None, None)
        self.mocker.result("hello")
        self.mocker.replay()

        yield retry_change(
            self.client, "/magic-beans", func)

        content, stat = yield self.client.get("/magic-beans")
        self.assertEqual(content, "hello")
        self.assertEqual(stat["version"], 0)

    @inlineCallbacks
    def test_node_update(self):
        """
        retry_change will update an existing node.
        """
        #use a mock to ensure the change function is only invoked once
        func = self.mocker.mock()
        func("", MATCH_STAT)
        self.mocker.result("hello")
        self.mocker.replay()

        yield self.client.create("/magic-beans")
        yield retry_change(
            self.client, "/magic-beans", func)

        content, stat = yield self.client.get("/magic-beans")
        self.assertEqual(content, "hello")
        self.assertEqual(stat["version"], 1)

    def test_error_in_change_function_propogates(self):
        """
        an error in the change function propogates to the caller.
        """

        def error_function(content, stat):
            raise SyntaxError()

        d = retry_change(self.client, "/magic-beans", error_function)
        self.failUnlessFailure(d, SyntaxError)
        return d

    @inlineCallbacks
    def test_concurrent_update_bad_version(self):
        """
        If the node is updated after the retry function has read
        the node but before the content is set, the retry function
        will perform another read/change_func/set cycle.
        """
        yield self.client.create("/animals")
        content, stat = yield self.client.get("/animals")
        yield self.client.set("/animals", "5")

        real_get = self.client.get
        p_client = self.mocker.proxy(self.client)
        p_client.get("/animals")
        self.mocker.result(succeed((content, stat)))

        p_client.get("/animals")
        self.mocker.call(real_get)

        self.mocker.replay()

        yield retry_change(
            p_client, "/animals", self.update_function_increment)

        content, stat = yield real_get("/animals")
        self.assertEqual(content, "6")
        self.assertEqual(stat["version"], 2)

    @inlineCallbacks
    def test_create_node_exists(self):
        """
        If the node is created after the retry function has determined
        the node doesn't exist but before the node is created by the
        retry function. the retry function will perform another
        read/change_func/set cycle.
        """
        yield self.client.create("/animals", "5")

        real_get = self.client.get
        p_client = self.mocker.patch(self.client)
        p_client.get("/animals")
        self.mocker.result(fail(zookeeper.NoNodeException()))

        p_client.get("/animals")
        self.mocker.call(real_get)
        self.mocker.replay()

        yield retry_change(
            p_client, "/animals", self.update_function_increment)

        content, stat = yield real_get("/animals")
        self.assertEqual(content, "6")
        self.assertEqual(stat["version"], 1)

    def test_set_node_does_not_exist(self):
        """
        if the retry function goes to update a node which has been
        deleted since it was read, it will cycle through to another
        read/change_func set cycle.
        """
        real_get = self.client.get
        p_client = self.mocker.patch(self.client)
        p_client.get("/animals")
        self.mocker.result("5", {"version": 1})

        p_client.get("/animals")
        self.mocker.call(real_get)

        yield retry_change(
            p_client, "/animals", self.update_function_increment)

        content, stat = yield real_get("/animals")
        self.assertEqual(content, "0")
        self.assertEqual(stat["version"], 0)

    def test_identical_content_noop(self):
        """
        If the change function generates identical content to
        the existing node, the retry change function exits without
        modifying the node.
        """
        self.client.create("/animals", "hello")

        def update(content, stat):
            return content

        yield retry_change(self.client, "/animals", update)
        content, stat = self.client.get("/animals")
        self.assertEqual(content, "hello")
        self.assertEqual(stat["version"], 0)
