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

from zookeeper import NoNodeException
from twisted.internet.defer import (
    inlineCallbacks, returnValue, DeferredList, Deferred, succeed, fail)


from txzookeeper import ZookeeperClient
from txzookeeper.client import NotConnectedException
from txzookeeper.queue import Queue, ReliableQueue, SerializedQueue, QueueItem
from txzookeeper.tests import ZookeeperTestCase, utils


class QueueTests(ZookeeperTestCase):

    queue_factory = Queue

    def setUp(self):
        super(QueueTests, self).setUp()
        self.clients = []

    def tearDown(self):
        cleanup = False

        for client in self.clients:
            if not cleanup and client.connected:
                utils.deleteTree(handle=client.handle)
                cleanup = True
            if client.connected:
                client.close()
        super(QueueTests, self).tearDown()

    def compare_data(self, data, item):
        if isinstance(item, QueueItem):
            self.assertEqual(data, item.data)
        else:
            self.assertEqual(data, item)

    def consume_item(self, item):
        if isinstance(item, QueueItem):
            return item.delete(), item.data
        return None, item

    @inlineCallbacks
    def open_client(self, credentials=None):
        """
        Open a zookeeper client, optionally authenticating with the
        credentials if given.
        """
        client = ZookeeperClient("127.0.0.1:2181")
        self.clients.append(client)
        yield client.connect()
        if credentials:
            d = client.add_auth("digest", credentials)
            # hack to keep auth fast
            yield client.exists("/")
            yield d
        returnValue(client)

    def test_path_property(self):
        """
        The queue has a property that can be used to introspect its
        path in read only manner.
        """
        q = self.queue_factory("/moon", None)
        self.assertEqual(q.path, "/moon")

    def test_persistent_property(self):
        """
        The queue has a property that can be used to introspect
        whether or not the queue entries are persistent.
        """
        q = self.queue_factory("/moon", None, persistent=True)
        self.assertEqual(q.persistent, True)

    @inlineCallbacks
    def test_put_item(self):
        """
        An item can be put on the queue, and is stored in a node in
        queue's directory.
        """
        client = yield self.open_client()
        path = yield client.create("/queue-test")
        queue = self.queue_factory(path, client)
        item = "transform image bluemarble.jpg"
        yield queue.put(item)
        children = yield client.get_children(path)
        self.assertEqual(len(children), 1)
        data, stat = yield client.get("/".join((path, children[0])))
        self.compare_data(data, item)

    @inlineCallbacks
    def test_qsize(self):
        """
        The client implements a method which returns an unreliable
        approximation of the number of items in the queue (mirrors api
        of Queue.Queue), its unreliable only in that the value represents
        a temporal snapshot of the value at the time it was requested,
        not its current value.
        """
        client = yield self.open_client()
        path = yield client.create("/test-qsize")
        queue = self.queue_factory(path, client)

        yield queue.put("abc")
        size = yield queue.qsize()
        self.assertTrue(size, 1)

        yield queue.put("bcd")
        size = yield queue.qsize()
        self.assertTrue(size, 2)

        yield queue.get()
        size = yield queue.qsize()
        self.assertTrue(size, 1)

    @inlineCallbacks
    def test_invalid_put_item(self):
        """
        The queue only accepts string items.
        """
        client = yield self.open_client()
        queue = self.queue_factory("/unused", client)
        self.failUnlessFailure(queue.put(123), ValueError)

    @inlineCallbacks
    def test_get_with_invalid_queue(self):
        """
        If the queue hasn't been created an unknown node exception is raised
        on get.
        """
        client = yield self.open_client()
        queue = self.queue_factory("/unused", client)
        yield self.failUnlessFailure(queue.put("abc"), NoNodeException)

    @inlineCallbacks
    def test_put_with_invalid_queue(self):
        """
        If the queue hasn't been created an unknown node exception is raised
        on put.
        """
        client = yield self.open_client()
        queue = self.queue_factory("/unused", client)
        yield self.failUnlessFailure(queue.put("abc"), NoNodeException)

    @inlineCallbacks
    def test_unexpected_error_during_item_retrieval(self):
        """
        If an unexpected error occurs when reserving an item, the error is
        passed up to the get deferred's errback method.
        """
        test_client = yield self.open_client()
        path = yield test_client.create("/reliable-queue-test")

        # setup the test scenario
        mock_client = self.mocker.patch(test_client)
        mock_client.get_children_and_watch(path)
        watch = Deferred()
        self.mocker.result((succeed(["entry-000000"]), watch))

        item_path = "%s/%s" % (path, "entry-000000")
        mock_client.get(item_path)
        self.mocker.result(fail(SyntaxError("x")))
        self.mocker.replay()

        # odd behavior, this should return a failure, as above, but it returns
        # None
        d = self.queue_factory(path, mock_client).get()
        assert d
        self.failUnlessFailure(d, SyntaxError)
        yield d

    @inlineCallbacks
    def test_get_and_put(self):
        """
        Get can also be used on empty queues and returns a deferred that fires
        whenever an item is has been retrieved from the queue.
        """
        client = yield self.open_client()
        path = yield client.create("/queue-wait-test")
        data = "zebra moon"
        queue = self.queue_factory(path, client)
        d = queue.get()

        @inlineCallbacks
        def push_item():
            queue = self.queue_factory(path, client)
            yield queue.put(data)

        from twisted.internet import reactor
        reactor.callLater(0.1, push_item)

        item = yield d
        self.compare_data(data, item)

    @inlineCallbacks
    def test_interleaved_multiple_consumers_wait(self):
        """
        Multiple consumers and a producer adding and removing items on the
        the queue concurrently.
        """
        test_client = yield self.open_client()
        path = yield test_client.create("/multi-consumer-wait-test")
        results = []

        @inlineCallbacks
        def producer(item_count):
            from twisted.internet import reactor
            client = yield self.open_client()
            queue = self.queue_factory(path, client)

            items = []
            producer_done = Deferred()

            def iteration(i):
                if len(items) == (item_count - 1):
                    return producer_done.callback(None)
                items.append(i)
                queue.put(str(i))

            for i in range(item_count):
                reactor.callLater(i * 0.05, iteration, i)
            yield producer_done
            returnValue(items)

        @inlineCallbacks
        def consumer(item_count):
            client = yield self.open_client()
            queue = self.queue_factory(path, client)
            for i in range(item_count):
                try:
                    data = yield queue.get()
                    d, data = self.consume_item(data)
                    if d:
                        yield d
                except NotConnectedException:
                    # when the test closes, we need to catch this
                    # as one of the producers will likely hang.
                    returnValue(len(results))
                results.append((client.handle, data))

            returnValue(len(results))

        yield DeferredList(
            [DeferredList([consumer(3), consumer(2)], fireOnOneCallback=1),
             producer(6)])
        # as soon as the producer and either consumer is complete than the test
        # is done. Thus the only assertion we can make is the result is the
        # size of at least the smallest consumer.
        self.assertTrue(len(results) >= 2)

    @inlineCallbacks
    def test_staged_multiproducer_multiconsumer(self):
        """
        A real world scenario test, A set of producers filling a queue with
        items, and then a set of concurrent consumers pulling from the queue
        till its empty. The consumers use a non blocking get (defer raises
        exception on empty).
        """
        test_client = yield self.open_client()
        path = yield test_client.create("/multi-prod-cons")

        consume_results = []
        produce_results = []

        @inlineCallbacks
        def producer(start, offset):
            client = yield self.open_client()
            q = self.queue_factory(path, client)
            for i in range(start, start + offset):
                yield q.put(str(i))
                produce_results.append(str(i))

        @inlineCallbacks
        def consumer(max):
            client = yield self.open_client()
            q = self.queue_factory(path, client)
            attempts = range(max)
            for el in attempts:
                value = yield q.get()
                d, value = self.consume_item(value)
                if d:
                    yield d
                consume_results.append(value)
            returnValue(True)

        # two producers 20 items total
        yield DeferredList(
            [producer(0, 10), producer(10, 10)])

        children = yield test_client.get_children(path)
        self.assertEqual(len(children), 20)

        yield DeferredList(
            [consumer(8), consumer(8), consumer(4)])

        err = set(produce_results) - set(consume_results)
        self.assertFalse(err)

        self.assertEqual(len(consume_results), len(produce_results))


class ReliableQueueTests(QueueTests):

    queue_factory = ReliableQueue

    @inlineCallbacks
    def test_unprocessed_item_reappears(self):
        """
        If a queue consumer exits before processing an item, then
        the item will become visible to other queue consumers.
        """
        test_client = yield self.open_client()
        path = yield test_client.create("/reliable-queue-test")

        data = "rabbit stew"
        queue = self.queue_factory(path, test_client)
        yield queue.put(data)

        test_client2 = yield self.open_client()
        queue2 = self.queue_factory(path, test_client2)
        item = yield queue2.get()
        self.compare_data(data, item)

        d = queue.get()
        yield test_client2.close()

        item = yield d
        self.compare_data(data, item)

    @inlineCallbacks
    def test_processed_item_removed(self):
        """
        If a client processes an item, than that item is removed from the queue
        permanently.
        """
        test_client = yield self.open_client()
        path = yield test_client.create("/reliable-queue-test")

        data = "rabbit stew"
        queue = self.queue_factory(path, test_client)
        yield queue.put(data)
        item = yield queue.get()
        self.compare_data(data, item)
        yield item.delete()
        yield test_client.close()

        test_client2 = yield self.open_client()
        children = yield test_client2.get_children(path)
        children = [c for c in children if c.startswith(queue.prefix)]
        self.assertFalse(bool(children))


class SerializedQueueTests(ReliableQueueTests):

    queue_factory = SerializedQueue

    @inlineCallbacks
    def test_serialized_behavior(self):
        """
        The serialized queue behavior is such that even with multiple
        consumers, items are processed in order.
        """
        test_client = yield self.open_client()
        path = yield test_client.create("/serialized-queue-test")

        queue = self.queue_factory(path, test_client, persistent=True)

        yield queue.put("a")
        yield queue.put("b")

        test_client2 = yield self.open_client()
        queue2 = self.queue_factory(path, test_client2, persistent=True)

        d = queue2.get()

        def on_get_item_sleep_and_close(item):
            """Close the connection after we have the item."""
            from twisted.internet import reactor
            reactor.callLater(0.1, test_client2.close)
            return item

        d.addCallback(on_get_item_sleep_and_close)

        # fetch the item from queue2
        item1 = yield d
        # fetch the item from queue1, this will not get "b", because client2 is
        # still processing "a". When client2 closes its connection, client1
        # will get item "a"
        item2 = yield queue.get()

        self.compare_data("a", item2)
        self.assertEqual(item1.data, item2.data)
