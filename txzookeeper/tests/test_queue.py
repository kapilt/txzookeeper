
from zookeeper import NoNodeException
from twisted.internet.defer import (
    inlineCallbacks, returnValue, DeferredList, Deferred)

from txzookeeper import ZookeeperClient
from txzookeeper.client import NotConnectedException
from txzookeeper.queue import Queue
from txzookeeper.tests import ZookeeperTestCase, utils


class QueueTests(ZookeeperTestCase):

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
        q = Queue("/moon", None)
        self.assertEqual(q.path, "/moon")

    def test_persistent_property(self):
        """
        The queue has a property that can be used to introspect
        whether or not the queue entries are persistent.
        """
        q = Queue("/moon", None, persistent=True)
        self.assertEqual(q.persistent, True)

    @inlineCallbacks
    def xtest_put_get_nowait_item(self):
        """
        We can put and get an item off the queue.
        """
        client = yield self.open_client()
        path = yield client.create("/queue-test")
        queue = Queue(path, client)
        item = "transform image bluemarble.jpg"
        yield queue.put(item)
        item2 = yield queue.get_nowait()
        self.assertEqual(item, item2)

    @inlineCallbacks
    def xtest_get_nowait_empty_queue(self):
        """
        Fetching from an empty queue raises the Empty exception if
        the client uses the the nowait api.
        """
        client = yield self.open_client()
        path = yield client.create("/queue-test")
        queue = Queue(path, client)
        queue_get = queue.get_nowait()
        yield self.failUnlessFailure(queue_get, Empty)

    @inlineCallbacks
    def test_put_item(self):
        """
        An item can be put on the queue, and is stored in a node in
        queue's directory.
        """
        client = yield self.open_client()
        path = yield client.create("/queue-test")
        queue = Queue(path, client)
        item = "transform image bluemarble.jpg"
        yield queue.put(item)
        children = yield client.get_children(path)
        self.assertEqual(len(children), 1)
        data, stat = yield client.get("/".join((path, children[0])))
        self.assertEqual(data, item)

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
        queue = Queue(path, client)

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
        queue = Queue("/unused", client)
        self.assertRaises(ValueError, queue.put, 123)

    @inlineCallbacks
    def test_get_with_invalid_queue(self):
        """
        If the queue hasn't been created an unknown node exception is raised
        on get.
        """
        client = yield self.open_client()
        queue = Queue("/unused", client)
        self.failUnlessFailure(queue.get(), NoNodeException)

    @inlineCallbacks
    def test_put_with_invalid_queue(self):
        """
        If the queue hasn't been created an unknown node exception is raised
        on put.
        """
        client = yield self.open_client()
        queue = Queue("/unused", client)
        self.failUnlessFailure(queue.put("abc"), NoNodeException)

    @inlineCallbacks
    def test_get_and_put(self):
        """
        get can also be used on empty queues and returns a deferred that fires
        whenever an item is has been retrieved from the queue.
        """
        client = yield self.open_client()
        path = yield client.create("/queue-wait-test")
        item = "zebra moon"
        queue = Queue(path, client)
        d = queue.get()

        @inlineCallbacks
        def push_item():
            queue = Queue(path, client)
            yield queue.put("zebra moon")

        def verify_item_received(data):
            self.assertEqual(data, item)
            return data

        d.addCallback(verify_item_received)

        from twisted.internet import reactor
        reactor.callLater(0.1, push_item)

        data = yield d
        self.assertEqual(data, item)

    @inlineCallbacks
    def test_interleaved_multiple_consumers_wait(self):
        """
        Multiple consumers waiting (deferred block) on a queue works as
        expected.
        """
        test_client = yield self.open_client()
        path = yield test_client.create("/multi-consumer-wait-test")
        results = []

        @inlineCallbacks
        def producer(item_count):
            from twisted.internet import reactor
            client = yield self.open_client()
            queue = Queue(path, client)

            items = []
            producer_done = Deferred()

            def iteration(i):
                if len(items) == (item_count-1):
                    return producer_done.callback(None)
                items.append(i)
                queue.put(str(i))

            for i in range(item_count):
                reactor.callLater(i*0.05, iteration, i)
            yield producer_done
            returnValue(items)

        @inlineCallbacks
        def consumer(item_count):
            client = yield self.open_client()
            queue = Queue(path, client)
            for i in range(item_count):
                try:
                    data = yield queue.get()
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
        # size of at the small consumer.
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
            q = Queue(path, client)
            for i in range(start, start+offset):
                yield q.put(str(i))
                produce_results.append(str(i))

        @inlineCallbacks
        def consumer(max):
            client = yield self.open_client()
            q = Queue(path, client)
            attempts = range(max)
            for el in attempts:
                value = yield q.get()
                consume_results.append(value)
            returnValue(True)

        # two producers 20 items total
        yield DeferredList(
            [producer(0, 10), producer(10, 10)])

        children = yield test_client.get_children(path)
        self.assertEqual(len(children), 20)

        yield DeferredList(
            [consumer(8), consumer(8), consumer(4)])

        err = set(produce_results)-set(consume_results)
        self.assertFalse(err)
        self.assertEqual(len(consume_results), len(produce_results))
