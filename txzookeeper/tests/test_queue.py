
from twisted.internet.defer import (
    inlineCallbacks, returnValue, DeferredList, Deferred)

from txzookeeper import ZookeeperClient
from txzookeeper.client import NotConnectedException
from txzookeeper.queue import Queue, Empty
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

    @inlineCallbacks
    def test_put_get_nowait_item(self):
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
    def test_get_nowait_empty_queue(self):
        """
        Fetching from an empty queue raises the Empty exception if
        the client uses the the nowait api.
        """
        client = yield self.open_client()
        path = yield client.create("/queue-test")
        queue = Queue(path, client)
        queue_get = queue.get()
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
    def test_get_with_wait(self):
        """
        Instead of getting an empty error, get can also be used
        that returns a deferred that only is called back when an
        item is ready in the queue.
        """
        client = yield self.open_client()
        path = yield client.create("/queue-wait-test")
        item = "zebra moon"
        queue = Queue(path, client)
        d = queue.get_wait()

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
                    data = yield queue.get_wait()
                except NotConnectedException:
                    # when the test closes, we need to catch this
                    # as one of the producers will likely hang.
                    returnValue(len(results))
                results.append((client.handle, data))
            returnValue(len(results))

        yield DeferredList(
            [DeferredList([consumer(3), consumer(2)], fireOnOneCallback=1),
             producer(6)])
        self.assertEqual(len(results), 5)

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
                try:
                    value = yield q.get()
                except Empty:
                    returnValue("")
                consume_results.append(value)
            returnValue(True)

        # two producers 20 items total
        yield DeferredList(
            [producer(0, 10), producer(10, 10)])

        children = yield test_client.get_children(path)
        self.assertEqual(len(children), 20)

        yield DeferredList(
            [consumer(10), consumer(10), consumer(10)])

        yield DeferredList(
            [consumer(5), consumer(5), consumer(5), consumer(6)])

        err = set(produce_results)-set(consume_results)
        self.assertFalse(err)
        self.assertEqual(len(consume_results), len(produce_results))
