
import zookeeper

from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList

from txzookeeper import ZookeeperClient
from txzookeeper.queue import Queue, Empty
from txzookeeper.tests import ZookeeperTestCase, utils


class LockTests(ZookeeperTestCase):

    def setUp(self):
        super(LockTests, self).setUp()
        self.clients = []

    def tearDown(self):
        cleanup = False
        for client in self.clients:
            if not cleanup and client.connected:
                utils.deleteTree(handle=client.handle)
                cleanup = True
            client.close()

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
    def test_put_get_item(self):
        client = yield self.open_client()
        path = yield client.create("/queue-test")
        queue = Queue(path, client)
        item = "transform image bluemarble.jpg"
        yield queue.put_nowait(item)
        item2 = yield queue.get_nowait()
        self.assertEqual(item, item2)

    @inlineCallbacks
    def test_multiconsumer_multiproducer(self):
        test_client = yield self.open_client()
        path = yield test_client.create("/multi-prod-cons")

        results = []

        @inlineCallbacks
        def producer(start, offset):
            client = yield self.open_client()
            q = Queue(path, client)
            for i in range(start, start+offset):
                yield q.put_nowait(str(i))

        @inlineCallbacks
        def consumer(max):
            client = yield self.open_client()
            q = Queue(path, client)

            try:
                for index in range(max):
                    value = yield q.get_nowait()
                    results.append(value)
            except Empty:
                pass

        yield DeferredList(
            [producer(0, 10), consumer(5), producer(11, 20), consumer(15)],
            )

        def verify_total():
            self.assertEqual(len(results), 20)
