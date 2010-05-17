from txzookeeper import ZookeeperClient
from txzookeeper.lock import Lock
from txzookeeper.tests import ZookeeperTestCase, utils
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList


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
        """authentication so the test always has access to clean up the
        zookeeper node tree. synchronous auth to avoid using deferred
        during setup."""
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
    def test_acquire_release(self):
        """
        """
        client = yield self.open_client()
        self.assertTrue(client.connected)
        path = yield client.create("/lock-test")
        exists = yield client.exists(path)
        self.assertTrue(exists)

        lock = Lock(path, client)
        self.assertTrue(client.connected)
        lock = yield lock.acquire()
        self.assertEqual(lock.locked, True)
        released = yield lock.release()
        self.assertEqual(released, True)

    @inlineCallbacks
    def test_multi_client_acquire_release(self):

        client = yield self.open_client()
        path = yield client.create("/parallel-lock-test")

        results = []
        defers = []
        for i in range(1):
            client = yield self.open_client()
            lock = Lock(path, client, i)
            d = lock.acquire()

            def release(lock):
                results.append(i)
                lock.release()

            d.addCallback(release)
            defers.append(d)

        for i in defers:
            yield i

        print results

        #     def create_acquire_lock(client):
        #         lock = Lock(path, client, i)
        #         return lock.acquire()

        #     def release_lock(lock):
        #         print "acquired", i
        #         self.assertTrue(lock.locked)
        #         results.append(i)
        #         return lock.release()

        #     def verify_released(value):
        #         print "released", i
        #         self.assertTrue(value)

        #     d.addCallback(create_acquire_lock)
        #     d.addCallback(release_lock)
        #     d.addCallback(verify_released)
        #     defers.append(d)

        # dlist = DeferredList(defers, fireOnOneErrback=1)
        # yield dlist
        # self.assertEqual(results, [0,1,2,3])

        #     d = self.open_client()
            
        #     def create_acquire_lock(client):
        #         lock = Lock(path, client, i)
        #         return lock.acquire()

        #     def release_lock(lock):
        #         print "acquired", i
        #         self.assertTrue(lock.locked)
        #         results.append(i)
        #         return lock.release()

        #     def verify_released(value):
        #         print "released", i
        #         self.assertTrue(value)

        #     d.addCallback(create_acquire_lock)
        #     d.addCallback(release_lock)
        #     d.addCallback(verify_released)
        #     defers.append(d)

        # dlist = DeferredList(defers, fireOnOneErrback=1)
        # yield dlist
        # self.assertEqual(results, [0,1,2,3])
