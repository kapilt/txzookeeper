
import zookeeper

from twisted.internet.defer import inlineCallbacks, returnValue

from txzookeeper import ZookeeperClient
from txzookeeper.lock import Lock
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
    def test_acquire_release(self):
        """
        A lock can be acquired and released.
        """
        client = yield self.open_client()
        path = yield client.create("/lock-test")
        lock = Lock(path, client)
        yield lock.acquire()
        self.assertEqual(lock.locked, True)
        released = yield lock.release()
        self.assertEqual(released, True)

    @inlineCallbacks
    def test_lock_reuse(self):
        """
        A lock instance may be reused after an acquire/release cycle.
        """
        client = yield self.open_client()
        path = yield client.create("/lock-test")
        lock = Lock(path, client)
        yield lock.acquire()
        self.assertTrue(lock.locked)
        yield lock.release()
        self.assertFalse(lock.locked)
        yield lock.acquire()
        self.assertTrue(lock.locked)
        yield lock.release()
        self.assertFalse(lock.locked)

    @inlineCallbacks
    def test_error_on_double_acquire(self):
        """
        Attempting to acquire an already held lock, raises a Value Error.
        """
        client = yield self.open_client()
        path = yield client.create("/lock-test")
        lock = Lock(path, client)
        yield lock.acquire()
        self.assertEqual(lock.locked, True)
        self.assertRaises(ValueError, lock.acquire)

    @inlineCallbacks
    def test_error_on_acquire_while_acquiring(self):
        """
        If a lock instance is already attempting to acquire the lock,
        then trying to reacquire it is an error.
        """
        client = yield self.open_client()
        lock_dir = yield client.create("/lock-test")
        lock = Lock(lock_dir, client)

        # create a fake candidate so the lock can't acquire.
        yield client.create(
            "/lock-test/%s"%(lock.prefix),
            flags=zookeeper.EPHEMERAL|zookeeper.SEQUENCE)
        children = yield client.get_children("/lock-test")
        self.assertTrue(len(children))

        # this won't ever acquire because of the above node
        # but we need to give it a moment to attempt it.
        d = lock.acquire()
        from twisted.internet import reactor

        def verify_acquire_while_acquiring():
            self.assertRaises(ValueError, lock.acquire)
            # stop the attempt to acquire the lock, fires
            # an error on the lock acquire deferred.
            return lock.release(acquiring=True)

        reactor.callLater(0.1, verify_acquire_while_acquiring)

        self.failUnlessFailure(d, ValueError)
        yield d

    @inlineCallbacks
    def test_error_when_releasing_unacquired(self):
        """
        If an attempt is made to release a lock, that not currently being held,
        than exception is raised.
        """
        client = yield self.open_client()
        lock_dir = yield client.create("/lock-multi-test")
        lock = Lock(lock_dir, client)
        self.assertRaises(ValueError, lock.release)

    @inlineCallbacks
    def test_multiple_acquiring_clients(self):
        client = yield self.open_client()
        client2 = yield self.open_client()
        lock_dir = yield client.create("/lock-multi-test")

        lock = Lock(lock_dir, client)
        lock2 = Lock(lock_dir, client2)

        yield lock.acquire()
        self.assertTrue(lock.locked)

        lock2_acquire = lock2.acquire()

        from twisted.internet import reactor

        def release_lock():
            return lock.release()

        reactor.callLater(0.1, release_lock)

        yield lock2_acquire
        self.assertTrue(lock2.locked)
        self.assertFalse(lock.locked)
