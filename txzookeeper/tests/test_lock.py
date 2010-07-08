
from mocker import ANY
from twisted.internet.defer import (
    inlineCallbacks, returnValue, Deferred, succeed)

from txzookeeper import ZookeeperClient
from txzookeeper.lock import Lock, LockError
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
        self.assertEqual(lock.acquired, True)
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
        self.assertTrue(lock.acquired)
        yield lock.release()
        self.assertFalse(lock.acquired)
        yield lock.acquire()
        self.assertTrue(lock.acquired)
        yield lock.release()
        self.assertFalse(lock.acquired)

    @inlineCallbacks
    def test_error_on_double_acquire(self):
        """
        Attempting to acquire an already held lock, raises a Value Error.
        """
        client = yield self.open_client()
        path = yield client.create("/lock-test")
        lock = Lock(path, client)
        yield lock.acquire()
        self.assertEqual(lock.acquired, True)
        yield self.failUnlessFailure(lock.acquire(), LockError)

    @inlineCallbacks
    def test_error_on_acquire_acquiring(self):
        """
        Attempting to acquire the lock while an attempt is already in progress,
        raises a LockError.
        """
        client = yield self.open_client()
        path = yield client.create("/lock-test")
        lock = Lock(path, client)

        # setup the client to create the intended environment
        mock_client = self.mocker.patch(client)
        mock_client.create(ANY, flags=ANY)
        self.mocker.result(succeed("%s/%s"%(path, "lock-3")))

        mock_client.get_children("/lock-test")
        self.mocker.result(succeed(["lock-2", "lock-3"]))

        mock_client.exists("%s/%s"%(path, "lock-2"), ANY)
        self.mocker.result(succeed(True))
        self.mocker.replay()

        # now we attempt to acquire the lock, rigged above to not succeed
        lock.acquire()
        test_deferred = Deferred()

        # and next we schedule a lock attempt, which should fail as we're
        # still attempting to acquire the lock.
        def attempt_acquire():
            self.failUnlessFailure(lock.acquire(), LockError)
            # after we've verified the error handling, end the test
            test_deferred.callback(None)

        from twisted.internet import reactor
        reactor.callLater(0.1, attempt_acquire)

        yield test_deferred

    @inlineCallbacks
    def test_no_previous_owner_bypasses_watch(self):
        """
        Coverage test.  Internally the lock algorithm checks and sets a
        watch on the nearest candidate node. If the node has been removed
        between the time between the get_children and exists call, the we
        immediately reattempt to get the lock without waiting on the watch.
        """
        client = yield self.open_client()
        path = yield client.create("/lock-no-previous")

        # setup the client to create the intended environment
        mock_client = self.mocker.patch(client)
        mock_client.create(ANY, flags=ANY)
        self.mocker.result(succeed("%s/%s"%(path, "lock-3")))

        mock_client.get_children(path)
        self.mocker.result(succeed(["lock-2", "lock-3"]))

        mock_client.exists("%s/%s"%(path, "lock-2"), ANY)
        self.mocker.result(succeed(False))

        mock_client.get_children(path)
        self.mocker.result(succeed(["lock-3"]))
        self.mocker.replay()

        lock = Lock(path, mock_client)
        yield lock.acquire()
        self.assertTrue(lock.acquired)

    @inlineCallbacks
    def test_error_when_releasing_unacquired(self):
        """
        If an attempt is made to release a lock, that not currently being held,
        than a C{LockError} exception is raised.
        """
        client = yield self.open_client()
        lock_dir = yield client.create("/lock-multi-test")
        lock = Lock(lock_dir, client)
        self.failUnlessFailure(lock.release(), LockError)

    @inlineCallbacks
    def test_multiple_acquiring_clients(self):
        """
        Multiple clients can compete for the lock, only one client's Lock
        instance may hold the lock at any given moment.
        """
        client = yield self.open_client()
        client2 = yield self.open_client()
        lock_dir = yield client.create("/lock-multi-test")

        lock = Lock(lock_dir, client)
        lock2 = Lock(lock_dir, client2)

        yield lock.acquire()
        self.assertTrue(lock.acquired)
        lock2_acquire = lock2.acquire()
        yield lock.release()
        yield lock2_acquire
        self.assertTrue(lock2.acquired)
        self.assertFalse(lock.acquired)