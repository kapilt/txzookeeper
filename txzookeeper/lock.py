import zookeeper
from twisted.internet.defer import Deferred


class Lock(object):
    """
    A distributed exclusive lock, based on the apache zookeeper recipe.

    http://hadoop.apache.org/zookeeper/docs/r3.3.0/recipes.html
    """

    prefix = "lock-"

    def __init__(self, path, client):
        self.path = path
        self.client = client
        self._candidate_path = None
        self._acquired = False
        self._acquire_deferred = None

    @property
    def locked(self):
        return self._acquired

    def acquire(self):
        """
        Acquire the lock.
        """
        if self._acquired:
            raise ValueError("Already holding the lock %s"%(self.path))

        if self._acquire_deferred:
            raise ValueError("Already attempting to acquire the lock")

        def on_candidate_create(candidate_path):
            self._candidate_path = candidate_path
            return self._acquire()

        # create our candidate node in the lock directory
        d = self.client.create(
            "/".join((self.path, self.prefix)),
            flags=zookeeper.EPHEMERAL|zookeeper.SEQUENCE)

        d.addCallback(on_candidate_create)
        return d

    def _acquire(self):
        """
        An implementation of the distributed lock zookeeper recipe.
        """
        d = self.client.get_children(self.path)

        def check_candidate_nodes(children):
            candidate_name = self._candidate_path[
                self._candidate_path.rfind('/')+1:]

            # check to see if our node is the best candidate
            children.sort()
            assert candidate_name in children
            index = children.index(candidate_name)

            # if yes, we have acquired the lock
            if index == 0:
                self._acquired = True
                if self._acquire_deferred:
                    acquire_deferred = self._acquire_deferred
                    self._acquire_deferred = None
                    return acquire_deferred.callback(self)
                return self

            # if no, we watch the nearest other candidate
            if self._acquire_deferred is None:
                self._acquire_deferred = Deferred()

            # and if it changes, we attempt to reacquire the lock
            self.client.exists(
                "/".join((self.path, children[index-1])),
                self._acquire_nearest_candidate_watch)

            return self._acquire_deferred

        d.addCallback(check_candidate_nodes)
        return d

    def _acquire_nearest_candidate_watch(self, event, state, path):
        print "fired"
        if self._acquire_deferred is None:
            # if we allow timeouts this might happen at least verify
            # that our instance state is sane.
            assert not self._candidate_path, "Watcher on bad acquiring lock."
            return
        print "reacquire"
        return self._acquire()

    def release(self, acquiring=False):
        """
        Release the lock. If acquiring is True, an in progress acquiring
        attempt will be halted.
        """

        if not self._acquired and not acquiring:
            raise ValueError("Not holding lock %s"%(self.path))

        d = self.client.delete(self._candidate_path)
        self._candidate_path = None

        def on_delete_success(value):
            self._acquired = False
            return True

        if self._acquire_deferred and acquiring:
            self._acquire_deferred.errback(
                ValueError("Lock acquire attempt released"))
            self._acquire_deferred = None

        d.addCallback(on_delete_success)
        return d
