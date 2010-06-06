import zookeeper
from twisted.internet.defer import Deferred


class Lock(object):
    """
    A distributed exclusive lock, based on the apache zookeeper recipe.

    http://hadoop.apache.org/zookeeper/docs/r3.3.0/recipes.html
    """

    prefix = "lock-"

    def __init__(self, path, client):
        self._path = path
        self._client = client
        self._candidate_path = None
        self._acquired = False

    @property
    def path(self):
        """Return the path to the lock."""
        return self._path

    @property
    def locked(self):
        """Has the lock been acquired."""
        return self._acquired

    def acquire(self):
        """Acquire the lock."""

        if self._acquired:
            raise ValueError("Already holding the lock %s"%(self.path))

        if self._candidate_path is not None:
            raise ValueError("Already attempting to acquire the lock")

        self._candidate_path = ""

        acquire_deferred = Deferred()

        # Create our candidate node in the lock directory.
        d = self._client.create(
            "/".join((self.path, self.prefix)),
            flags=zookeeper.EPHEMERAL|zookeeper.SEQUENCE)

        def on_candidate_create(path):
            self._candidate_path = path
            return self._acquire(acquire_deferred, nearest_watcher)

        d.addCallback(on_candidate_create)

        # Define our watcher as a closure with access to the acquire deferred.
        def nearest_watcher(*args):
            if not self._acquired:
                return self._acquire(acquire_deferred, nearest_watcher)

        return acquire_deferred

    def _acquire(self, acquire_deferred, watcher):
        d = self._client.get_children(self.path)
        d.addCallback(self._check_candidate_nodes, acquire_deferred, watcher)
        return d

    def _check_candidate_nodes(self, children, acquire_deferred, watcher):
        """
        Check if our lock attempt candidate path is the best candidate
        among the set of list of children names. If it is then fire the
        acquire deferred. If its not then use the given watcher on the
        nearest candidate name to our candidate.
        """
        candidate_name = self._candidate_path[
            self._candidate_path.rfind('/')+1:]

        # check to see if our node is the best candidate
        children.sort()
        assert candidate_name in children
        index = children.index(candidate_name)

        # if yes, we have acquired the lock
        if index == 0:
            self._acquired = True
            return acquire_deferred.callback(self)

        # and if it changes, we attempt to reacquire the lock
        return self._client.exists(
            "/".join((self.path, children[index-1])), watcher)

    def release(self):
        """
        Release the lock. If acquiring is True, an in progress acquiring
        attempt will be halted.
        """

        if not self._acquired:
            raise ValueError("Not holding lock %s"%(self.path))

        d = self._client.delete(self._candidate_path)

        def on_delete_success(value):
            self._acquired = False
            self._candidate_path = None
            return True

        d.addCallback(on_delete_success)
        return d
