import zookeeper
from twisted.internet.defer import Deferred


class Lock(object):

    prefix = "lock-"

    def __init__(self, path, client, extra=None):
        self.path = path
        self.extra = extra
        self.client = client
        assert self.client.connected
        self._candidate_path = None
        self._candidate_name = None
        self._acquired = False

    @property
    def locked(self):
        return self._acquired

    def acquire(self):
        """
        Acquire the lock
        """
        if self._acquired:
            raise ValueError("Already holding the lock %s"%(self.path))

        d = self.client.create(
            "/".join((self.path, self.prefix)),
            flags=zookeeper.EPHEMERAL|zookeeper.SEQUENCE)

        acquire_deferred = Deferred()

        # first we'll check for other children
        def on_candidate_create_success(path):
            print "on candiate create", path, self.client.connected, self.extra
            self._candidate_path = path
            self._candidate_name = path[path.rfind('/')+1:]
            print "checking children", self.extra, self.path
            print zookeeper.get_children(self.client.handle, self.path)
            return self.client.get_children(self.path)

        # if we have the lowest sequence node, lock acquired
        # else call exists with watch on the next lowest sequence node.
        def on_get_children_success(*args):
            children = args[0]
            print "get children", children
            children.sort()
            index = children.index(self._candidate_name)
            if index == 0:
                self._acquired = True
                print "acquired", self.extra
                return self
            print "checking exists", self.extra
            next_candidate = children[index-1]
            self.client.exists(next_candidate, watch_previous_candidate)

        def on_error(failure):
            import pdb; pdb.set_trace()
            print self.client.connected, self.path
            return self.client.get_children(self.path)
            return failure

        # if the next lowest seqeuence doesn't exist, we've acquired
        # the lock. Else we wait on the acquire deferred
        def on_exists_success(data):
            print "on exists success", data, self.extra
            if isinstance(data, bool):
                return data
            if data is None:
                self._acquired = True
                return True
            return acquire_deferred

        def retry_on_change(data):
            d = self.client.get_children(self._candidate_path)
            d.addCallback(on_get_children_success)
            d.addCallback(on_exists_success)
            return d

        def watch_previous_candidate(event, state, path):
            print "watch invoked", event, state, path, self.extra
            self._acquired = True
            return acquire_deferred.callback(self)

        d.addCallback(on_candidate_create_success)
        d.addCallback(on_get_children_success)
        d.addErrback(on_error)
        #d.addCallback(on_exists_success)
        return d

    def release(self):
        """
        Release the lock
        """
        if not self._acquired:
            raise ValueError("Not holding lock %s"%(self.path))

        d = self.client.delete(self._candidate_path)

        def on_delete_success(value):
            self._acquired = False
            return True

        d.addCallback(on_delete_success)
        return d
