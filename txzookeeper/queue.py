"""

So there are some oddities to using zookeeper as a queue. If you have two queue
processors, trying to fetch items.


"""

from Queue import Empty
import zookeeper
from twisted.python.failure import Failure
from client import ZOO_OPEN_ACL_UNSAFE


class Queue(object):

    prefix = "entry-"

    def __init__(self, path, client, max_size=None, acl=ZOO_OPEN_ACL_UNSAFE):
        self.max_size = max_size
        self.path = path
        self.client = client
        self._acl = list(acl)
        self._cached_entries = []
        self._child_watch = False

    def _on_queue_items_changed(self, event, state, path):
        self._child_watch = False
        self._refill()

    def _refill(self):
        """
        refetch the queue children, setting a watch as needed, and invalidating
        any previous children entries queue
        """
        self._cached_entries = []

        if self._child_watch is False:
            d = self.client.get_children(
                self.path, self._on_queue_items_changed)
            self._child_watch = True
        else:
            d = self.client.get_children(self.path)

        def on_success(children):
            self._cached_entries.extend(children)
            self._cached_entries.sort()

        d.addCallback(on_success)
        return d

    def _get(self, name):
        d = self.client.get("/".join((self.path, name)))

        def on_success((data, stat)):
            return data

        def on_no_node(failure):
            # on invalid cache entry, refill directly from zk
            if isinstance(failure.value, zookeeper.NoNodeException):
                d = self._refill()

                def on_success(data):
                    if not self._cached_entries:
                        return Failure(Empty(self.path))
                    return self._get(self._cached_entries.pop(0))
                d.addCallback(on_success)
                return d

        d.addErrback(on_no_node)
        d.addCallback(on_success)
        return d

    def get_nowait(self):
        if not self._cached_entries:
            self._refill()
        if not self._cached_entries:
            raise Empty(self.path)

        name = self._cached_entries.pop(0)
        d = self._get(name)

        def on_no_node_error(failure):
            if isinstance(failure.value, zookeeper.NoNodeException):
                return self.get_nowait()
            return failure

        def on_success_remove(data):
            d = self.client.remove("/".join((self.path, name)))

            def on_success(data):
                return data

            d.addCallback(on_success)
            d.addErrback(on_no_node_error)
            return d

        d.addCallback(on_success_remove)
        d.addErrback(on_no_node_error)
        return d

    def put_nowait(self, item):
        d = self.client.create(
            "/".join((self.path, self.prefix)), item,
            self._acl, zookeeper.EPHEMERAL|zookeeper.SEQUENCE)
        return d
