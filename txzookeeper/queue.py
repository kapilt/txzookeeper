"""

So there are some oddities to using zookeeper as a queue. If you have two queue
processors, trying to fetch items.


"""

from Queue import Empty

import zookeeper

from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from twisted.python.failure import Failure

from client import ZOO_OPEN_ACL_UNSAFE


class Queue(object):

    prefix = "entry-"

    def __init__(
        self, path, client, acl=[ZOO_OPEN_ACL_UNSAFE], persistent=False):

        self.path = path
        self.client = client
        self.persistent = persistent
        self._acl = list(acl)
        self._cached_entries = []
        self._child_watch = False

    def _refill(self):
        """
        Refetch the queue children, setting a watch as needed, and invalidating
        any previous children entries queue.
        """
        d = None

        def on_queue_items_changed(event, state, path):
            """Event watcher on queue node child events."""
            self._child_watch = False
            self._cached_entries = []
            if self.client.connected:
                self._refill()

        d = self.client.get_children(
            self.path, on_queue_items_changed)

        self._child_watch = True

        def on_success(children):
            self._cached_entries.extend(children)
            self._cached_entries.sort()

        d.addCallback(on_success)
        return d

    def _get(self, name):
        d = self.client.get("/".join((self.path, name)))

        def on_success(data):
            # on nested _get calls we get back just the data
            # versus as a result of client.get.
            if not isinstance(data, tuple):
                return data
            return data[0]

        def on_no_node(failure):
            if isinstance(failure.value, zookeeper.NoNodeException):
                # kapil -> this isn't just an optimization. If we attempt to
                # refill directly zookeeper client will toss either Interupted
                # system call or No such process exceptions, at least in looped
                # tests. Instead we process our entire our node cache before
                # proceeding.
                if self._cached_entries:
                    return self._get(self._cached_entries.pop())

                # Rare in practice, but if the cache is empty, restart the get
                return self.get() # pragma: no cover

            return failure

        d.addErrback(on_no_node)
        d.addCallback(on_success)
        return d

    @inlineCallbacks
    def get(self):
        """
        Get and remove an item from the queue. If no item is available
        an Empty exception is raised.
        """
        if not self._cached_entries:
            yield self._refill()

        if not self._cached_entries:
            raise Empty()

        name = self._cached_entries.pop(0)
        d = self._get(name)

        def on_no_node_error(failure):
            if isinstance(failure.value, zookeeper.NoNodeException):
                return self.get()
            return failure

        def on_success_remove(data):
            d = self.client.delete("/".join((self.path, name)))

            def on_success(delete_result):
                return data

            d.addCallback(on_success)
            d.addErrback(on_no_node_error)
            return d

        d.addCallback(on_success_remove)
        d.addErrback(on_no_node_error)

        data = yield d
        returnValue(data)

    def put(self, item):
        """
        Put an item into the queue.
        """
        if not isinstance(item, str):
            raise ValueError("queue items must be strings")

        flags = zookeeper.SEQUENCE
        if not self.persistent:
            flags = flags|zookeeper.EPHEMERAL

        d = self.client.create(
            "/".join((self.path, self.prefix)), item, self._acl, flags)
        return d
