"""

So there are some oddities to using zookeeper as a queue. If you have two queue
processors, trying to fetch items.


"""

from Queue import Empty

import zookeeper

from twisted.internet.defer import inlineCallbacks, returnValue, Deferred

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

    def _refill(self, wait=False):
        """
        Refetch the queue children, setting a watch as needed, and invalidating
        any previous children entries queue. If wait is True, then a deferred
        is return if no items are available after the refill, which chains to
        a watcher to trigger.
        """
        d = None

        child_available = Deferred()

        def on_queue_items_changed(*args):
            """Event watcher on queue node child events."""
            if not self.client.connected:
                return

            # refill cache
            self._cached_entries = []
            d = self._refill(wait=wait)
            if not wait:
                return

            # notify any waiting
            def notify_waiting(value):
                child_available.callback(None)
            d.addCallback(notify_waiting)

        d = self.client.get_children(
            self.path, on_queue_items_changed)

        def on_success(children):
            self._cached_entries = children
            self._cached_entries.sort()

            if not self._cached_entries:
                if wait:
                    return child_available

        def on_error(failure):
            # if no children than our queue has been destroyed.
            if isinstance(failure.value, zookeeper.NoNodeException):
                if not self.client.connected:
                    return
                raise zookeeper.NoNodeException(
                    "Queue non existant %s"%self.path)

        d.addCallback(on_success)
        d.addErrback(on_error)
        return d

    def _get_item(self, name, wait):
        """
        Fetch the node data in the queue directory for the given node name. If
        wait is
        """
        d = self.client.get("/".join((self.path, name)))

        def on_success(data):
            # on nested _get calls we get back just the data versus a result of
            # client.get.
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
                    return self._get_item(self._cached_entries.pop(), wait)

                # if the cache is empty, restart the get. Fairly rare.
                return self._get(wait) # pragma: no cover

            return failure

        d.addErrback(on_no_node)
        d.addCallback(on_success)
        return d

    def get(self):
        """
        Get and remove an item from the queue. If no item is available
        an Empty exception is raised.
        """
        return self._get()

    get_nowait = get

    def get_wait(self):
        """
        Get and remove an item from the queue. If no item is available
        at the moment, a deferred is return that will fire when an item
        is available.
        """
        return self._get(wait=True)

    @inlineCallbacks
    def _get(self, wait=False):
        """
        Get and remove an item from the queue. If no item is available
        an Empty exception is raised.
        """
        if not self._cached_entries:
            yield self._refill(wait=wait)

        if not wait and not self._cached_entries:
            raise Empty()

        name = self._cached_entries.pop(0)
        d = self._get_item(name, wait)

        def on_no_node_error(failure):
            if isinstance(failure.value, zookeeper.NoNodeException):
                return self._get(wait=wait)
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
