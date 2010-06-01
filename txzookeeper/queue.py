"""
A distributed multiprocess queue.

There are some things to keep in mind when using zookeeper as a queue
compared to a dedicated messaging service. An error condition in a queue
processor must requeue the item, else its lost, as its removed from
zookeeper on retrieval in this implementation. This implementation more
closely mirrors the behavior and api of standard library Queue, ableit
with the caveat of only strings for queue items.

Todo:

 -implement Queue.join.
 -implement Sized queues.
 -message queue application as queue composite with dedicated processes.

 - one failing of the current implementation, is if the queue itself is
   transient, waiting gets won't ever be invoked if the queue is deleted.
"""

from Queue import Empty

import zookeeper

from twisted.internet.defer import inlineCallbacks, returnValue, Deferred

from client import ZOO_OPEN_ACL_UNSAFE


class Queue(object):

    _prefix = "entry-"

    def __init__(self, path, client, acl=None, persistent=False):
        self._path = path
        self._client = client
        self._persistent = persistent
        if acl is None:
            acl = [ZOO_OPEN_ACL_UNSAFE]
        self._acl = list(acl)
        self._cached_entries = []

    @property
    def path(self):
        """Path to the queue."""
        return self._path

    @property
    def persistent(self):
        """If the queue is persistent returns True."""
        return self._persistent

    def get(self):
        """
        Get and remove an item from the queue. If no item is available
        at the moment, a deferred is return that will fire when an item
        is available.
        """
        return self._get(wait=True)

    def get_nowait(self):
        """
        Get and remove an item from the queue. If no item is available
        an Empty exception is raised.
        """
        return self._get()

    def put(self, item):
        """
        Put an item into the queue.
        """
        if not isinstance(item, str):
            raise ValueError("queue items must be strings")

        flags = zookeeper.SEQUENCE
        if not self._persistent:
            flags = flags|zookeeper.EPHEMERAL

        d = self._client.create(
            "/".join((self._path, self._prefix)), item, self._acl, flags)
        return d

    def qsize(self):
        """
        Return the approximate size of the queue. This value is always
        effectively a snapshot. Returns a deferred returning an integer.
        """
        d = self._client.exists(self._path)

        def on_success(stat):
            return stat["numChildren"]

        d.addCallback(on_success)
        return d

    def _refill(self, wait=False):
        """
        Refetch the queue children, setting a watch as needed, and invalidating
        any previous children entries queue.

        If wait is True, then a deferred is return if no items are available
        after the refill, which chained to a watcher to trigger, and triggers
        when the children of the queue have changed. If there are no children
        available it will recurse.
        """
        d = None

        child_available = Deferred()

        def on_queue_items_changed(*args):
            """Event watcher on queue node child events."""
            if not self._client.connected:
                return

            # refill cache
            d = self._refill(wait=wait)
            if not wait:
                return

            # notify any waiting
            def notify_waiting(value):
                child_available.callback(None)
            d.addCallback(notify_waiting)

        d = self._client.get_children(
            self._path, on_queue_items_changed)

        def on_success(children):
            self._cached_entries = children
            self._cached_entries.sort()

            if not self._cached_entries:
                if wait:
                    return child_available

        def on_error(failure): # pragma: no cover
            # if no node error on get children than our queue has been
            # destroyed or was never created.
            if isinstance(failure.value, zookeeper.NoNodeException):
                if not self._client.connected:
                    return
                raise zookeeper.NoNodeException(
                    "Queue node doesn't exist %s"%self._path)
            return failure

        d.addCallback(on_success)
        d.addErrback(on_error)
        return d

    def _get_item(self, name, wait):
        """
        Fetch the node data in the queue directory for the given node name. The
        wait boolean argument is passed only to restart the get.
        """
        d = self._client.get("/".join((self._path, name)))

        def on_success(data):
            # on nested _get calls we get back just the data versus a result of
            # client.get.
            if not isinstance(data, tuple):
                return data
            return data[0]

        def on_no_node(failure):
            failure.trap(zookeeper.NoNodeException)
            # We process our entire node cache before attempting to refill.
            if self._cached_entries:
                return self._get_item(self._cached_entries.pop(), wait)

            # If the cache is empty, restart the get. Fairly rare.
            return self._get(wait) # pragma: no cover

        d.addErrback(on_no_node)
        d.addCallback(on_success)
        return d

    @inlineCallbacks
    def _get(self, wait=False):
        """
        Get and remove an item from the queue. If no item is available
        an Empty exception is raised. If wait is True, a deferred
        that fires only when an item has been retrieved is returned.
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
            d = self._client.delete("/".join((self._path, name)))

            def on_success(delete_result):
                return data

            d.addCallback(on_success)
            d.addErrback(on_no_node_error)
            return d

        d.addCallback(on_success_remove)
        d.addErrback(on_no_node_error)

        data = yield d
        returnValue(data)
