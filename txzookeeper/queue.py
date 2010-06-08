"""
A distributed multiprocess queue.

Implementation is based off the apache zookeeper Queue recipe. This
implementation also uses a cache of child nodes. Its not clear what
value the cache of children, as it effectively means the queue is
constantly watching the queue node, and constantly receiving updates.

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

import zookeeper

from twisted.internet.defer import Deferred, fail

from client import ZOO_OPEN_ACL_UNSAFE


class Queue(object):

    _prefix = "entry-"

    def __init__(self, path, client, acl=None, persistent=False):
        self._path = path
        self._client = client
        self._persistent = persistent
        if acl is None:
            acl = [ZOO_OPEN_ACL_UNSAFE]
        self._acl = acl

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
        # watcher for queue folder
        def on_queue_items_changed(*args):
            """Event watcher on queue node child events."""
            if request.complete or not self._client.connected:
                return

            if request.processing:
                request.refetch = True
            else:
                self._get(request)

        request = GetRequest(Deferred(), on_queue_items_changed)
        self._get(request)
        return request.deferred

    def put(self, item):
        """
        Put an item into the queue.
        """
        if not isinstance(item, str):
            return fail(ValueError("queue items must be strings"))

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

    def _get(self, request):
        d = self._client.get_children(self._path, request.watcher)
        d.addCallback(self._get_item, request)
        return d

    def _get_item(self, children, request):

        if not children:
            return

        request.processing = True

        def fetch_node(name):
            path = "/".join((self._path, name))
            d = self._client.get(path)
            d.addCallback(on_get_node_success)
            d.addErrback(on_no_node_error)
            return d

        def on_get_node_success((data, stat)):
            d = self._client.delete("/".join((self._path, name)))
            d.addCallback(on_delete_node_success, data)
            d.addErrback(on_no_node_error)
            return d

        def on_delete_node_success(result_code, data):
            request.processing = False
            request.callback(data)

        def on_no_node_error(failure):
            failure.trap(zookeeper.NoNodeException)
            if children:
                name = children.pop(0)
                return fetch_node(name)
            # Refetching deferred until we process all the children from
            # from a get children call.
            request.processing = False
            if request.refetch:
                return self._get(request)

        children.sort()
        name = children.pop(0)
        return fetch_node(name)


class GetRequest(object):
    """
    A request to fetch an item of the queue.
    """

    def __init__(self, deferred, watcher):
        self.deferred = deferred
        self.watcher = watcher
        self.processing = False
        self.refetch = False

    @property
    def complete(self):
        return self.deferred.called

    def callback(self, data):
        self.deferred.callback(data)

    def errback(self, error):
        self.deferred.errback(error)
