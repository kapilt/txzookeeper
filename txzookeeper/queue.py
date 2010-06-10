"""
Several distributed multiprocess queue implementations.

 -implement Queue.join.
 -implement Sized queues.
 -message queue application as queue composite with dedicated processes.
"""

import zookeeper

from twisted.internet.defer import Deferred, fail

from client import ZOO_OPEN_ACL_UNSAFE


class Queue(object):
    """
    Implementation is based off the apache zookeeper Queue recipe.

    There are some things to keep in mind when using this queue implementation.
    Its primarily to enforce isolation and concurrent access, however it does
    not provide for reliable consumption.  An error condition in a queue
    consumer must requeue the item, else its lost, as its removed from
    zookeeper on retrieval in this implementation. This implementation more
    closely mirrors the behavior and api of the pythonstandard library Queue,
    or multiprocessing.Queue ableit with the caveat of only strings for queue
    items.
    """
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
                return # pragma: no cover

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
        request.processing = True
        d = self._client.get_children(self._path, request.watcher)
        d.addCallback(self._get_item, request)
        return d

    def _get_item(self, children, request):

        def fetch_node(name):
            path = "/".join((self._path, name))
            d = self._client.get(path)
            d.addCallback(on_get_node_success)
            d.addErrback(on_no_node)
            return d

        def on_get_node_success((data, stat)):
            d = self._client.delete("/".join((self._path, name)))
            d.addCallback(on_delete_node_success, data)
            d.addErrback(on_no_node)
            return d

        def on_delete_node_success(result_code, data):
            request.processing = False
            request.callback(data)

        def on_no_node(failure=None):
            if failure and not failure.check(zookeeper.NoNodeException):
                request.errback(failure)
                return
            if children:
                name = children.pop(0)
                return fetch_node(name)

            # Refetching deferred until we process all the children from
            # from a get children call.
            request.processing = False
            if request.refetch:
                request.refetch = False
                return self._get(request)

        if not children:
            return on_no_node()

        children.sort()
        name = children.pop(0)
        return fetch_node(name)


class GetRequest(object):
    """
    An encapsulation of a consumer request to fetch an item from the queue.

    @refetch - boolean field, when true signals that children should be
               refetched after processing the current set of children.

    @watcher -The child watcher.

    @processing - When the last known children of the queue are being processed

    @deferred - The deferred representing retrieving an item from the queue.
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


class QueueItem(object):
    """
    An encapsulation of a work item put into a queue. The work item data is
    accessible via the data attribute. When the item has been processed by
    the consumer, the delete method can be invoked to remove the item
    permanently from the queue.
    """

    def __init__(self, path, data, client):
        self._path = path
        self._data = data
        self._client = client

    @property
    def data(self):
        return self._data

    @property
    def path(self):
        return self._path

    def delete(self):
        """
        Delete the item node and the item processing node in the queue.
        Typically invoked by a queue consumer, to signal succesful processing
        of the queue item.
        """
        d = self._client.delete(self.path)
        d.addCallback(
            lambda result_code: self._client.delete(self.path+"-processing"))
        return d


class ReliableQueue(Queue):
    """
    A distributed queue. It varies from a C{Queue} in that it ensures any
    item consumed from the queue is explicitly ack'd by the consumer.
    If the consumer dies after retrieving an item before ack'ing the item.
    The item will be made available to another consumer. To encapsulate the
    acking behavior the queue item data is returned in a C{QueueItem} instance,
    with a delete method that will remove it from the queue after processing.
    """

    def _filter_children(self, children, suffix="-processing"):
        """
        Filter any children currently being processed, modified in place.
        """
        children.sort()
        for name in list(children):
            if name.endswith(suffix):
                children.remove(name)
                item_name = name[:-len(suffix)]
                if item_name in children:
                    children.remove(item_name)

    def _get_item(self, children, request):

        def fetch_node(name):
            path = "/".join((self._path, name))
            d = self._client.get(path)
            d.addCallback(on_get_node_success, path)
            d.addErrback(on_reservation_failed)
            return d

        def on_get_node_success((data, stat), path):
            d = self._client.create(path+"-processing",
                                    flags=zookeeper.EPHEMERAL)
            d.addCallback(on_reservation_success, path, data)
            d.addErrback(on_reservation_failed)
            return d

        def on_reservation_success(processing_path, path, data):
            request.processing = False
            request.callback(QueueItem(path, data, self._client))

        def on_reservation_failed(failure=None):
            if failure and not failure.check(
                zookeeper.NodeExistsException, zookeeper.NoNodeException):
                request.errback(failure)
                return

            if children:
                name = children.pop(0)
                return fetch_node(name)

            # If a watch fired while processing children, process it
            # after the children list is exhausted.
            request.processing = False
            if request.refetch:
                request.refetch = False
                return self._get(request)

        self._filter_children(children)

        if not children:
            return on_reservation_failed()

        name = children.pop(0)
        return fetch_node(name)


class SerializedQueue(ReliableQueue):
    """
    A serialized queue ensures even with multiple consumers items are retrieved
    and processed in the order they where placed in the queue. (TODO) An
    implementation with less contention between consumers might instead utilize
    a reliable queue with a lock.
    """

    def _filter_children(self, children, suffix="-processing"):
        """
        Filter any children currently being processed, modified in place.
        """
        children.sort()
        for name in list(children):
            if name.endswith(suffix):
                children[:] = []
                break
