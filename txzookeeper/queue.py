#
#  Copyright (C) 2010-2011 Canonical Ltd. All Rights Reserved
#
#  This file is part of txzookeeper.
#
#  Authors:
#   Kapil Thangavelu
#
#  txzookeeper is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  txzookeeper is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with txzookeeper.  If not, see <http://www.gnu.org/licenses/>.
#

"""
Several distributed multiprocess queue implementations.

The C{Queue} implementation follows closely the apache zookeeper recipe, it
provides no guarantees beyond isolation and concurrency of retrieval of items.

The C{ReliableQueue} implementation, provides isolation, and concurrency, as
well guarantees that if a consumer dies before processing an item, that item is
made available to another consumer.

The C{SerializedQueue} implementation provides for strict in order processing
of items within a queue.
"""

import zookeeper

from twisted.internet.defer import Deferred, fail
from twisted.python.failure import Failure
from txzookeeper.lock import Lock
from txzookeeper.client import ZOO_OPEN_ACL_UNSAFE


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

    prefix = "entry-"

    def __init__(self, path, client, acl=None, persistent=False):
        """
        @param client: A connected C{ZookeeperClient} instance.
        @param path: The path to the queue inthe zookeeper hierarchy.
        @param acl: An acl to be used for queue items.
        @param persistent: Boolean flag which denotes if items in the queue are
        persistent.
        """
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

        def on_queue_items_changed(*args):
            """Event watcher on queue node child events."""
            if request.complete or not self._client.connected:
                return  # pragma: no cover

            if request.processing_children:
                # If deferred stack is currently processing a set of children
                # defer refetching the children till its done.
                request.refetch_children = True
            else:
                # Else the item get request is just waiting for a watch,
                # restart the get.
                self._get(request)

        request = GetRequest(Deferred(), on_queue_items_changed)
        self._get(request)
        return request.deferred

    def put(self, item):
        """
        Put an item into the queue.

        @param item: String data to be put on the queue.
        """
        if not isinstance(item, str):
            return fail(ValueError("queue items must be strings"))

        flags = zookeeper.SEQUENCE
        if not self._persistent:
            flags = flags | zookeeper.EPHEMERAL

        d = self._client.create(
            "/".join((self._path, self.prefix)), item, self._acl, flags)
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
        request.processing_children = True
        d, w = self._client.get_children_and_watch(self._path)
        w.addCallback(request.child_watcher)
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
            request.processing_children = False
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
            request.processing_children = False
            if request.refetch_children:
                request.refetch_children = False
                return self._get(request)

        if not children:
            return on_no_node()

        children.sort()
        name = children.pop(0)
        return fetch_node(name)


class GetRequest(object):
    """
    An encapsulation of a consumer request to fetch an item from the queue.

    @refetch_children - boolean field, when true signals that children should
    be refetched after processing the current set of children.

    @child_watcher -The queue child/item watcher.

    @processing_children - Boolean flag, set to true when the last known
    children of the queue are being processed. If a watch fires while the
    children are being processed it sets the refetch_children flag to true
    instead of getting the children immediately.

    @deferred - The deferred representing retrieving an item from the queue.
    """

    def __init__(self, deferred, watcher):
        self.deferred = deferred
        self.child_watcher = watcher
        self.processing_children = False
        self.refetch_children = False

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

    An optional processed callback maybe passed to the constructor that will
    be invoked after the node has been processed.
    """

    def __init__(self, path, data, client, processed_callback=None):
        self._path = path
        self._data = data
        self._client = client
        self._processed_callback = processed_callback

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

        if self._processed_callback:
            d.addCallback(self._processed_callback, self.path)
        return d


class ReliableQueue(Queue):
    """
    A distributed queue. It varies from a C{Queue} in that it ensures any
    item consumed from the queue is explicitly ack'd by the consumer.
    If the consumer dies after retrieving an item before ack'ing the item.
    The item will be made available to another consumer. To encapsulate the
    acking behavior the queue item data is returned in a C{QueueItem} instance,
    with a delete method that will remove it from the queue after processing.

    Reliable queues may be persistent or transient. If the queue is durable,
    than any item added to the queue must be processed in order to be removed.
    If the queue is transient, then any jobs placed in the queue by a client
    are removed when the client is closed, regardless of whether the job
    has been processed or not.
    """

    def _item_processed_callback(self, result_code, item_path):
        return self._client.delete(item_path + "-processing")

    def _filter_children(self, children, suffix="-processing"):
        """
        Filter any children currently being processed, modified in place.
        """
        children.sort()
        for name in list(children):
            # remove any processing nodes and their associated queue item.
            if name.endswith(suffix):
                children.remove(name)
                item_name = name[:-len(suffix)]
                if item_name in children:
                    children.remove(item_name)

    def _get_item(self, children, request):

        def check_node(name):
            """Check the node still exists."""
            path = "/".join((self._path, name))
            d = self._client.exists(path)
            d.addCallback(on_node_exists, path)
            d.addErrback(on_reservation_failed)
            return d

        def on_node_exists(stat, path):
            """Reserve the node for consumer processing."""
            d = self._client.create(path + "-processing",
                                    flags=zookeeper.EPHEMERAL)
            d.addCallback(on_reservation_success, path)
            d.addErrback(on_reservation_failed)
            return d

        def on_reservation_success(processing_path, path):
            """Fetch the node data to return"""
            d = self._client.get(path)
            d.addCallback(on_get_node_success, path)
            d.addErrback(on_get_node_failed, path)
            return d

        def on_get_node_failed(failure, path):
            """If we can't fetch the node, delete the processing node."""
            d = self._client.delete(path + "-processing")

            # propogate unexpected errors appropriately
            if not failure.check(zookeeper.NoNodeException):
                d.addCallback(lambda x: request.errback(failure))
            else:
                d.addCallback(on_reservation_failed)
            return d

        def on_get_node_success((data, stat), path):
            """If we got the node, we're done."""
            request.processing_children = False
            request.callback(
                QueueItem(
                    path, data, self._client, self._item_processed_callback))

        def on_reservation_failed(failure=None):
            """If we can't get the node or reserve, continue processing
            the children."""
            if failure and not failure.check(
                zookeeper.NodeExistsException, zookeeper.NoNodeException):
                request.processing_children = True
                request.errback(failure)
                return

            if children:
                name = children.pop(0)
                return check_node(name)

            # If a watch fired while processing children, process it
            # after the children list is exhausted.
            request.processing_children = False
            if request.refetch_children:
                request.refetch_children = False
                return self._get(request)

        self._filter_children(children)

        if not children:
            return on_reservation_failed()

        name = children.pop(0)
        return check_node(name)


class SerializedQueue(Queue):
    """
    A serialized queue ensures even with multiple consumers items are retrieved
    and processed in the order they where placed in the queue.

    This implementation aggregates a reliable queue, with a lock to provide
    for serialized consumer access. The lock is released only when a queue item
    has been processed.
    """

    def __init__(self, path, client, acl=None, persistent=False):
        super(SerializedQueue, self).__init__(path, client, acl, persistent)
        self._lock = Lock("%s/%s" % (self.path, "_lock"), client)

    def _item_processed_callback(self, result_code, item_path):
        return self._lock.release()

    def _filter_children(self, children, suffix="-processing"):
        """
        Filter the lock from consideration as an item to be processed.
        """
        children.sort()
        for name in list(children):
            if name.startswith('_'):
                children.remove(name)

    def _on_lock_directory_does_not_exist(self, failure):
        """
        If the lock directory does not exist, go ahead and create it and
        attempt to acquire the lock.
        """
        failure.trap(zookeeper.NoNodeException)
        d = self._client.create(self._lock.path)
        d.addBoth(self._on_lock_created_or_exists)
        return d

    def _on_lock_created_or_exists(self, failure):
        """
        The lock node creation will either result in success or node exists
        error, if a concurrent client created the node first. In either case
        we proceed with attempting to acquire the lock.
        """
        if isinstance(failure, Failure):
            failure.trap(zookeeper.NodeExistsException)
        d = self._lock.acquire()
        return d

    def _on_lock_acquired(self, lock):
        """
        After the exclusive queue lock is acquired, we proceed with an attempt
        to fetch an item from the queue.
        """
        d = super(SerializedQueue, self).get()
        return d

    def get(self):
        """
        Get and remove an item from the queue. If no item is available
        at the moment, a deferred is return that will fire when an item
        is available.
        """
        d = self._lock.acquire()

        d.addErrback(self._on_lock_directory_does_not_exist)
        d.addCallback(self._on_lock_acquired)
        return d

    def _get_item(self, children, request):

        def fetch_node(name):
            path = "/".join((self._path, name))
            d = self._client.get(path)
            d.addCallback(on_node_retrieved, path)
            d.addErrback(on_reservation_failed)
            return d

        def on_node_retrieved((data, stat), path):
            request.processing_children = False
            request.callback(
                QueueItem(
                    path, data, self._client, self._item_processed_callback))

        def on_reservation_failed(failure=None):
            """If we can't get the node or reserve, continue processing
            the children."""
            if failure and not failure.check(
                zookeeper.NodeExistsException, zookeeper.NoNodeException):
                request.processing_children = True
                request.errback(failure)
                return

            if children:
                name = children.pop(0)
                return fetch_node(name)

            # If a watch fired while processing children, process it
            # after the children list is exhausted.
            request.processing_children = False
            if request.refetch_children:
                request.refetch_children = False
                return self._get(request)

        self._filter_children(children)

        if not children:
            return on_reservation_failed()

        name = children.pop(0)
        return fetch_node(name)
