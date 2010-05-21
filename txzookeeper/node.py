from zookeeper import NoNodeException, NodeExistsException, BadVersionException
from twisted.internet.defer import Deferred


class ZNode(object):
    """
    A minimal object abstraction over a zookeeper node, utilizes no caching
    outside of trying to keep track of node existance and the last read
    version. It will attempt to utilize the last read version when modifying
    the node. On a bad version exception this values are cleared and the
    except reraised (errback chain continue.)
    """

    def __init__(self, path, context):
        self.path = path
        self.name = path.split("/")[-1]
        self._context = context
        self._node_exists = None
        self._node_stat = None

    def _get_version(self):
        if not self._node_stat:
            return -1
        return self._node_stat["version"]

    def _on_error_bad_version(self, failure):
        if isinstance(failure.value, BadVersionException):
            self._node_exists = None
            self._node_stat = None
        return failure # pragma: no cover

    def _on_exists_success(self, node_stat):
            if node_stat is None:
                return False
            self._node_exists = True
            self._node_stat = node_stat
            return True

    def exists(self):
        """
        Does the node exist or not, returns a boolean.
        """
        d = self._context.exists(self.path)

        def on_success(node_stat):
            if node_stat is None:
                return False
            self._node_exists = True
            self._node_stat = node_stat
            return True

        d.addCallback(on_success)
        return d

    def exists_with_watch(self):
        """
        Returns a boolean based on the node's existence. Also returns a
        deferred that fires when the node is modified/created/added/deleted.
        """
        node_changed = Deferred()

        def on_node_event(event, state, path):
            return node_changed.callback((event, state, path))

        d = self._context.exists(self.path, on_node_event)
        return d, node_changed

    def _on_get_node_error(self, failure):
        if isinstance(failure.value, NoNodeException):
            self._node_exists = False
            return
        return failure # pragma: no cover

    def _on_get_node_success(self, data):
        (node_data, node_stat) = data
        self._node_exists = True
        self._node_stat = node_stat
        return node_data

    def get_data(self):
        """
        Retrieve the node's data.
        """
        d = self._context.get(self.path)
        d.addCallback(self._on_get_node_success)
        d.addErrback(self._on_get_node_error)
        return d

    def get_data_with_watch(self):
        """
        Retrieve the node's data and a deferred that fires when this data
        changes.
        """
        node_changed = Deferred()

        def on_node_change(event, status, path):
            node_changed.callback((event, status, path))

        d = self._context.get(self.path, watcher=on_node_change)
        d.addCallback(self._on_get_node_success)
        d.addCallback(self._on_get_node_error)
        return d, node_changed

    def set_data(self, data):
        """
        Set the node's data.
        """

        def on_success(value):
            if not self._node_exists:
                self._node_exists = True
            return self

        if self._node_exists:
            # if the node is deleted while we have this cached state we should
            # covert the set to a create. XXX ??? Should we
            def on_error_node_nonexistant(failure):
                if isinstance(failure.value, NoNodeException):
                    return self._context.create(self.path, data)
                return failure # pragma: no cover

            d = self._context.set(self.path, data, self._get_version())
            d.addErrback(on_error_node_nonexistant)
            d.addErrback(self._on_error_bad_version)
            d.addCallback(on_success)
            return d

        d = self._context.create(self.path, data)

        def on_error_node_exists(failure):
            if isinstance(failure.value, NodeExistsException):
                return self._context.set(self.path, data, self._get_version())
            return failure # pragma: no cover

        d.addCallback(on_success)
        d.addErrback(on_error_node_exists)
        d.addErrback(self._on_error_bad_version)
        return d

    def get_acl(self):
        """
        Get the ACL for this node. An ACL is a list of access control
        entity dictionaries.
        """

        def on_success((acl, stat)):
            if stat is not None:
                self._node_exists = True
                self._node_stat = stat
            return acl
        d = self._context.get_acl(self.path)
        d.addCallback(on_success)
        return d

    def set_acl(self, acl):
        """
        Set the ACL for this node.
        """
        # oddly zookeeper will let you set an acl on a nonexistant node
        # this is our one caveat to no caching though management though.
        if self._node_exists is False:
            raise ValueError("node doesn't exist")
        d = self._context.set_acl(
            self.path, acl, self._get_version())
        d.addErrback(self._on_error_bad_version)
        return d

    def _on_get_children_filter_results(self, children, prefix):
        if prefix:
            children = [
                name for name in children if name.startswith(prefix)]
        return [
            self.__class__("/".join((self.path, name)), self._context)
            for name in children]

    def get_children(self, prefix=None):
        """
        Get the children of this node, as ZNode objects. Optionally
        a name prefix may be passed which the child node must abide.
        """
        d = self._context.get_children(self.path)
        d.addCallback(self._on_get_children_filter_results, prefix)
        return d

    def get_children_watch(self, prefix=None):
        """
        Get the children of this node, as ZNode objects, and also return
        a deferred that fires if a child is added or deleted. Optionally
        a name prefix may be passed which the child node must abide.
        """
        children_changed = Deferred()

        def on_child_added_removed(event, status, path):
            # path is the container not the child.
            children_changed.callback((event, status, path))

        d = self._context.get_children(
            self.path, watcher=on_child_added_removed)
        d.addCallback(self._on_get_children_filter_results, prefix)
        return d, children_changed

    def __cmp__(self, other):
        return cmp(self.path, other.path)
