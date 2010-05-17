from zookeeper import NoNodeException, NodeExistsException, BadVersionException


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
        return failure

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

    def get_data(self):
        """
        Retrieve the node's data.
        """
        d = self._context.get(self.path)

        def on_error(failure):
            if isinstance(failure.value, NoNodeException):
                self._node_exists = False
                return
            return failure

        def on_success((node_data, node_stat)):
            self._node_exists = True
            self._node_stat = node_stat
            return node_data

        d.addCallback(on_success)
        d.addErrback(on_error)
        return d

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
                return failure

            d = self._context.set(self.path, data, self._get_version())
            d.addErrback(on_error_node_nonexistant)
            d.addErrback(self._on_error_bad_version)
            d.addCallback(on_success)
            return d

        d = self._context.create(self.path, data)

        def on_error_node_exists(failure):
            if isinstance(failure.value, NodeExistsException):
                return self._context.set(self.path, data, self._get_version())
            return failure

        d.addCallback(on_success)
        d.addErrback(on_error_node_exists)
        d.addErrback(self._on_error_bad_version)
        return d

    def subscribe(self, notify):
        """
        Subscribe to events that change the node. This includes,
        modify, create, delete events.

        @param notify: function taking three arguments (event, state, path)
                       the function will be called back when an event happens
                       on the node.
        """
        return self._context.exists(self.path, watcher=notify)

    def subscribe_children(self, notify):
        """
        @param notify: function taking three
        """
        return self._context.get_children(self.path, watcher=notify)

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

    def get_children(self, prefix=None):
        """
        Get the children of this node, as ZNode objects. Optionally
        a name prefix may be passed which the child node must abide.
        """
        d = self._context.get_children(self.path)

        def on_success(children):
            if prefix:
                children = [
                    name for name in children if name.startswith(prefix)]
            return [
                self.__class__("/".join((self.path, name)), self._context)
                for name in children]
        d.addCallback(on_success)
        return d

    def __cmp__(self, other):
        return cmp(self.path, other.path)
