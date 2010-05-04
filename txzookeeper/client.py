import zookeeper
import time

from twisted.internet import defer, reactor

# Session timeout, not a connect timeout
DEFAULT_TIMEOUT = 30000

# Default node acl
ZOO_OPEN_ACL_UNSAFE = {
    "perms": zookeeper.PERM_ALL,
    "scheme": "world",
    "id": "anyone"}


class Connected(object):

    def __init__(self, handle, type, state, path):
        self.handle = handle
        self.type = type
        self.state = state
        self.path = path


class ConnectionTimeout(zookeeper.ZooKeeperException):
    """
    An exception raised when the we can't connect to zookeeper within
    the user specified timeout period.
    """


class ZookeeperClient(object):
    """
    An asynchronous twisted client.
    """

    def __init__(self, servers):
        self.servers = servers
        self.connected = False
        self.handle = None

    def _check_connected(self):
        if not self.connected:
            raise zookeeper.ConnectionLossException()

    def _check_result(self, result_code, callback=False, extra_codes=()):
        error = None
        if not result_code == zookeeper.OK and not result_code in extra_codes:
            error_msg = zookeeper.zerror(result_code)
            error = zookeeper.ZooKeeperException(error_msg)
            if callback:
                return error
            raise error
        return None

    def _wrap_watcher(self, watcher):
        if watcher is None:
            return watcher
        if not callable(watcher):
            raise SyntaxError("Invalid Watcher")

        def wrapper(handle, type, state, path):
            reactor.callFromThread(watcher, type, path)
        return wrapper

    @property
    def recv_timeout(self):
        """
        What's the session timeout for this connection.
        """
        return zookeeper.recv_timeout(self.handle)

    @property
    def state(self):
        """
        What's the current state of this connection, result is an
        integer value mapping to zoookeeper module constants.
        """
        return zookeeper.state(self.handle)

    @property
    def client_id(self):
        """
        The connection's client id, useful when introspecting the server logs
        for specific client activity.
        """
        return zookeeper.client_id(self.handle)

    @property
    def unrecoverable(self):
        """
        Boolean value representing whether the current connection be recovered.
        """
        return bool(zookeeper.is_unrecoverable(self.handle))

    def auth(self, scheme, identity):
        """
        Adds an authentication identity to this connection. A connection
        can use multiple authentication identities at the same time, all
        are checked when verifying acls on a node.

        @param scheme: a string specifying a an authentication scheme
                       valid values include 'digest'.
        @param identity: a string
        """
        self._check_connected()

    def close(self, force=False):
        """
        Close the underlying socket connection and zookeeper server side
        session. Due to sensitiveies of the underlying libraries if there
        any outstanding async calls, we automatically reschedule the
        close till outstanding requests are completed. if the force
        parameter is passed we raise an exception if there are outstanding
        async requests.

        @param force: boolean, require the connection to be closed now or
                      an exception be raised.
        """
        if not self.connected:
            return

        time.sleep(0.1)
        result = zookeeper.close(self.handle)
        self.connected = False
        self._check_result(result)
        return result

    def connect(self, timeout=10):
        """
        Establish a connection to the given zookeeper server(s)
        @param timeout: How many seconds to wait on a connection to the
                        zookeeper servers.
        @returns A deferred that's fired when the connection is established.
        """
        d = defer.Deferred()

        def _cb_connected(handle, type, state, path):
            value = Connected(handle, type, state, path)
            if state == zookeeper.CONNECTED_STATE:
                self.connected = True
                d.callback(self)
            else:
                d.errback(value)

        def _zk_cb_connected(handle, type, state, path):
            reactor.callFromThread(_cb_connected, handle, type, state, path)

        # use a scheduled function to ensure a timeout
#        def _check_timeout():
#            if d.called:
#                return
#            d.errback(ConnectionTimeout())
#        reactor.callLater(timeout, _check_timeout)

        self.handle = zookeeper.init(
            self.servers, _zk_cb_connected, DEFAULT_TIMEOUT)

        return d

    def create(self, path, data="", acls=[ZOO_OPEN_ACL_UNSAFE], flags=0):
        """
        create a node

        @params path: The path to the node
        @params data: The node's content
        @params acls: A list of dictionaries specifying permissions.
        @params flags: Node creation flags (ephemeral, sequence, persistent)
        """
        self._check_connected()
        d = defer.Deferred()

        def _cb_created(result_code, path):
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback(path)

        def _zk_cb_created(handle, result_code, path):
            reactor.callFromThread(_cb_created, result_code, path)

        result = zookeeper.acreate(
            self.handle, path, data, acls, flags, _zk_cb_created)
        self._check_result(result)
        return d

    def delete(self, path, version=-1):
        """
        Delete the node at the given path. If the current node version on the
        server is more recent than that supplied by the client, a bad version
        exception wil be thrown. A version of -1 (default) specifies any
        version

        @param path: the path of the node to be deleted.
        @param version: the integer version of the node.
        """
        self._check_connected()
        d = defer.Deferred()

        def _cb_delete(result_code):
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback(result_code)

        def _zk_cb_delete(handle, result_code):
            reactor.callFromThread(_cb_delete, result_code)

        result = zookeeper.adelete(self.handle, path, version, _zk_cb_delete)
        self._check_result(result)
        return d

    def exists(self, path, watcher=None):
        """
        Check that the given node path exists. Returns a deferred, the deferred
        value If it does exist is the node stat information (created, modified,
        version, etc.). If it doesn't exist the deferred value is None.

        An optional watcher callable may be passed that will be called back
        when the node is modified or removed.
        """
        self._check_connected()
        d = defer.Deferred()

        def _cb_exists(result_code, stat):
            error = self._check_result(
                result_code, True, extra_codes=(zookeeper.NONODE,))
            if error:
                return d.errback(error)
            d.callback(stat)

        def _zk_cb_exists(handle, result_code, stat):
            reactor.callFromThread(_cb_exists, result_code, stat)

        watcher = self._wrap_watcher(watcher)

        result = zookeeper.aexists(self.handle, path, watcher, _zk_cb_exists)
        self._check_result(result)
        return d

    def get(self, path, watcher=None):
        """
        Get the node's data for the given node path. Returns a deferred. The
        deferred value is the content of the Node.

        An optional watcher callable may be passed that will be called back
        when the node is modified or removed.
        """

        self._check_connected()
        d = defer.Deferred()

        def _cb_get(result_code, value, stat):
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback((value, stat))

        def _zk_cb_get(handle, result_code, value, stat):
            reactor.callFromThread(_cb_get, result_code, value, stat)

        watcher = self._wrap_watcher(watcher)
        result = zookeeper.aget(self.handle, path, watcher, _zk_cb_get)
        self._check_result(result)
        return d

    def get_children(self, path, watcher=None):
        """
        Get the ids of all children directly under the given path.

        Optionally a watcher (callable) may be set on this path to be notified
        of changes.
        """
        self._check_connected()
        d = defer.Deferred()

        def _cb_get_children(result_code, *args):
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback(args)

        def _zk_cb_get_children(result_code, *args):
            reactor.callFromThread(_cb_get_children, result_code, *args)

        watcher = self._wrap_watcher(watcher)
        result = zookeeper.aget_children(
            self.handle, path, watcher, _zk_cb_get_children)
        self._check_result(result)
        return d

    def get_acl(self, path, acls):
        self._check_connected()

    def set_acl(self, path, acls):
        self._check_connected()

    def set(self, path, data="", version=-1):
        """
        Sets the data of a node at the given

        @param path: The path of the node whose data we will set.
        @param data: The data to store on the node.
        @param version: Integer version value
        """
        self._check_connected()
        d = defer.Deferred()

        def _cb_set(result_code, node_stat):
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback(node_stat)

        def _zk_cb_set(handle, result_code, node_stat):
            reactor.callFromThread(_cb_set, result_code, node_stat)

        result = zookeeper.aset(self.handle, path, data, version, _zk_cb_set)
        self._check_result(result)
        return d

    def set_watcher(self, watcher):
        """
        Sets a permanent global watcher.

        @param: watcher function
        """
        zookeeper.set_watcher(self.handle, watcher)

    def sync(self, path="/"):
        """
        Flushes the zookeeper leader

        @param path: The root path to flush, all child nodes are also flushedd.
        """
        self._check_connected()
        d = defer.Deferred()

        def _cb_sync(result_code):
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback(result_code)

        def _zk_cb_sync(handle, result_code):
            reactor.callFromThread(_cb_sync, result_code)

        result = zookeeper.async(path, _zk_cb_sync)
        self._check_result(result)
        return d
