import zookeeper

from twisted.internet import defer, reactor

# Default session timeout
DEFAULT_SESSION_TIMEOUT = 10000

# Default node acl (public)
ZOO_OPEN_ACL_UNSAFE = {
    "perms": zookeeper.PERM_ALL,
    "scheme": "world",
    "id": "anyone"}

# Map result codes to exceptions classes.
ERROR_MAPPING = {
    zookeeper.APIERROR: zookeeper.ApiErrorException,
    zookeeper.AUTHFAILED: zookeeper.AuthFailedException,
    zookeeper.BADARGUMENTS: zookeeper.BadArgumentsException,
    zookeeper.BADVERSION: zookeeper.BadVersionException,
    zookeeper.CLOSING: zookeeper.ClosingException,
    zookeeper.CONNECTIONLOSS: zookeeper.ConnectionLossException,
    zookeeper.DATAINCONSISTENCY: zookeeper.DataInconsistencyException,
    zookeeper.INVALIDACL: zookeeper.InvalidACLException,
    zookeeper.INVALIDCALLBACK: zookeeper.InvalidCallbackException,
    zookeeper.INVALIDSTATE: zookeeper.InvalidStateException,
    zookeeper.MARSHALLINGERROR: zookeeper.MarshallingErrorException,
    zookeeper.NOAUTH: zookeeper.NoAuthException,
    zookeeper.NOCHILDRENFOREPHEMERALS: (
        zookeeper.NoChildrenForEphemeralsException),
    zookeeper.NONODE: zookeeper.NoNodeException,
    zookeeper.NODEEXISTS: zookeeper.NodeExistsException,
    zookeeper.NOTEMPTY: zookeeper.NotEmptyException,
    zookeeper.NOTHING: zookeeper.NothingException,
    zookeeper.OPERATIONTIMEOUT: zookeeper.OperationTimeoutException,
    zookeeper.RUNTIMEINCONSISTENCY: zookeeper.RuntimeInconsistencyException,
    zookeeper.SESSIONEXPIRED: zookeeper.SessionExpiredException,
    zookeeper.SESSIONMOVED: zookeeper.SessionMovedException,
    zookeeper.SYSTEMERROR: zookeeper.SystemErrorException,
    zookeeper.UNIMPLEMENTED: zookeeper.UnimplementedException}


class NotConnectedException(zookeeper.ZooKeeperException):
    """
    Raised if an attempt is made to use the client's api before the
    client has a connection to a zookeeper server.
    """


class ConnectionException(zookeeper.ZooKeeperException):
    """
    Raised if an error occurs during the client's connection attempt.
    """


class ConnectionTimeoutException(zookeeper.ZooKeeperException):
    """
    An exception raised when the we can't connect to zookeeper within
    the user specified timeout period.
    """


class ZookeeperClient(object):
    """
    An asynchronous twisted client for zookeeper.
    """

    def __init__(self, servers=None, session_timeout=None):
        self._servers = servers
        self._session_timeout = session_timeout
        self.connected = False
        self.handle = None

    def _check_connected(self):
        if not self.connected:
            raise NotConnectedException("not connected")

    def _check_result(self, result_code, callback=False, extra_codes=()):
        error = None
        if not result_code == zookeeper.OK and not result_code in extra_codes:
            error_msg = zookeeper.zerror(result_code)
            error_class = ERROR_MAPPING.get(
                result_code, zookeeper.ZooKeeperException)
            error = error_class(error_msg)
            if callback:
                return error
            raise error
        return None

    def _wrap_watcher(self, watcher):
        if watcher is None:
            return watcher
        if not callable(watcher):
            raise SyntaxError("invalid watcher")
        return self._zk_thread_callback(watcher)

    def _zk_thread_callback(self, func):
        """
        The client library invokes callbacks in a separate thread, we wrap
        any user defined callback so that they are called back in the main
        thread after, zookeeper calls the wrapper.
        """

        def wrapper(handle, *args): # pragma: no cover
            reactor.callFromThread(func, *args)
        return wrapper

    @property
    def servers(self):
        """
        Servers that we're connected to or None if the client is not connected
        """
        if self.connected:
            return self._servers

    @property
    def session_timeout(self):
        """
        What's the negotiated session timeout for this connection, in seconds.
        """
        if self.connected:
            return zookeeper.recv_timeout(self.handle)

    @property
    def state(self):
        """
        What's the current state of this connection, result is an
        integer value corresponding to zoookeeper module constants.
        """
        if self.connected:
            return zookeeper.state(self.handle)

    @property
    def client_id(self):
        """
        The connection's client id, useful when introspecting the server logs
        for specific client activity.
        """
        if self.handle is None:
            return None
        return zookeeper.client_id(self.handle)

    @property
    def unrecoverable(self):
        """
        Boolean value representing whether the current connection can be
        recovered.
        """
        return bool(zookeeper.is_unrecoverable(self.handle))

    def add_auth(self, scheme, identity):
        """
        Adds an authentication identity to this connection. A connection
        can use multiple authentication identities at the same time, all
        are checked when verifying acls on a node.

        @param scheme: a string specifying a an authentication scheme
                       valid values include 'digest'.
        @param identity: a string containingusername and password colon
                      separated for example 'mary:apples'
        """
        self._check_connected()
        d = defer.Deferred()

        def _cb_authenticated(result_code):
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback(self)

        callback = self._zk_thread_callback(_cb_authenticated)
        result = zookeeper.add_auth(self.handle, scheme, identity, callback)
        self._check_result(result)
        return d

    def close(self, force=False):
        """
        Close the underlying socket connection and zookeeper server side
        session.

        @param force: boolean, require the connection to be closed now or
                      an exception be raised.
        """
        if not self.connected:
            return

        result = zookeeper.close(self.handle)
        self.connected = False
        self._check_result(result)
        return result

    def connect(self, servers=None, timeout=10):
        """
        Establish a connection to the given zookeeper server(s).

        @param servers: A string specifying the servers and their ports to
                        connect to.
        @param timeout: How many seconds to wait on a connection to the
                        zookeeper servers.
        @returns A deferred that's fired when the connection is established.
        """
        d = defer.Deferred()

        if self.connected:
            raise zookeeper.ZooKeeperException("Already Connected")

        def _cb_connected(type, state, path):
            delayed.cancel()
            if state == zookeeper.CONNECTED_STATE:
                self.connected = True
                d.callback(self)
            else:
                d.errback(
                    ConnectionException("connection error", type, state, path))

        callback = self._zk_thread_callback(_cb_connected)

        # use a scheduled function to ensure a timeout
        def _check_timeout():
            d.errback(
                ConnectionTimeoutException("could not connect before timeout"))
        delayed = reactor.callLater(timeout, _check_timeout)

        if self._session_timeout is None:
            self._session_timeout = DEFAULT_SESSION_TIMEOUT

        if servers is not None:
            self._servers = servers

        self.handle = zookeeper.init(
            self._servers, callback, self._session_timeout)

        return d

    def create(self, path, data="", acls=[ZOO_OPEN_ACL_UNSAFE], flags=0):
        """
        Create a node with the given data and access control.

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

        callback = self._zk_thread_callback(_cb_created)
        result = zookeeper.acreate(
            self.handle, path, data, acls, flags, callback)
        self._check_result(result)
        return d

    def delete(self, path, version=-1):
        """
        Delete the node at the given path. If the current node version on the
        server is more recent than that supplied by the client, a bad version
        exception wil be thrown. A version of -1 (default) specifies any
        version.

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

        callback = self._zk_thread_callback(_cb_delete)
        result = zookeeper.adelete(self.handle, path, version, callback)
        self._check_result(result)
        return d

    def exists(self, path, watcher=None):
        """
        Check that the given node path exists. Returns a deferred that
        holds the node stat information if the node exists (created,
        modified, version, etc.), or ``None`` if it does not exist.

        Optionally a watcher (callable) may be set on this path to be
        notified of changes.
        """

        self._check_connected()
        d = defer.Deferred()

        def _cb_exists(result_code, stat):
            error = self._check_result(
                result_code, True, extra_codes=(zookeeper.NONODE,))
            if error:
                return d.errback(error)
            d.callback(stat)

        callback = self._zk_thread_callback(_cb_exists)
        watcher = self._wrap_watcher(watcher)
        result = zookeeper.aexists(self.handle, path, watcher, callback)
        self._check_result(result)
        return d

    def exists_and_watch(self, path):
        """
        Check that the given node path exists and set watch.

        In addition to the deferred method result, this method returns
        a deferred that is called back when the node is modified or
        removed (once).
        """

        d = defer.Deferred()
        def callback(*args):
            return d.callback(args)
        return self.exists(path, callback), d

    def get(self, path, watcher=None):
        """
        Get the node's data for the given node path. Returns a
        deferred that holds the content of the node.

        Optionally a watcher (callable) may be set on this path to be
        notified of changes.
        """

        self._check_connected()
        d = defer.Deferred()

        def _cb_get(result_code, value, stat):
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback((value, stat))

        callback = self._zk_thread_callback(_cb_get)
        watcher = self._wrap_watcher(watcher)
        result = zookeeper.aget(self.handle, path, watcher, callback)
        self._check_result(result)
        return d

    def get_and_watch(self, path):
        """
        Get the node's data for the given node path and set watch.

        In addition to the deferred method result, this method returns
        a deferred that is called back when the node is modified or
        removed (once).
        """

        d = defer.Deferred()
        def callback(*args):
            return d.callback(args)
        return self.get(path, callback), d

    def get_children(self, path, watcher=None):
        """
        Get the ids of all children directly under the given path.

        An optional watcher (callable) may be passed that will be
        called back when a change happens on the provided path (once).
        """
        self._check_connected()
        d = defer.Deferred()

        def _cb_get_children(result_code, children):
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback(children)

        callback = self._zk_thread_callback(_cb_get_children)
        watcher = self._wrap_watcher(watcher)
        result = zookeeper.aget_children(self.handle, path, watcher, callback)
        self._check_result(result)
        return d

    def get_children_and_watch(self, path):
        """
        Get the ids of all children directly under the given path.

        In addition to the deferred method result, this method returns
        a deferred that is called back when a change happens on the
        provided path (once).
        """

        d = defer.Deferred()
        def callback(*args):
            return d.callback(args)
        return self.get_children(path, callback), d

    def get_acl(self, path):
        """
        Get the list of acls that apply to node with the give path.

        Each acl is a dictionary containing keys/values for scheme, id,
        and perms.
        """
        self._check_connected()
        d = defer.Deferred()

        def _cb_get_acl(result_code, acls, stat):
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback((acls, stat))

        callback = self._zk_thread_callback(_cb_get_acl)
        result = zookeeper.aget_acl(self.handle, path, callback)
        self._check_result(result)
        return d

    def set_acl(self, path, acls, version=-1):
        """
        Set the list of acls on a node.

        Each acl is a dictionary containing keys/values for scheme, id,
        and perms. The value for id is username:hash_value The hash_value
        component is the base64 encoded sha1 hash of a username and
        password that's colon separated. For example

        >>> import hashlib, base64
        >>> digest = base64.b64encode(
        ...                 hashlib.new('sha1', 'mary:apples').digest()))
        >>> id = '%s:%s'%('mary', digest)
        >>> id
        'mary:9MTr9XuZvmudebp9aOo4DtXwyII='
        >>> acl = {'id':id, 'scheme':'digest', 'perms':zookeeper.PERM_ALL}

        @param path: The string path to the node.
        @param acls: A list of acl dictionaries.
        @param version: A version id of the node we're modifying, if this
                        doesn't match the version on the server, then a
                        BadVersionException is raised.
        """
        self._check_connected()
        d = defer.Deferred()

        def _cb_set_acl(result_code):
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback(result_code)

        callback = self._zk_thread_callback(_cb_set_acl)
        result = zookeeper.aset_acl(
            self.handle, path, version, acls, callback)
        self._check_result(result)
        return d

    def set(self, path, data="", version=-1):
        """
        Sets the data of a node at the given path. If the current node version
        on the server is more recent than that supplied by the client, a bad
        version exception wil be thrown. A version of -1 (default) specifies
        any version.

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

        callback = self._zk_thread_callback(_cb_set)
        result = zookeeper.aset(self.handle, path, data, version, callback)
        self._check_result(result)
        return d

    def set_connection_watch(self):
        """
        Sets a permanent global watcher on the connection. This will get
        notice of changes to the connection state.
        """
        d = defer.Deferred()
        watcher = self._wrap_watcher(d.callback)
        zookeeper.set_watcher(self.handle, watcher)
        return d

    def sync(self, path="/"):
        """
        Flushes the zookeeper connection to the leader.

        @param path: The root path to flush, all child nodes are also flushed.
        """
        self._check_connected()
        d = defer.Deferred()

        def _cb_sync(result_code, path):
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback(path)

        callback = self._zk_thread_callback(_cb_sync)
        result = zookeeper.async(self.handle, path, callback)
        self._check_result(result)
        return d
