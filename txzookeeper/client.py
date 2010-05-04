import zookeeper
import sys
import thread

from twisted.internet import defer, reactor, task

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


class Wrapper(object):

    def __init__(self, target):
        self.target = target

    def __getattr__(self, name):
        sys.stdout.write("tx - access %s %s\n"%(name, thread.get_ident()))
        return getattr(self.target, name)

zookeeper = Wrapper(zookeeper)


class ZookeeperClient(object):
    """
    An asynchronous twisted client.
    """

    def __init__(self, servers):
        self.servers = servers
        self.connected = False
        self.handle = None
        self._async_calls = [] # outstanding async calls

    def _check_connected(self):
        if not self.connected:
            raise zookeeper.ConnectionLossException()

    def _check_result(self, result_code, callback=False):
        error = None
        if not result_code == zookeeper.OK:
            error_msg = zookeeper.zerror(result_code)
            error = zookeeper.ZooKeeperException(error_msg)
        if error is not None:
            if not callback:
                raise error
            return callback
        return None

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
        if self._async_calls:
            if force:
                raise zookeeper.ClosingException(
                    "Error on close, outstanding async calls")
            print "reschedule close"

            return task.deferLater(reactor, 0.1, self.close)
        self.connected = False
        result = zookeeper.close(self.handle)
        error = self._check_result(result, callback=True)
        if error:
            return defer.fail(error)
        return defer.succeed(result)

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
            print "tx - twised callback invoked connected"
            if state == zookeeper.CONNECTED_STATE:
                self.connected = True
                d.callback(value)
            else:
                d.errback(value)

        def _zk_cb_connected(handle, type, state, path):
            self._async_calls.remove(_zk_cb_connected)
            print "tx - zk callback invoked connected"
            reactor.callFromThread(_cb_connected, handle, type, state, path)

        # use a scheduled function to ensure a timeout
        def _check_timeout():
            d.errback()

        self._async_calls.append(_zk_cb_connected)

        self.handle = zookeeper.init(
            self.servers, _zk_cb_connected, DEFAULT_TIMEOUT)

        return d

    def create(self, path, data, acls=[ZOO_OPEN_ACL_UNSAFE], flags=0):
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
            print "tx - twisted callback invoked created"
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback(path)

        def _zk_cb_created(handle, result_code, path):
            reactor.callFromThread(_cb_created, result_code, path)
            self._async_calls.remove(_zk_cb_created)
            print "tx - callback finished created"

        self._async_calls.append(_zk_cb_created)
        result = zookeeper.acreate(
            self.handle, path, data, acls, flags, _zk_cb_created)
        self._check_result(result)
        return d

    def delete(self, path, version=-1):
        self._check_connected()
        d = defer.Deferred()

        def _cb_delete(result_code):
            print "tx - twisted callback invoked delete"
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback()

        def _zk_cb_delete(handle, result_code):
            reactor.callFromThread(_cb_delete, result_code)
            print "tx - callback finished delete"
            self._async_calls.remove(_zk_cb_delete)

        self._async_calls.append(_zk_cb_delete)
        result = zookeeper.adelete(self.handle, path, version, _zk_cb_delete)
        self._check_result(result)
        return d

    def exists(self, path, watcher=None):
        self._check_connected()
        d = defer.Deferred()

        def _cb_exists(result_code, stat):
            print "tx - twisted callback invoked exists"
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback(stat)

        def _zk_cb_exists(handle, result_code, stat):
            reactor.callFromThread(_cb_exists, result_code, stat)
            print "tx - zk callback finished exists"
            self._async_calls.remove(_zk_cb_exists)

        self._async_calls.append(_zk_cb_exists)
        result = zookeeper.aexists(self.handle, path, watcher, _zk_cb_exists)
        self._check_result(result)
        return d

    def get(self, path, watcher=None):
        self._check_connected()
        d = defer.Deferred()

        def _cb_get(result_code, value, stat):
            print "tx - twisted callback invoked get"
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback((value, stat))

        def _zk_cb_get(handle, result_code, value, stat):
            reactor.callFromThread(_cb_get, result_code, value, stat)
            print "tx - zk callback finished get"
            self._async_calls.remove(_zk_cb_get)

        self._async_calls.append(_zk_cb_get)
        result = zookeeper.aget(self.handle, path, watcher, _zk_cb_get)
        self._check_result(result)
        return d

    def get_children(self, path):
        pass

    def get_acl(self, path, acls):
        pass

    def set_acl(self, path, acls):
        pass

    def set(self, path, data="", version=-1):
        """
        Sets the data of a node

        @param path
        @param data
        """
        self._check_connected()
        d = defer.Deferred()

        def _cb_set(result_code):
            print "tx - twisted callback invoked set"
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback()

        def _zk_cb_set(handle, result_code):
            reactor.callFromThread(_cb_set, result_code)
            print "tx - zk callback finished set"
            self._async_calls.remove(_cb_set)

        self._async_calls.append(_zk_cb_set)
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
            print "tx - twisted callback invoked sync"
            error = self._check_result(result_code, True)
            if error:
                return d.errback(error)
            d.callback()

        def _zk_cb_sync(handle, result_code):
            reactor.callFromThread(_cb_sync, result_code)
            print "tx - zk callback finished sync"

        self._async_calls.append(_zk_cb_sync)
        result = zookeeper.async(path, _zk_cb_sync)
        self._check_result(result)
        return d
