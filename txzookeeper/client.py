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

from collections import namedtuple
from functools import partial

from twisted.internet import defer, reactor

import zookeeper

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

# Mapping of connection state values to human strings.
STATE_NAME_MAPPING = {
    zookeeper.ASSOCIATING_STATE: "associating",
    zookeeper.AUTH_FAILED_STATE: "auth-failed",
    zookeeper.CONNECTED_STATE: "connected",
    zookeeper.CONNECTING_STATE: "connecting",
    zookeeper.EXPIRED_SESSION_STATE: "expired",
    None: "unknown",
}

# Mapping of event type to human string.
TYPE_NAME_MAPPING = {
    zookeeper.NOTWATCHING_EVENT: "not-watching",
    zookeeper.SESSION_EVENT: "session",
    zookeeper.CREATED_EVENT: "created",
    zookeeper.DELETED_EVENT: "deleted",
    zookeeper.CHANGED_EVENT: "changed",
    zookeeper.CHILD_EVENT: "child",
    None: "unknown",
    }


class NotConnectedException(zookeeper.ZooKeeperException):
    """
    Raised if an attempt is made to use the client's api before the
    client has a connection to a zookeeper server.
    """


class ConnectionException(zookeeper.ZooKeeperException):
    """
    Raised if an error occurs during the client's connection attempt.
    """

    @property
    def state_name(self):
        return STATE_NAME_MAPPING[self.args[2]]

    @property
    def type_name(self):
        return TYPE_NAME_MAPPING[self.args[1]]

    @property
    def handle(self):
        return self.args[3]

    def __str__(self):
        return (
            "<txzookeeper.ConnectionException handle: %s type: %s state: %s>"
            % (self.handle, self.type_name, self.state_name))


def is_connection_exception(e):
    """
    For connection errors in response to api calls, a utility method
    to determine if the cause is a connection exception.
    """
    return isinstance(e,
                      (zookeeper.ClosingException,
                       zookeeper.ConnectionLossException,
                       zookeeper.SessionExpiredException))


class ConnectionTimeoutException(zookeeper.ZooKeeperException):
    """
    An exception raised when the we can't connect to zookeeper within
    the user specified timeout period.
    """


class ClientEvent(namedtuple("ClientEvent", 'type, connection_state, path')):
    """
    A client event is returned when a watch deferred fires. It denotes
    some event on the zookeeper client that the watch was requested on.
    """

    @property
    def type_name(self):
        return TYPE_NAME_MAPPING[self.type]

    @property
    def state_name(self):
        return STATE_NAME_MAPPING[self.connection_state]

    def __repr__(self):
        return  "<ClientEvent %s at %r state: %s>" % (
            self.type_name, self.path, self.state_name)


class ZookeeperClient(object):
    """Asynchronous twisted client for zookeeper."""

    def __init__(self, servers=None, session_timeout=None):
        """
        @param servers: A string specifying the servers and their
                        ports to connect to. Multiple servers can be
                        specified in comma separated fashion. if they are,
                        then the client will automatically rotate
                        among them if a server connection fails. Optionally
                        a chroot can be specified. A full server spec looks
                        like host:port/chroot_path

        @param session_timeout: The client's zookeeper session timeout can be
                       hinted. The actual value is negotiated between the
                       client and server based on their respective
                       configurations.
        """
        self._servers = servers
        self._session_timeout = session_timeout
        self._session_event_callback = None
        self._connection_error_callback = None
        self.connected = False
        self.handle = None

    def __repr__(self):
        if not self.client_id:
            session_id = ""
        else:
            session_id = self.client_id[0]

        return  "<txZookeeperClient session: %s handle: %r state: %s>" % (
            session_id, self.handle, self.state)

    def _check_connected(self, d):
        if not self.connected:
            d.errback(NotConnectedException("not connected"))
            return d

    def _check_result(self, result_code, deferred, extra_codes=()):
        """Check an API call or result for errors.

        :param result_code: The api result code.
        :param deferred: The deferred returned the client api consumer.
        :param extra_codes: Additional result codes accepted as valid/ok.

        If the result code is an error, an appropriate Exception class
        is constructed and the errback on the deferred is invoked with it.
        """
        error = None
        if not result_code == zookeeper.OK and not result_code in extra_codes:
            error_msg = zookeeper.zerror(result_code)
            error_class = ERROR_MAPPING.get(
                result_code, zookeeper.ZooKeeperException)
            error = error_class(error_msg)

            if is_connection_exception(error):
                # Route connection errors to a connection level error
                # handler if specified.
                if self._connection_error_callback:
                    # The result of the connection error handler is returned
                    # to the api invoker.
                    d = defer.maybeDeferred(
                        self._connection_error_callback,
                        self, error)
                    d.chainDeferred(deferred)
                    return True

            deferred.errback(error)
            return True
        return None

    def _get(self, path, watcher):
        d = defer.Deferred()
        if self._check_connected(d):
            return d

        def _cb_get(result_code, value, stat):
            if self._check_result(result_code, d):
                return
            d.callback((value, stat))

        callback = self._zk_thread_callback(_cb_get)
        watcher = self._wrap_watcher(watcher, "get", path)
        result = zookeeper.aget(self.handle, path, watcher, callback)
        self._check_result(result, d)
        return d

    def _get_children(self, path, watcher):
        d = defer.Deferred()
        if self._check_connected(d):
            return d

        def _cb_get_children(result_code, children):
            if self._check_result(result_code, d):
                return
            d.callback(children)

        callback = self._zk_thread_callback(_cb_get_children)
        watcher = self._wrap_watcher(watcher, "child", path)
        result = zookeeper.aget_children(self.handle, path, watcher, callback)
        self._check_result(result, d)
        return d

    def _exists(self, path, watcher):
        d = defer.Deferred()
        if self._check_connected(d):
            return d

        def _cb_exists(result_code, stat):
            if self._check_result(
                result_code, d, extra_codes=(zookeeper.NONODE,)):
                return
            d.callback(stat)

        callback = self._zk_thread_callback(_cb_exists)
        watcher = self._wrap_watcher(watcher, "exists", path)
        result = zookeeper.aexists(self.handle, path, watcher, callback)
        self._check_result(result, d)
        return d

    def _wrap_watcher(self, watcher, watch_type, path):
        if watcher is None:
            return watcher
        if not callable(watcher):
            raise SyntaxError("invalid watcher")
        return self._zk_thread_callback(
            partial(self._session_event_wrapper, watcher))

    def _session_event_wrapper(self, watcher, event_type, conn_state, path):
        """Watch wrapper that diverts session events to a connection callback.
        """
        # If it's a session event pass it to the session callback, else
        # ignore it. Session events are sent repeatedly to watchers
        # which we have modeled after deferred, which only accept a
        # single return value.
        if event_type == zookeeper.SESSION_EVENT:
            if self._session_event_callback:
                self._session_event_callback(
                    self, ClientEvent(event_type, conn_state, path))
            # We do propagate to watch deferreds, in one case in
            # particular, namely if the session is expired, in which
            # case the watches are dead, and we send an appropriate
            # error.
            if conn_state == zookeeper.EXPIRED_SESSION_STATE:
                error = zookeeper.SessionExpiredException("Session expired")
                return watcher(None, None, None, error=error)
        else:
            return watcher(event_type, conn_state, path)

    def _zk_thread_callback(self, func, *f_args, **f_kw):
        """
        The client library invokes callbacks in a separate thread, we wrap
        any user defined callback so that they are called back in the main
        thread after, zookeeper calls the wrapper.
        """
        f_args = list(f_args)

        def wrapper(handle, *args):  # pragma: no cover
            # make a copy, the conn watch callback gets invoked multiple times
            cb_args = list(f_args)
            cb_args.extend(args)
            reactor.callFromThread(func, *cb_args, **f_kw)
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
        The negotiated session timeout for this connection, in milliseconds.

        If the client is not connected the value is None.
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
        """Returns the client id that identifies the server side session.

        A client id is a tuple represented by the session id and
        session password. It can be used to manually connect to an
        extant server session (which contains associated ephemeral
        nodes and watches)/ The connection's client id is also useful
        when introspecting the server logs for specific client
        activity.
        """
        if self.handle is None:
            return None
        try:
            return zookeeper.client_id(self.handle)
        # Invalid handle
        except zookeeper.ZooKeeperException:
            return None

    @property
    def unrecoverable(self):
        """
        Boolean value representing whether the current connection can be
        recovered.
        """
        return bool(zookeeper.is_unrecoverable(self.handle))

    def add_auth(self, scheme, identity):
        """Adds an authentication identity to this connection.

        A connection can use multiple authentication identities at the
        same time, all are checked when verifying acls on a node.

        @param scheme: a string specifying a an authentication scheme
                       valid values include 'digest'.
        @param identity: a string containing username and password colon
                      separated, for example 'mary:apples'
        """
        d = defer.Deferred()
        if self._check_connected(d):
            return d

        def _cb_authenticated(result_code):
            if self._check_result(result_code, d):
                return
            d.callback(self)

        callback = self._zk_thread_callback(_cb_authenticated)
        result = zookeeper.add_auth(self.handle, scheme, identity, callback)
        self._check_result(result, d)
        return d

    def close(self, force=False):
        """
        Close the underlying socket connection and server side session.

        @param force: boolean, require the connection to be closed now or
                      an exception be raised.
        """
        self.connected = False

        if not self.handle:
            return

        try:
            result = zookeeper.close(self.handle)
        except zookeeper.ZooKeeperException:
            self.handle = None
            return

        d = defer.Deferred()
        if self._check_result(result, d):
            return d
        self.handle = None
        d.callback(True)
        return d

    def connect(self, servers=None, timeout=10, client_id=None):
        """
        Establish a connection to the given zookeeper server(s).

        @param servers: A string specifying the servers and their ports to
                        connect to. Multiple servers can be specified in
                        comma separated fashion.
        @param timeout: How many seconds to wait on a connection to the
                        zookeeper servers.

        @param session_id:
        @returns A deferred that's fired when the connection is established.
        """
        d = defer.Deferred()

        if self.connected:
            return defer.fail(
                zookeeper.ZooKeeperException("Already Connected"))

        # Use a scheduled function to ensure a timeout.
        def _check_timeout():
            d.errback(
                ConnectionTimeoutException("could not connect before timeout"))

        scheduled_timeout = reactor.callLater(timeout, _check_timeout)

        # Assemble an on connect callback with closure variable access.
        callback = partial(self._cb_connected, scheduled_timeout, d)
        callback = self._zk_thread_callback(callback)

        if self._session_timeout is None:
            self._session_timeout = DEFAULT_SESSION_TIMEOUT

        if servers is not None:
            self._servers = servers

        # Use client id if specified.
        if client_id:
            self.handle = zookeeper.init(
                self._servers, callback, self._session_timeout, client_id)
        else:
            self.handle = zookeeper.init(
                self._servers, callback, self._session_timeout)
        return d

    def _cb_connected(
        self, scheduled_timeout, connect_deferred, type, state, path):
        """This callback is invoked through the lifecycle of the connection.

        It's used for all connection level events and session events.
        """
        # Cancel the timeout delayed task if it hasn't fired.
        if scheduled_timeout.active():
            scheduled_timeout.cancel()

        # Update connected boolean
        if state == zookeeper.CONNECTED_STATE:
            self.connected = True
        elif state != zookeeper.CONNECTING_STATE:
            self.connected = False

        if connect_deferred.called:
            # If we timed out and then connected, then close the conn.
            if state == zookeeper.CONNECTED_STATE and scheduled_timeout.called:
                self.close()
                return

            # Send session events to the callback, in addition to any
            # duplicate session events that will be sent for extant watches.
            if self._session_event_callback:
                self._session_event_callback(
                    self, ClientEvent(type, state, path))

            return
        # Connected successfully, or If we're expired on an initial
        # connect, someone else expired us.
        elif state in (zookeeper.CONNECTED_STATE,
                       zookeeper.EXPIRED_SESSION_STATE):
            connect_deferred.callback(self)
            return

        connect_deferred.errback(
            ConnectionException("connection error", type, state, path))

    def create(self, path, data="", acls=[ZOO_OPEN_ACL_UNSAFE], flags=0):
        """
        Create a node with the given data and access control.

        @params path: The path to the node
        @params data: The node's content
        @params acls: A list of dictionaries specifying permissions.
        @params flags: Node creation flags (ephemeral, sequence, persistent)
        """
        d = defer.Deferred()
        if self._check_connected(d):
            return d

        callback = self._zk_thread_callback(
            self._cb_created, d, data, acls, flags)
        result = zookeeper.acreate(
            self.handle, path, data, acls, flags, callback)
        self._check_result(result, d)
        return d

    def _cb_created(self, d, data, acls, flags, result_code, path):
        if self._check_result(result_code, d):
            return
        d.callback(path)

    def delete(self, path, version=-1):
        """
        Delete the node at the given path. If the current node version on the
        server is more recent than that supplied by the client, a bad version
        exception wil be thrown. A version of -1 (default) specifies any
        version.

        @param path: the path of the node to be deleted.
        @param version: the integer version of the node.
        """
        d = defer.Deferred()
        if self._check_connected(d):
            return d

        callback = self._zk_thread_callback(self._cb_deleted, d, path)
        result = zookeeper.adelete(self.handle, path, version, callback)
        self._check_result(result, d)
        return d

    def _cb_deleted(self, d, path, result_code):
        if self._check_result(result_code, d):
            return
        d.callback(result_code)

    def exists(self, path):
        """
        Check that the given node path exists. Returns a deferred that
        holds the node stat information if the node exists (created,
        modified, version, etc.), or ``None`` if it does not exist.

        @param path: The path of the node whose existence will be checked.
        """
        return self._exists(path, None)

    def exists_and_watch(self, path):
        """
        Check that the given node path exists and set watch.

        In addition to the deferred method result, this method returns
        a deferred that is called back when the node is modified or
        removed (once).

        @param path: The path of the node whose existence will be checked.
        """
        d = defer.Deferred()

        def watcher(event_type, conn_state, path, error=None):
            if error:
                d.errback(error)
            else:
                d.callback(ClientEvent(event_type, conn_state, path))
        return self._exists(path, watcher), d

    def get(self, path):
        """
        Get the node's data for the given node path. Returns a
        deferred that holds the content of the node.

        @param path: The path of the node whose content will be retrieved.
        """
        return self._get(path, None)

    def get_and_watch(self, path):
        """
        Get the node's data for the given node path and set watch.

        In addition to the deferred method result, this method returns
        a deferred that is called back when the node is modified or
        removed (once).

        @param path: The path of the node whose content will be retrieved.
        """
        d = defer.Deferred()

        def watcher(event_type, conn_state, path, error=None):
            if error:
                d.errback(error)
            else:
                d.callback(ClientEvent(event_type, conn_state, path))
        return self._get(path, watcher), d

    def get_children(self, path):
        """
        Get the ids of all children directly under the given path.

        @param path: The path of the node whose children will be retrieved.
        """
        return self._get_children(path, None)

    def get_children_and_watch(self, path):
        """
        Get the ids of all children directly under the given path.

        In addition to the deferred method result, this method returns
        a deferred that is called back when a change happens on the
        provided path (once).

        @param path: The path of the node whose children will be retrieved.
        """
        d = defer.Deferred()

        def watcher(event_type, conn_state, path, error=None):
            if error:
                d.errback(error)
            else:
                d.callback(ClientEvent(event_type, conn_state, path))
        return self._get_children(path, watcher), d

    def get_acl(self, path):
        """
        Get the list of acls that apply to node with the give path.

        Each acl is a dictionary containing keys/values for scheme, id,
        and perms.

        @param path: The path of the node whose acl will be retrieved.
        """
        d = defer.Deferred()
        if self._check_connected(d):
            return d

        def _cb_get_acl(result_code, acls, stat):
            if self._check_result(result_code, d):
                return
            d.callback((acls, stat))

        callback = self._zk_thread_callback(_cb_get_acl)
        result = zookeeper.aget_acl(self.handle, path, callback)
        self._check_result(result, d)
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
        d = defer.Deferred()
        if self._check_connected(d):
            return d

        callback = self._zk_thread_callback(self._cb_set_acl, d, path, acls)
        result = zookeeper.aset_acl(
            self.handle, path, version, acls, callback)
        self._check_result(result, d)
        return d

    def _cb_set_acl(self, d, path, acls, result_code):
        if self._check_result(result_code, d):
            return
        d.callback(result_code)

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
        d = defer.Deferred()
        if self._check_connected(d):
            return d

        callback = self._zk_thread_callback(self._cb_set, d, path, data)
        result = zookeeper.aset(self.handle, path, data, version, callback)
        self._check_result(result, d)
        return d

    def _cb_set(self, d, path, data, result_code, node_stat):
        if self._check_result(result_code, d):
            return
        d.callback(node_stat)

    def set_connection_watcher(self, watcher):
        """
        Sets a permanent global watcher on the connection. This will get
        notice of changes to the connection state.

        @param: watcher function
        """
        if not callable(watcher):
            raise SyntaxError("Invalid Watcher %r" % (watcher))
        watcher = self._wrap_watcher(watcher, None, None)
        zookeeper.set_watcher(self.handle, watcher)

    def set_session_callback(self, callback):
        """Set a callback to receive session events.

        Session events are by default ignored. Interested applications
        may choose to set a session event watcher on the connection
        to receive session events. Session events are typically broadcast
        by the libzookeeper library to all extant watchers, but the
        twisted integration using deferreds is not capable of receiving
        multiple values (session events and watch events), so this
        client implementation instead provides for a user defined callback
        to be invoked with them instead. The callback receives a single
        parameter, the session event in the form of a ClientEvent instance.

        Additional details on session events
        ------------------------------------
        http://bit.ly/mQrOMY
        http://bit.ly/irKpfn
        """
        if not callable(callback):
            raise TypeError("Invalid callback %r" % callback)
        self._session_event_callback = callback

    def set_connection_error_callback(self, callback):
        """Set a callback to receive connection error exceptions.

        By default the error will be raised when the client API
        call is made. Setting a connection level error handler allows
        applications to centralize their handling of connection loss,
        instead of having to guard every zk interaction.

        The callback receives two parameters, the client instance
        and the exception.
        """
        if not callable(callback):
            raise TypeError("Invalid callback %r" % callback)
        if self._connection_error_callback is not None:
            raise RuntimeError((
                "Connection error handlers can't be changed %s" %
                self._connection_error_callback))
        self._connection_error_callback = callback

    def set_deterministic_order(self, boolean):
        """
        The zookeeper client will by default randomize the server hosts
        it will connect to unless this is set to True.

        This is a global setting across connections.
        """
        zookeeper.deterministic_conn_order(bool(boolean))

    def sync(self, path="/"):
        """Flushes the connected zookeeper server with the leader.

        @param path: The root path to flush, all child nodes are also flushed.
        """
        d = defer.Deferred()
        if self._check_connected(d):
            return d

        def _cb_sync(result_code, path):
            if self._check_result(result_code, d):
                return
            d.callback(path)

        callback = self._zk_thread_callback(_cb_sync)
        result = zookeeper.async(self.handle, path, callback)
        self._check_result(result, d)
        return d
