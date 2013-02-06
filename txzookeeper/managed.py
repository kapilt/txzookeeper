from functools import partial

import contextlib
import logging
import time
import zookeeper

from twisted.internet.defer import (
    inlineCallbacks, DeferredLock, fail, returnValue, Deferred)

from client import (
    ZookeeperClient, ClientEvent, NotConnectedException,
    ConnectionTimeoutException)
from retry import RetryClient
from utils import sleep


class StopWatcher(Exception):
    pass

BACKOFF_INCREMENT = 10
MAX_BACKOFF = 360

WATCH_KIND_MAP = {
    "child": "get_children_and_watch",
    "exists": "exists_and_watch",
    "get": "get_and_watch"}


log = logging.getLogger("txzk.managed")


class Watch(object):
    """
    For application driven persistent watches, where the application
    is manually resetting the watch.
    """

    __slots__ = ("_mgr", "_client", "_path", "_kind", "_callback")

    def __init__(self, mgr, path, kind, callback):
        self._mgr = mgr
        self._path = path
        self._kind = kind
        self._callback = callback

    @property
    def path(self):
        return self._path

    @property
    def kind(self):
        return self._kind

    @contextlib.contextmanager
    def _ctx(self):
        mgr = self._mgr
        del self._mgr
        try:
            yield mgr
        finally:
            mgr.remove(self)

    @inlineCallbacks
    def reset(self):
        with self._ctx():
            yield self._callback(
                zookeeper.SESSION_EVENT,
                zookeeper.CONNECTED_STATE,
                self._path)

    def __call__(self, *args, **kw):
        with self._ctx():
            return self._callback(*args, **kw)

    def __str__(self):
        return "<Watcher %s %s %r>" % (self.kind, self.path, self._callback)


class WatchManager(object):

    watch_class = Watch

    def __init__(self):
        self._watches = []

    def add(self, path, watch_type, watcher):
        w = self.watch_class(self, path, watch_type, watcher)
        self._watches.append(w)
        return w

    def remove(self, w):
        try:
            self._watches.remove(w)
        except ValueError:
            pass

    def iterkeys(self):
        for w in self._watches:
            yield (w.path, w.kind)

    def clear(self):
        del self._watches
        self._watches = []

    @inlineCallbacks
    def reset(self, *ignored):
        watches = self._watches
        self._watches = []

        for w in watches:
            try:
                yield w.reset()
            except Exception, e:
                log.error("Error reseting watch %s with session event. %s %r",
                          w, e, e)
                continue


class SessionClient(ZookeeperClient):
    """A managed client that automatically re-establishes ephemerals and
    triggers watches after reconnecting post session expiration.

    This abstracts the client from session expiration handling. It does
    come at a cost though.

    There are two application constraints that need to be considered for usage
    of the SessionClient or ManagedClient. The first is that watch callbacks
    which examine the event, must be able to handle the synthetic session
    event which is sent to them when the session is re-established.

    The second and more problematic is that algorithms/patterns
    utilizing ephemeral sequence nodes need to be rethought, as the
    session client will recreate the nodes when reconnecting at their
    previous paths. Some algorithms (like the std zk lock recipe) rely
    on properties like the smallest valued ephemeral sequence node in
    a container to identify the lock holder, with the notion that upon
    session expiration a new lock/leader will be sought. Sequence
    ephemeral node recreation in this context is problematic as the
    node is recreated at the exact previous path. Alternative lock
    strategies that do work are fairly simple at low volume, such as
    owning a particular node path (ie. /locks/lock-holder) with an
    ephemeral.

    As a result the session client only tracks and restablishes non sequence
    ephemeral nodes. For coordination around ephemeral sequence nodes it
    provides for watching for the establishment of new sessions via
    `subscribe_new_session`
    """

    def __init__(
        self, servers=None, session_timeout=None, connect_timeout=4000):
        """
        """
        super(SessionClient, self).__init__(servers, session_timeout)
        self._connect_timeout = connect_timeout
        self._watches = WatchManager()
        self._ephemerals = {}
        self._session_notifications = []
        self._reconnect_lock = DeferredLock()
        self.set_connection_error_callback(self._cb_connection_error)
        self.set_session_callback(self._cb_session_event)
        self._backoff_seconds = 0
        self._last_reconnect = time.time()

    def subscribe_new_session(self):
        d = Deferred()
        self._session_notifications.append(d)
        return d

    @inlineCallbacks
    def cb_restablish_session(self, e=None, forced=False):
        """Called on intercept of session expiration to create new session.

        This will reconnect to zk, re-establish ephemerals, and
        trigger watches.
        """

        yield self._reconnect_lock.acquire()
        log.debug(
            "Connection reconnect, lock acquired handle:%d", self.handle)

        try:
            # If its been explicitly closed, don't re-establish.
            if self.handle is None:
                log.debug("No handle, client closed")
                return

            # Its already connected, don't re-establish.
            if not forced and not self.unrecoverable:
                log.debug("Client already connected, allowing retry")
                return
            elif self.connected or self.handle:
                self.close()
                self.handle = -1

            # Re-establish
            yield self._cb_restablish_session().addErrback(
                self._cb_restablish_errback, e)

        except Exception, e:
            log.error("error while re-establish %r %s" % (e, e))
        finally:
            log.debug("Reconnect lock released %s", self)
            log.debug("")
            yield self._reconnect_lock.release()

    @inlineCallbacks
    def _cb_restablish_session(self):
        """Re-establish a new session, and recreate ephemerals and watches.
        """
        # Reconnect
        while 1:
            log.debug("Reconnect loop")

            # If we have some failures, back off
            if self._backoff_seconds:
                log.debug("Backing off reconnect %d" % self._backoff_seconds)
                yield sleep(self._backoff_seconds)

            # The client was explicitly closed, abort reconnect.
            if self.handle is None:
                returnValue(self.handle)
            try:
                yield self.connect(timeout=self._connect_timeout)
                log.info(
                    "Restablished connection")
                self._last_reconnect = time.time()
            except ConnectionTimeoutException:
                log.info("Timeout establishing connection, retrying...")
                pass
            except zookeeper.ZooKeeperException, e:
                log.exception("Error while connecting %r %s" % (e, e))
            except Exception, e:
                log.info("Unknown error, %s", e)
                raise
            else:
                break

            if self._backoff_seconds < MAX_BACKOFF:
                self._backoff_seconds = min(
                    self._backoff_seconds + BACKOFF_INCREMENT,
                    MAX_BACKOFF - 1)

        # Recreate ephemerals
        items = self._ephemerals.items()
        self._ephemerals = {}

        for path, e in items:
            try:
                yield self.create(
                    path, e['data'], acls=e['acls'], flags=e['flags'])
            except zookeeper.NodeExistsException:
                log.error("Attempt to create ephemeral node failed %r", path)

        # Signal watches
        yield self._watches.reset()

        # Notify new session observers
        notifications = self._session_notifications
        self._session_notifications = []

        # all good, reset backoff
        self._backoff_seconds = 0

        for n in notifications:
            n.callback(True)

    def _cb_restablish_errback(self, err, failure):
        """If there's an error re-establishing the session log it.
        """
        log.error("Error while trying to re-establish connection %s\n%s" % (
            failure.value, failure.getTraceback()))
        return failure

    @inlineCallbacks
    def _cb_connection_error(self,  client, error):
        """Convert session expiration to a transient connection error.

        Dispatches from api usage error.
        """
        if not isinstance(error, (zookeeper.SessionExpiredException,
                                  NotConnectedException,
                                  zookeeper.ClosingException)):
            raise error
        log.debug("Connection error detected, reconnecting...")
        yield self.cb_restablish_session()
        raise zookeeper.ConnectionLossException

    # Dispatch from retry exceed session maximum
    def cb_retry_expired(self, error):
        log.info("Persistent retry error, reconnecting...")
        return self.cb_restablish_session(forced=True)

    # Dispatch from connection events
    def _cb_session_event(self, client, event):
        if (event.type == zookeeper.SESSION_EVENT and
            event.connection_state == zookeeper.EXPIRED_SESSION_STATE):
            log.debug("Client session expired event, restablishing")
            self.cb_restablish_session()

    # Client connected tracker on client operations.
    def _check_connected(self, d):
        """Clients are automatically reconnected."""
        if self.connected:
            return

        if self.handle is None:
            d.errback(NotConnectedException("Connection closed"))
            return d

        log.info("Detected dead connection, reconnecting...")
        c_d = self.cb_restablish_session()

        def after_connected(client):
            """Return a transient connection failure.

            The retry client will automatically attempt to retry the operation.
            """
            log.debug("Reconnected, returning transient error")
            return fail(zookeeper.ConnectionLossException("Retry"))

        c_d.addCallback(after_connected)
        c_d.chainDeferred(d)
        return d

    # Dispatch from node watches on session expiration
    def _watch_session_wrapper(self, watcher, event_type, conn_state, path):
        """Watch wrapper that diverts session events to a connection callback.
        """
        if (event_type == zookeeper.SESSION_EVENT and
                conn_state == zookeeper.EXPIRED_SESSION_STATE):
            if self.unrecoverable:
                log.debug("Watch got session expired event, reconnecting...")
                d = self.cb_restablish_session()
                d.addErrback(self._cb_restablish_errback)
                return d

        if event_type == zookeeper.SESSION_EVENT:
            if self._session_event_callback:
                self._session_event_callback(
                    self, ClientEvent(
                        event_type, conn_state, path, self.handle))
        else:
            return watcher(event_type, conn_state, path)

    # Track all watches
    def _wrap_watcher(self, watcher, watch_type, path):
        if watcher is None:
            return watcher
        if not callable(watcher):
            raise SyntaxError("invalid watcher")

        # handle conn watcher, separately.
        if watch_type is None and path is None:
            return self._zk_thread_callback(
                self._watch_session_wrapper, watcher)

        return self._zk_thread_callback(
            partial(
                self._watch_session_wrapper,
                self._watches.add(path, watch_type, watcher)))

    # Track ephemerals
    def _cb_created(self, d, data, acls, flags, result_code, path):
        if self._check_result(result_code, d, path=path):
            return

        if (flags & zookeeper.EPHEMERAL) and not (
            flags & zookeeper.SEQUENCE):
            self._ephemerals[path] = dict(
                data=data, acls=acls, flags=flags)

        d.callback(path)

    def _cb_deleted(self, d, path, result_code):
        if self._check_result(result_code, d, path=path):
            return

        self._ephemerals.pop(path, None)
        d.callback(result_code)

    def _cb_set_acl(self, d, path, acls, result_code):
        if self._check_result(result_code, d, path=path):
            return

        if path in self._ephemerals:
            self._ephemerals[path]['acls'] = acls

        d.callback(result_code)

    def _cb_set(self, d, path, data, result_code, node_stat):
        if self._check_result(result_code, d, path=path):
            return

        if path in self._ephemerals:
            self._ephemerals[path]['data'] = data

        d.callback(node_stat)


class _ManagedClient(RetryClient):

    def subscribe_new_session(self):
        return self.client.subscribe_new_session()


def ManagedClient(servers=None, session_timeout=None, connect_timeout=10000):
    client = SessionClient(servers, session_timeout, connect_timeout)
    managed_client = _ManagedClient(client)
    managed_client.set_retry_error_callback(client.cb_retry_expired)
    return managed_client
