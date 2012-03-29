from functools import partial

import contextlib
import logging
import zookeeper

from twisted.internet.defer import inlineCallbacks

from client import ZookeeperClient, ClientEvent
from retry import RetryClient


class StopWatcher(Exception):
    pass


WATCH_KIND_MAP = {
    "child": "get_children_and_watch",
    "exists": "exists_and_watch",
    "get": "get_and_watch"
    }


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
        with self._ctx() as mgr:
            yield self._callback(
                ClientEvent(zookeeper.SESSION_EVENT,
                            zookeeper.CONNECTED_STATE,
                            self._path))
            mgr  # keep the flakes happy

    def __call__(self, *args, **kw):
        with self._ctx() as mgr:
            mgr  # keep the flakes happy
            if isinstance(self._callback, str):
                import pdb; pdb.set_trace()
                print self
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
            except:
                log.error("Error reseting watch %s with session event.", w)
                continue


class SessionClient(ZookeeperClient):
    """A managed client that automatically restablishes ephemerals and
    triggers watches after reconnecting post session expiration.

    This abstracts the client from session expiration handling. It does
    come at a cost though.
    """

    def __init__(
        self, servers=None, session_timeout=None, connect_timeout=4000):
        """
        """
        super(SessionClient, self).__init__(servers, session_timeout)
        self._connect_timeout = connect_timeout
        self._watches = WatchManager()
        self._ephemerals = {}
        #self._connect_lock = DeferredLock()

    @inlineCallbacks
    def cb_restablish_session(self, e=None):
        """Called on session expiration to restablish the session.

        This will reconnect
        """
        while 1:
            try:
                yield self.connect(timeout=self._connect_timeout)
            except:
                continue
            else:
                break
        for path, e in self._ephemerals:
            try:
                yield self.create(
                    path, e['data'], acls=e['acls'], flags=['flags'])
            except zookeeper.NodeExistsException:
                log.error("Attempt to create ephemeral node failed %r", path)

        yield self.watches.reset()

    # On restablish errback
    def _on_restablish_errback(self, error):
        print "Error", error

    # Dispatch on session expiration
    def _watch_session_wrapper(self, watcher, event_type, conn_state, path):
        """Watch wrapper that diverts session events to a connection callback.
        """
        if (event_type == zookeeper.SESSION_EVENT and
            conn_state == zookeeper.EXPIRED_SESSION_STATE):
            d = self.cb_restablish_session()
            d.addErrback(self._on_restablish_errback)
            return d
        if event_type == zookeeper.SESSION_EVENT:
            if self._session_event_callback:
                self._session_event_callback(
                    self, ClientEvent(event_type, conn_state, path))
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
        if self._check_result(result_code, d):
            return

        if flags & zookeeper.EPHEMERAL:
            self._ephemerals[path] = dict(
                data=data, acls=acls, flags=flags)

        d.callback(path)

    def _cb_deleted(self, d, path, result_code):
        if self._check_result(result_code, d):
            return

        self._ephemerals.pop(path, None)
        d.callback(result_code)

    def _cb_set_acl(self, d, path, acls, result_code):
        if self._check_result(result_code, d):
            return

        if path in self._ephemerals:
            self._ephemerals[path]['acls'] = acls

        d.callback(result_code)

    def _cb_set(self, d, path, data, result_code, node_stat):
        if self._check_result(result_code, d):
            return

        if path in self._ephemerals:
            self._ephemerals[path]['data'] = data

        d.callback(node_stat)


class ManagedClient(RetryClient):

    def __init__(self, *args, **kw):
        client = SessionClient(*args, **kw)
        super(ManagedClient, self).__init__(
            client, client.cb_restablish_session)


def ManagedClient(servers=None, session_timeout=None, connect_timeout=10000):
    from retry import RetryClient
    client = SessionClient(servers, session_timeout, connect_timeout)
    return RetryClient(client, client.cb_restablish_session)
