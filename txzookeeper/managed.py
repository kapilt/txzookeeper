from functools import partial

import zookeeper
from twisted.internet.defer import inlineCallbacks

from client import ZookeeperClient, ClientEvent
from retry import RetryClient


class Watch(object):

    __slots__ = ("client", "path", "kind", "callback")

    def __init__(self, client, path, kind, callback):
        self._cllient = client
        self._path = path
        self._kind = kind
        self._callback = callback

    @property
    def path(self):
        return self._path

    @property
    def kind(self):
        return self._kind

    def reset(self):
        self.callback()

    def __call__(self):
        pass


class WatchManager(object):

    watch_class = Watch

    def __init__(self):
        self._watches = {}

    def add(self, path, watch_type, watcher):
        w = self.watch_class(path, watch_type, watcher)
        self._watches.append(w)
        return w

    def clear(self):
        self._watches = []

    def reset(self, *ignored):
        watches = self._watches
        for w in watches:
            w.reset()
        self._watches = []


class SessionClient(ZookeeperClient):
    """A managed client that automatically restablishes ephemerals and
    triggers watches after reconnecting post session expiration.

    This abstracts the client from session expiration handling. It does
    come at a cost though.
    """

    def __init__(
        self, servers=None, session_timeout=None, connect_timeout=10000):
        """
        """
        super(SessionClient, self).__init__(servers, session_timeout)
        self._connect_timeout = connect_timeout
        self._watches = WatchManager()
        self._ephemerals = {}

    # Restablish session ephemerals and watches
    @inlineCallbacks
    def cb_restablish_session(self, e=None):
        while 1:
            try:
                yield self.connect(timeout=self._connect_timeout)
            except:
                continue
            else:
                break
        for path, e in self._ephemerals:
            yield self.create(path, e['data'], acls=e['acls'], flags=['flags'])
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
            d = self._on_restablish_session()
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

        return self._zk_thread_callback(
            partial(
                self._watch_session_wrapper,
                self._watches.add(watcher, watch_type, path)))

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

    def _cb_set_acl(self, path, acls, d, result_code):
        if self._check_result(result_code, d):
            return

        if path in self._ephemerals:
            self._ephemerals[path]['acls'] = acls

        d.callback(result_code)

    def _cb_set(self, path, data, d, result_code, node_stat):
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
