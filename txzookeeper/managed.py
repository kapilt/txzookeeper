from functools import partial

import zookeeper

from client import ZookeeperClient, ClientEvent


class WatchManager(object):
    pass


class ManagedClient(ZookeeperClient):
    """
    """

    def __init__(self, *args, **kw):
        super(ManagedClient, self).__init__(*args, **kw)
        self._watches = {}
        self._ephemerals = {}

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

    def _wrap_watcher(self, watcher):
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
