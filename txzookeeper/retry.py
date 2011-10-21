import functools
import time

import zookeeper

from twisted.internet.defer import inlineCallbacks, returnValue, Deferred

from client import ZookeeperClient

#
# 1. passthrough methods (connect, close, set_determinstic_order)
# 2. retry methods returning deferreds
# 3. retry method returning two deferreds (watches)
# 4. properties
#


def is_retryable(e):
    return isinstance(
        e,
        (zookeeper.ClosingException,
         zookeeper.ConnectionLossException,
         zookeeper.OperationTimeoutException))


def sleep(delay):
    """Non-blocking sleep."""
    from twisted.internet import reactor
    d = Deferred()
    reactor.callLater(delay, d.callback, None)
    return d


def retry(name, delay=True):

    original = getattr(ZookeeperClient, name)

    @inlineCallbacks
    def wrapper(retry_client, *args, **kw):
        method = getattr(retry_client.client, name)
        # If we keep retrying past the session timeout without success just
        # die, the session expired is fatal.

        session_timeout = retry_client.client.session_timeout
        if session_timeout is None:
            session_timeout = 10
        max_time = session_timeout * 1.5 + time.time()

        while 1:
            try:
                value = method(*args, **kw)
                if isinstance(value, Deferred):
                    value = yield value
            except Exception, e:
                if max_time <= time.time():
                    raise
                if is_retryable(e) and not retry_client.client.unrecoverable:
                    if delay:
                        # Allow for the connection to heal.
                        yield sleep(session_timeout / 10.0)
                    continue
                raise
            returnValue(value)

    return functools.update_wrapper(wrapper, original)


def passmethod(name):

    original = getattr(ZookeeperClient, name)

    def wrapper(retry_client, *args, **kw):
        method = getattr(retry_client.client, name)
        return method(*args, **kw)

    return functools.update_wrapper(wrapper, original)


def passproperty(name):

    def wrapper(retry_client):
        return getattr(retry_client.client, name)
    return wrapper


class RetryClient(object):

    def __init__(self, client):
        self.client = client

    # async methods returning deferreds
    add_auth = retry("add_auth")
    create = retry("create")
    delete = retry("delete")
    exists = retry("exists")
    get = retry("get")
    get_acl = retry("get_acl")
    get_children = retry("get_children")
    set_acl = retry("set_acl")
    set = retry("set_acl")
    sync = retry("sync")

    # pass through methods
    set_session_callback = passmethod("set_session_callback")
    set_connection_error_callback = passmethod("set_connection_error_callback")
    set_determinstic_order = passmethod("set_determinstic_order")
    set_connection_watcher = passmethod("set_connection_watcher")
    close = passmethod("close")
    connect = passmethod("connect")

    # watch methods
    exists_and_watch = None
    get_and_watch = None
    get_children_and_watch = None

    # passthrough properties
    state = property(passproperty("state"))
    client_id = property(passproperty("client_id"))
    session_timeout = property(passproperty("session_timeout"))
    servers = property(passproperty("servers"))
    handle = property(passproperty("handle"))
    connected = property(passproperty("connected"))
    unrecoverable = property(passproperty("unrecoverable"))
