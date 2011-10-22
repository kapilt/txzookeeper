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
        # die, the session expiry is fatal.
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
                        # Allow for the connection to heal, 1/30 of the session
                        # but under 5s
                        retry_delay = session_timeout / (30.0 * 1000)
                        if retry_delay > 5:
                            retry_delay = 5
                        yield sleep(retry_delay)
                    continue
                raise
            returnValue(value)

    return functools.update_wrapper(wrapper, original)


def retry_watch(name):
    # the signature of watch methods returns back a tuple of deferreds
    # not a deferred returning a tuple of deferreds.
    original = getattr(ZookeeperClient, name)

    def wrapper(retry_client, *args, **kw):
        method = getattr(retry_client.client, name)
        session_timeout = retry_client.client.session_timeout
        if session_timeout is None:
            session_timeout = 10
        max_time = session_timeout * 1.5 + time.time()

        v_d, w_d = method(*args, **kw)

        def retry_inner(f):
            """Retry callback

            On retry check the error, and chain the watch callback to
            the original handed back from the api.  Any retryable
            error here would invalidate the watch as is.
            """
            # If the error isn't known, re-raise.
            if not is_retryable(f.value):
                return f
            # If we've exceeded the max allotted time, re-raise.
            if max_time <= time.time():
                return f
            # If the client's been explicitly closed, re-raise
            if not retry_client.client.connected:
                return f
            r_v_d, r_w_d = method(*args, **kw)
            # If we need to retry again.
            r_v_d.addErrback(retry_inner)
            # Chain the new watch deferred to the old, presuming its doa
            # if the value deferred errored on a connection error.
            r_w_d.chainDeferred(w_d)
            # Insert back into the callback chain.
            return r_v_d
        v_d.addErrback(retry_inner)

        return v_d, w_d

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
    set = retry("set")
    sync = retry("sync")

    # pass through methods
    set_session_callback = passmethod("set_session_callback")
    set_connection_error_callback = passmethod("set_connection_error_callback")
    set_determinstic_order = passmethod("set_determinstic_order")
    set_connection_watcher = passmethod("set_connection_watcher")
    close = passmethod("close")
    connect = passmethod("connect")

    # watch methods
    exists_and_watch = retry_watch("exists_and_watch")
    get_and_watch = retry_watch("get_and_watch")
    get_children_and_watch = retry_watch("get_children_and_watch")

    # passthrough properties
    state = property(passproperty("state"))
    client_id = property(passproperty("client_id"))
    session_timeout = property(passproperty("session_timeout"))
    servers = property(passproperty("servers"))
    handle = property(passproperty("handle"))
    connected = property(passproperty("connected"))
    unrecoverable = property(passproperty("unrecoverable"))
