"""
A retry client facade, that transparently handles transient connection
errors.
"""

import functools
import time

import zookeeper

from twisted.internet.defer import inlineCallbacks, returnValue, Deferred

from client import ZookeeperClient


def is_retryable(e):
    """Determine if an exception signfies a recoverable connection error.
    """
    return isinstance(
        e,
        (zookeeper.ClosingException,
         zookeeper.ConnectionLossException,
         zookeeper.OperationTimeoutException))


def sleep(delay):
    """Non-blocking sleep.
    """
    from twisted.internet import reactor
    d = Deferred()
    reactor.callLater(delay, d.callback, None)
    return d


def get_delay(session_timeout, max_delay=5, session_fraction=30.0):
    """Get retry delay between retrying an operation.

    Returns either the specified fraction of a session timeout or the
    max delay, whichever is smaller.

    The goal is to allow for the connection time to auto-heal, before
    retrying an operation.

    :param session_timeout: The timeout for the session, in milliseconds
    :param max_delay: The max delay for a retry, in seconds.
    :param session_fraction: The fractinoal amount of a timeout to wait
    """
    retry_delay = session_timeout / (float(session_fraction) * 1000)
    if retry_delay > max_delay:
        return max_delay

    return retry_delay


def check_retryable(retry_client, max_time, error):
    """Check if the operation is retryable.
    """
    # Only if the error is known.
    if not is_retryable(error):
        return False

    # Only if we've haven't exceeded the max allotted time.
    if max_time <= time.time():
        return False

    # Only if the client hasn't been explicitly closed
    if not retry_client.connected:
        return False

    # Only If the client is in a recoverable state
    if retry_client.unrecoverable:
        return False

    return True


def retry(name):
    """Returns a facade method that incorporates automatically retrying.

    :param name: The name of the zookeeperclient method to decorate with retry.
    :param delay: Whether delay should be employed between operations.
    """
    original = getattr(ZookeeperClient, name)

    @inlineCallbacks
    def wrapper(retry_client, *args, **kw):
        method = getattr(retry_client.client, name)

        # If we keep retrying past the session timeout without success just
        # die, the session expiry is fatal.
        session_timeout = retry_client.session_timeout or 10
        max_time = session_timeout * 1.5 + time.time()

        while 1:
            try:
                value = method(*args, **kw)
                if isinstance(value, Deferred):
                    value = yield value
            except Exception, e:
                if not check_retryable(retry_client, max_time, e):
                    raise
                # Give the connection a chance to auto-heal.
                yield sleep(get_delay(session_timeout))
                continue

            returnValue(value)

    return functools.update_wrapper(wrapper, original)


def retry_watch(name):
    """Returns a facade method with retry for methods that return watches.

    :param name: The name of the zookeeperclient method to decorate with retry.
    :param delay: Whether delay should be employed between operations.
    """
    # The signature of watch methods returns back a tuple of deferreds
    # not a deferred returning a tuple of deferreds, hence no inlinecallbacks.
    original = getattr(ZookeeperClient, name)

    def wrapper(retry_client, *args, **kw):
        method = getattr(retry_client.client, name)
        session_timeout = retry_client.session_timeout or 10
        max_time = session_timeout * 1.5 + time.time()

        v_d, w_d = method(*args, **kw)

        def retry_delay(f):
            """Retry operation delay, verifies retryability and delays.
            """
            # Check that operation is retryable.
            if not check_retryable(retry_client, max_time, f.value):
                return f

            # Give the connection a chance to auto-heal
            d = sleep(get_delay(session_timeout))
            d.addCallback(retry_inner)
            return d

        def retry_inner(f):
            """Retry operation invoker.
            """
            # Invoke the method
            r_v_d, r_w_d = method(*args, **kw)

            # If we need to retry again.
            r_v_d.addErrback(retry_delay)

            # Chain the new watch deferred to the old, presuming its doa
            # if the value deferred errored on a connection error.
            r_w_d.chainDeferred(w_d)

            # Insert back into the callback chain.
            return r_v_d

        # Attach the retry
        v_d.addErrback(retry_delay)

        return v_d, w_d

    return functools.update_wrapper(wrapper, original)


def passmethod(name):
    """Returns a facade method that directly invokes the client's method.
    """
    original = getattr(ZookeeperClient, name)

    def wrapper(retry_client, *args, **kw):
        method = getattr(retry_client.client, name)
        return method(*args, **kw)

    return functools.update_wrapper(wrapper, original)


def passproperty(name):
    """Returns a facade method that delegates to a client's property.
    """
    def wrapper(retry_client):
        return getattr(retry_client.client, name)
    return wrapper


class RetryClient(object):
    """A ZookeeperClient wrapper that transparently performs retries.
    """
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
