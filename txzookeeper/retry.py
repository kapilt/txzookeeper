#
#  Copyright (C) 2011 Canonical Ltd. All Rights Reserved
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

"""
A retry client facade that transparently handles transient connection
errors.
"""

import functools
import time

import zookeeper

from twisted.internet.defer import inlineCallbacks, returnValue, Deferred


def is_retryable(e):
    """Determine if an exception signifies a recoverable connection error.
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

    The goal is to allow the connection time to auto-heal, before
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

    # Only if the client hasn't been explicitly closed.
    if not retry_client.connected:
        return False

    # Only if the client is in a recoverable state.
    if retry_client.unrecoverable:
        return False

    return True


def retry(func):
    """Constructs a retry wrapper around a function that retries invocations.

    If the function execution results in an exception due to a transient
    connection error, the retry wrapper will reinvoke the operation after
    a suitable delay (fractional value of the session timeout).

    The first paramter of the wrapper is required to be the same
    zookeeper client used by the parameter function.

    :param func: A method or function that interacts with
           zookeeper. If txzookeeper client must the first parameter
           of this function. If its a method the zookeeper client
           passed to the wrapper is not passed to the method. The
           function must return a single value (either a deferred or
           result value).
    """
    method = getattr(func, "im_func", None) and func or None

    @inlineCallbacks
    def wrapper(retry_client, *args, **kw):
        # For clients which aren't connected (session timeout == None)
        # we raise the errors to the callers
        session_timeout = retry_client.session_timeout or 0

        # If we keep retrying past the 1.5 * session timeout without
        # success just die, the session expiry is fatal.
        max_time = session_timeout * 1.5 + time.time()

        while 1:
            try:
                if method:
                    value = yield method(*args, **kw)
                else:
                    value = yield func(retry_client, *args, **kw)
            except Exception, e:
                if not check_retryable(retry_client, max_time, e):
                    raise
                # Give the connection a chance to auto-heal.
                yield sleep(get_delay(session_timeout))
                continue

            returnValue(value)

    return functools.update_wrapper(wrapper, func)


def retry_watch(func):
    """Contructs a wrapper around a watch function that retries invocations.

    If the function execution results in an exception due to a transient
    connection error, the retry wrapper will reinvoke the operation after
    a suitable delay (fractional value of the session timeout).

    A watch function must return back a tuple of deferreds
    (value_deferred, watch_deferred). No inline callbacks are
    performed in the wrapper to ensure that callers continue to see a
    tuple of results.

    The first paramter of the wrapper is required to be the same
    zookeeper client used by the parameter function.

    :param func: A function that interacts with zookeeper. If a
           function is passed, a txzookeeper client must the first
           parameter of this function. If its a method the zookeeper
           client passed to the wrapper is not passed to the
           method. The function must return a tuple of
           (value_deferred, watch_deferred)
    """

    method = getattr(func, "im_func", None) and func or None

    def wrapper(retry_client, *args, **kw):
        # For clients which aren't connected (session timeout == None)
        # we raise the usage errors to the callers
        session_timeout = retry_client.session_timeout or 0

        # If we keep retrying past the 1.5 * session timeout without
        # success just die, the session expiry is fatal.
        max_time = session_timeout * 1.5 + time.time()

        if method:
            value_d, watch_d = method(*args, **kw)
        else:
            value_d, watch_d = func(retry_client, *args, **kw)

        def retry_delay(f):
            """Errback, verifes an op is retryable, and delays the next retry.
            """
            # Check that operation is retryable.
            if not check_retryable(retry_client, max_time, f.value):
                return f

            # Give the connection a chance to auto-heal
            d = sleep(get_delay(session_timeout))
            d.addCallback(retry_inner)
            return d

        def retry_inner(value):
            """Retry operation invoker.
            """
            # Invoke the method
            if method:
                retry_value_d, retry_watch_d = method(*args, **kw)
            else:
                retry_value_d, retry_watch_d = func(retry_client, *args, **kw)

            # If we need to retry again.
            retry_value_d.addErrback(retry_delay)

            # Chain the new watch deferred to the old, presuming its doa
            # if the value deferred errored on a connection error.
            retry_watch_d.chainDeferred(watch_d)

            # Insert back into the callback chain.
            return retry_value_d

        # Attach the retry
        value_d.addErrback(retry_delay)

        return value_d, watch_d

    return functools.update_wrapper(wrapper, func)


def _passmethod(method):
    """Returns a method wrapper that directly invokes the client's method.
    """
    def wrapper(retry_client, *args, **kw):
        return method(*args, **kw)
    return functools.update_wrapper(wrapper, method)


def _passproperty(name):
    """Returns a method wrapper that delegates to a client's property.
    """
    def wrapper(retry_client):
        return getattr(retry_client.client, name)
    return wrapper


class RetryClient(object):
    """A ZookeeperClient wrapper that transparently performs retries.

    A zookeeper connection can experience transient connection failures
    on any operation. As long as the session associated to the connection
    is still active on the zookeeper cluster, libzookeeper can reconnect
    automatically to the cluster and session and the client is able to
    retry.

    Whether a given operation is safe for retry depends on the application
    in question and how's interacting with zookeeper.

    In particular coordination around sequence nodes can be
    problematic, as the client has no way of knowing if the operation
    succeed or not without additional application specific context.

    Idempotent operations against the zookeeper tree are generally
    safe to retry.

    This class provides a simple wrapper around a zookeeper client,
    that will automatically perform retries on operations that
    interact with the zookeeper tree, in the face of transient errors,
    till the session timeout has been reached. All of the attributes
    and methods of a zookeeper client are exposed.

    All the methods of the client that interact with the zookeeper tree
    are retry enabled.
    """

    def __init__(self, client):
        self.client = client
        self._initialize()

    def _bind(self, name, factory):
        method = getattr(self.client, name)
        retry_func = factory(method)
        retry_method = retry_func.__get__(self)
        setattr(self, name, retry_method)

    def _initialize(self):
        # async client methods returning deferreds
        for i in ["add_auth",
                  "create",
                  "delete",
                  "exists",
                  "get",
                  "get_acl",
                  "get_children",
                  "set_acl",
                  "set",
                  "sync"]:
            self._bind(i, retry)

        # watch methods
        for i in ["exists_and_watch",
                  "get_and_watch",
                  "get_children_and_watch"]:
            self._bind(i, retry_watch)

        # pass through methods
        for i in ["set_session_callback",
                  "set_connection_error_callback",
                  "set_determinstic_order",
                  "set_connection_watcher",
                  "close",
                  "connect"]:
            self._bind(i, _passmethod)

    # passthrough properties
    state = property(_passproperty("state"))
    client_id = property(_passproperty("client_id"))
    session_timeout = property(_passproperty("session_timeout"))
    servers = property(_passproperty("servers"))
    handle = property(_passproperty("handle"))
    connected = property(_passproperty("connected"))
    unrecoverable = property(_passproperty("unrecoverable"))
