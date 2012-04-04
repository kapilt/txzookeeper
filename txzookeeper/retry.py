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

import time

import zookeeper

from twisted.internet.defer import inlineCallbacks, returnValue, Deferred

__all__ = ["retry", "RetryClient"]


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
    :param session_fraction: The fractional amount of a timeout to wait
    """
    retry_delay = session_timeout / (float(session_fraction) * 1000)
    return min(retry_delay, max_delay)


def check_retryable(retry_client, max_time, error):
    """Check an error and a client to see if an operation is retryable.

    :param retry_client: A txzookeeper client
    :param max_time: The max time (epoch tick) that the op is retryable till.
    :param error: The client operation exception.
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


@inlineCallbacks
def retry(client, func, *args, **kw):
    """Constructs a retry wrapper around a function that retries invocations.

    If the function execution results in an exception due to a transient
    connection error, the retry wrapper will reinvoke the operation after
    a suitable delay (fractional value of the session timeout).

    :param client: A ZookeeperClient instance.
    :param func: A callable python object that interacts with
           zookeeper, the callable must utilize the same zookeeper
           connection as passed in the `client` param. The function
           must return a single value (either a deferred or result
           value).
    """
    while 1:
        try:
            value = yield func(*args, **kw)
        except Exception, e:
            # For clients which aren't connected (session timeout == None)
            # we raise the errors to the callers
            session_timeout = client.session_timeout or 0

            # If we keep retrying past the 1.5 * session timeout without
            # success just die, the session expiry is fatal.
            max_time = session_timeout * 1.5 + time.time()
            if not check_retryable(client, max_time, e):
                raise

            # Give the connection a chance to auto-heal.
            yield sleep(get_delay(session_timeout))
            continue

        returnValue(value)


def retry_watch(client, func, *args, **kw):
    """Contructs a wrapper around a watch callable that retries invocations.

    If the callable execution results in an exception due to a transient
    connection error, the retry wrapper will reinvoke the operation after
    a suitable delay (fractional value of the session timeout).

    A watch function must return back a tuple of deferreds
    (value_deferred, watch_deferred). No inline callbacks are
    performed in here to ensure that callers continue to see a
    tuple of results.

    The client passed to this retry function must be the same as
    the one utilized by the python callable.

    :param client: A ZookeeperClient instance.
    :param func: A python callable that interacts with zookeeper. If a
           function is passed, a txzookeeper client must the first
           parameter of this function. The function must return a
           tuple of (value_deferred, watch_deferred)
    """
    # For clients which aren't connected (session timeout == None)
    # we raise the usage errors to the callers
    session_timeout = client.session_timeout or 0

    # If we keep retrying past the 1.5 * session timeout without
    # success just die, the session expiry is fatal.
    max_time = session_timeout * 1.5 + time.time()
    value_d, watch_d = func(*args, **kw)

    def retry_delay(f):
        """Errback, verifes an op is retryable, and delays the next retry.
        """
        # Check that operation is retryable.
        if not check_retryable(client, max_time, f.value):
            return f

        # Give the connection a chance to auto-heal
        d = sleep(get_delay(session_timeout))
        d.addCallback(retry_inner)

        return d

    def retry_inner(value):
        """Retry operation invoker.
        """
        # Invoke the function
        retry_value_d, retry_watch_d = func(*args, **kw)

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


def _passproperty(name):
    """Returns a method wrapper that delegates to a client's property.
    """
    def wrapper(retry_client):
        return getattr(retry_client.client, name)
    return property(wrapper)


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

    def add_auth(self, *args, **kw):
        return retry(self.client, self.client.add_auth, *args, **kw)

    def create(self, *args, **kw):
        return retry(self.client, self.client.create, *args, **kw)

    def delete(self, *args, **kw):
        return retry(self.client, self.client.delete, *args, **kw)

    def exists(self, *args, **kw):
        return retry(self.client, self.client.exists, *args, **kw)

    def get(self, *args, **kw):
        return retry(self.client, self.client.get, *args, **kw)

    def get_acl(self, *args, **kw):
        return retry(self.client, self.client.get_acl, *args, **kw)

    def get_children(self, *args, **kw):
        return retry(self.client, self.client.get_children, *args, **kw)

    def set_acl(self, *args, **kw):
        return retry(self.client, self.client.set_acl, *args, **kw)

    def set(self, *args, **kw):
        return retry(self.client, self.client.set, *args, **kw)

    def sync(self, *args, **kw):
        return retry(self.client, self.client.sync, *args, **kw)

    # Watch retries

    def exists_and_watch(self, *args, **kw):
        return retry_watch(
            self.client, self.client.exists_and_watch, *args, **kw)

    def get_and_watch(self, *args, **kw):
        return retry_watch(
            self.client, self.client.get_and_watch, *args, **kw)

    def get_children_and_watch(self, *args, **kw):
        return retry_watch(
            self.client, self.client.get_children_and_watch, *args, **kw)

    # Passthrough methods

    def set_connection_watcher(self, *args, **kw):
        return self.client.set_connection_watcher(*args, **kw)

    def set_connection_error_callback(self, *args, **kw):
        return self.client.set_connection_error_callback(*args, **kw)

    def set_session_callback(self, *args, **kw):
        return self.client.set_session_callback(*args, **kw)

    def set_determinstic_order(self, *args, **kw):
        return self.client.set_determinstic_order(*args, **kw)

    def close(self):
        return self.client.close()

    def connect(self, *args, **kw):
        return self.client.connect(*args, **kw)

    # passthrough properties
    state = _passproperty("state")
    client_id = _passproperty("client_id")
    session_timeout = _passproperty("session_timeout")
    servers = _passproperty("servers")
    handle = _passproperty("handle")
    connected = _passproperty("connected")
    unrecoverable = _passproperty("unrecoverable")
