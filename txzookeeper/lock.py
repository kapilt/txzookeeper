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

import zookeeper
from twisted.internet.defer import fail


class LockError(Exception):
    """
    A usage or parameter exception that violated the lock conditions.
    """


class Lock(object):
    """
    A distributed exclusive lock, based on the apache zookeeper recipe.

    http://hadoop.apache.org/zookeeper/docs/r3.3.0/recipes.html
    """

    prefix = "lock-"

    def __init__(self, path, client):
        self._path = path
        self._client = client
        self._candidate_path = None
        self._acquired = False

    @property
    def path(self):
        """Return the path to the lock."""
        return self._path

    @property
    def acquired(self):
        """Has the lock been acquired. Returns a boolean"""
        return self._acquired

    def acquire(self):
        """Acquire the lock."""

        if self._acquired:
            error = LockError("Already holding the lock %s" % (self.path))
            return fail(error)

        if self._candidate_path is not None:
            error = LockError("Already attempting to acquire the lock")
            return fail(error)

        self._candidate_path = ""

        # Create our candidate node in the lock directory.
        d = self._client.create(
            "/".join((self.path, self.prefix)),
            flags=zookeeper.EPHEMERAL | zookeeper.SEQUENCE)

        d.addCallback(self._on_candidate_create)
        d.addErrback(self._on_no_queue_error)
        return d

    def _on_candidate_create(self, path):
        self._candidate_path = path
        return self._acquire()

    def _on_no_queue_error(self, failure):
        self._candidate_path = None
        return failure

    def _acquire(self, *args):
        d = self._client.get_children(self.path)
        d.addCallback(self._check_candidate_nodes)
        d.addErrback(self._on_no_queue_error)
        return d

    def _check_candidate_nodes(self, children):
        """
        Check if our lock attempt candidate path is the best candidate
        among the list of children names. If it is then we hold the lock
        if its not then watch the nearest candidate till it is.
        """
        candidate_name = self._candidate_path[
            self._candidate_path.rfind('/') + 1:]

        # Check to see if our node is the first candidate in the list.
        children.sort()
        assert candidate_name in children
        index = children.index(candidate_name)

        if index == 0:
            # If our candidate is first, then we already have the lock.
            self._acquired = True
            return self

        # If someone else holds the lock, then wait until holder immediately
        # before us releases the lock or dies.
        previous_path = "/".join((self.path, children[index - 1]))
        exists_deferred, watch_deferred = self._client.exists_and_watch(
            previous_path)
        exists_deferred.addCallback(
            self._check_previous_owner_existence,
            watch_deferred)
        return exists_deferred

    def _check_previous_owner_existence(self, previous_owner_exists,
                                        watch_deferred):
        if not previous_owner_exists:
            # Hah! It's actually already dead!  That was quick.  Note
            # how we never use the watch deferred in this case.
            return self._acquire()
        else:
            # Nope, there's someone ahead of us in the queue indeed. Let's
            # wait for the watch to detect it went away.
            watch_deferred.addCallback(self._acquire)
            return watch_deferred

    def release(self):
        """Release the lock."""

        if not self._acquired:
            error = LockError("Not holding lock %s" % (self.path))
            return fail(error)

        d = self._client.delete(self._candidate_path)

        def on_delete_success(value):
            self._candidate_path = None
            self._acquired = False
            return True

        d.addCallback(on_delete_success)
        return d
