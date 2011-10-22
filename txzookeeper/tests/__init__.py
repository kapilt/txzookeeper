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

import sys

import zookeeper
from twisted.internet.defer import Deferred
from twisted.trial.unittest import TestCase

from mocker import MockerTestCase


class ZookeeperTestCase(TestCase, MockerTestCase):

    def setUp(self):
        super(ZookeeperTestCase, self).setUp()
        zookeeper.set_debug_level(0)

    def tearDown(self):
        super(ZookeeperTestCase, self).tearDown()

    def get_log(self):
        return open(self.log_file_path).read()

    def sleep(self, delay):
        """Non-blocking sleep."""
        from twisted.internet import reactor
        deferred = Deferred()
        reactor.callLater(delay, deferred.callback, None)
        return deferred

    _missing_attr = object()

    def patch(self, object, attr, value):
        """Replace an object's attribute, and restore original value later.

        Returns the original value of the attribute if any or None.
        """
        original_value = getattr(object, attr, self._missing_attr)

        @self.addCleanup
        def restore_original():
            if original_value is self._missing_attr:
                try:
                    delattr(object, attr)
                except AttributeError:
                    pass
            else:
                setattr(object, attr, original_value)
        setattr(object, attr, value)

        if original_value is self._missing_attr:
            return None
        return original_value


def egg_test_runner():
    """
    Test collector and runner for setup.py test
    """
    from twisted.scripts.trial import run
    original_args = list(sys.argv)
    sys.argv = ["", "txzookeeper"]
    try:
        return run()
    finally:
        sys.argv = original_args
