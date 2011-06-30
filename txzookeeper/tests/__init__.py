#
#  Copyright (C) 2010 Canonical Ltd. All Rights Reserved
#
#  This file is part of txzookeeper.
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
from twisted.trial.unittest import TestCase

from mocker import MockerTestCase


class ZookeeperTestCase(TestCase, MockerTestCase):

    def setUp(self):
        super(ZookeeperTestCase, self).setUp()
        self.log_file_path = self.makeFile()
        self.log_file = open(self.log_file_path, 'w')
        #zookeeper.set_log_stream(self.log_file)
        zookeeper.set_debug_level(0)

    def tearDown(self):
        super(ZookeeperTestCase, self).tearDown()
        #zookeeper.set_log_stream(sys.stderr)  # reset to default
        #zookeeper.set_debug_level(zookeeper.LOG_LEVEL_DEBUG)
        self.log_file.close()

    def get_log(self):
        return open(self.log_file_path).read()


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
