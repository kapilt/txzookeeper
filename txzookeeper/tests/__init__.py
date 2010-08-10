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
        zookeeper.set_log_stream(sys.stderr) # reset to default
        zookeeper.set_debug_level(zookeeper.LOG_LEVEL_DEBUG)
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
