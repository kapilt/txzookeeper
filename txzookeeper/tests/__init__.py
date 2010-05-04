import zookeeper
import sys

from twisted.trial.unittest import TestCase
from mocker import MockerTestCase

#from txzookeeper.client import Wrapper

#zookeeper = Wrapper(zookeeper)

class ZookeeperTestCase(TestCase, MockerTestCase):

    def setUp(self):
        super(ZookeeperTestCase, self).setUp()
        self.log_file_path = self.makeFile()
        self.log_file = open(self.log_file_path, 'w')
        zookeeper.set_log_stream(self.log_file)
        zookeeper.set_debug_level(zookeeper.LOG_LEVEL_ERROR)

    def tearDown(self):
        super(ZookeeperTestCase, self).tearDown()
        zookeeper.set_log_stream(sys.stderr) # reset to default
        #zookeeper.set_debug_level(zookeeper.LOG_LEVEL_INFO)
        self.log_file.close()

    def get_log(self):
        return open(self.log_file_path).read()
