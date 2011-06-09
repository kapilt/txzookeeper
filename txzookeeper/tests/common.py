import os
import os.path
import shutil
import subprocess
import tempfile

from contextlib import contextmanager
from glob import glob


__all__ = ("ManagedZooKeeper",
           "zookeeper_test_context",
           "get_zookeeper_test_address")


class ManagedZooKeeper(object):
    """Class to manage the running of a ZooKeeper instance for testing.

    Note: no attempt is made to probe the ZooKeeper instance is
    actually available, or that the selected port is free. In the
    future, we may want to do that, especially when run in a
    Hudson/Buildbot context, to ensure more test robustness."""

    def __init__(self, install_path, port=28181, peers=None):
        """Define the ZooKeeper test instance.

        @param install_path: The path to the install for ZK
        @param port: The port to run the managed ZK instance
        """

        self.install_path = install_path
        self.port = port
        self.host = "127.0.0.1"
        self.peers = None

    def run(self):
        """Run the ZooKeeper instance under a temporary directory.

        Writes ZK log messages to zookeeper.log in the current directory.
        """
        self.working_path = tempfile.mkdtemp()
        config_path = os.path.join(self.working_path, "zoo.cfg")
        log_path = os.path.join(self.working_path, "log")
        log4j_path = os.path.join(self.working_path, "log4j.properties")
        data_path = os.path.join(self.working_path, "data")

        # various setup steps
        os.mkdir(log_path)

        with open(config_path, "w") as config:
            config.write("""
tickTime=2000
dataDir=%s
clientPort=%s
maxClientCnxns=0
""" % (data_path, self.port))

        # setup a replicated setup if peers are specified
        if self.peers:
            server_id = len(self.peers) + 1
            peers_cfg = ["%s:%s:%s" % p for p in self.peers]
            peer_entry = "server." + str(server_id) + "=localhost:%d:%d" % (
                2000 + server_id, 3000 + server_id)
            peers_cfg.append("%s:%s:%s" % (peer_entry))

            peers_cfg = "\n".join(self.peers)

            with open(config_path, "a") as config:
                config.write("""
initLimit=4
syncLimit=2
%s
""" % peers_cfg)
            # Write server ids into datadir

            with open(os.path.join(data_path, "myid"), "w") as myid_file:
                myid_file.write(str(server_id))

        with open(log4j_path, "w") as log4j:
            log4j.write("""
# DEFAULT: console appender only
log4j.rootLogger=INFO, ROLLINGFILE
log4j.appender.ROLLINGFILE.layout=org.apache.log4j.PatternLayout
log4j.appender.ROLLINGFILE.layout.ConversionPattern=%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n
log4j.appender.ROLLINGFILE=org.apache.log4j.RollingFileAppender
log4j.appender.ROLLINGFILE.Threshold=DEBUG
log4j.appender.ROLLINGFILE.File=zookeeper.log
""")

        self.process = subprocess.Popen(
            args=["java",
                  "-cp", self.classpath,
                  "-Dzookeeper.log.dir=%s" % log_path,
                  "-Dzookeeper.root.logger=INFO,CONSOLE",
                  "-Dlog4j.configuration=file:%s" % log4j_path,
                  # "-Dlog4j.debug",
                  "org.apache.zookeeper.server.quorum.QuorumPeerMain",
                  config_path],
            )

    @property
    def classpath(self):
        """Get the classpath necessary to run ZooKeeper."""

        # Two possibilities, as seen in zkEnv.sh:
        # Check for a release - top-level zookeeper-*.jar?
        jars = glob((os.path.join(
            self.install_path, 'zookeeper-*.jar')))
        if jars:
            # Relase build (`ant package`)
            jars.extend(glob(os.path.join(
                self.install_path,
                "lib/*.jar")))
        else:
            # Development build (plain `ant`)
            jars = glob((os.path.join(
                self.install_path, 'build/zookeeper-*.jar')))
            jars.extend(glob(os.path.join(
                self.install_path,
                "build/lib/*.jar")))
        return ":".join(jars)

    @property
    def address(self):
        """Get the address of the ZooKeeper instance."""
        return "%s:%s" % (self.host, self.port)

    def stop(self):
        """Stop the ZooKeeper instance and cleanup."""
        self.process.terminate()
        self.process.wait()
        shutil.rmtree(self.working_path)


"""Global to manage the ZK test address - only for testing of course!"""
_zookeeper_address = "127.0.0.1:2181"


def get_test_zookeeper_address():
    """Get the current test ZK address, such as '127.0.0.1:2181'"""
    return _zookeeper_address


@contextmanager
def zookeeper_test_context(install_path, port=28181):
    """Manage context to run/stop a ZooKeeper for testing and related vars.

    @param install_path: The path to the install for ZK
    @param port: The port to run the managed ZK instance
    """
    global _zookeeper_address

    saved_zookeeper_address = _zookeeper_address
    saved_env = os.environ.get("ZOOKEEPER_ADDRESS")
    test_zookeeper = ManagedZooKeeper(install_path, port)
    test_zookeeper.run()
    os.environ["ZOOKEEPER_ADDRESS"] = test_zookeeper.address
    _zookeeper_address = test_zookeeper.address
    try:
        yield test_zookeeper
    finally:
        test_zookeeper.stop()
        _zookeeper_address = saved_zookeeper_address
        if saved_env:
            os.environ["ZOOKEEPER_ADDRESS"] = saved_env
        else:
            del os.environ["ZOOKEEPER_ADDRESS"]
