import zookeeper
from twisted.internet.defer import inlineCallbacks


@inlineCallbacks
def retry_change(client, path, change_function):
    """
    A utility function to execute a node change function, repeatedly
    in the face of transient errors.

    @param client A connected txzookeeper client

    @param path A path to a node that will be modified

    @param change_function A python function that will receive two parameters
           the node_content and the current node stat, and will return the
           new node content. The function must not have side-effects as
           it will be called again in the event of various error conditions.
           ie. it must be idempotent.
    """

    while True:
        create_mode = False

        try:
            content, stat = yield client.get(path)
        except zookeeper.NoNodeException:
            create_mode = True
            content, stat = None, None

        new_content = yield change_function(content, stat)
        if new_content == content:
            break

        try:
            if create_mode:
                yield client.create(path, new_content)
            else:
                yield client.set(path, new_content, version=stat["version"])
            break
        except (zookeeper.NodeExistsException,
                zookeeper.NoNodeException,
                zookeeper.BadVersionException):
            pass
