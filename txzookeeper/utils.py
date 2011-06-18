import zookeeper
from twisted.internet.defer import inlineCallbacks


@inlineCallbacks
def retry_change(client, path, change_function):
    """
    A utility function to execute a node change function, repeatedly
    in the face of transient errors. The node at 'path's content will
    be changed by the 'change_function' which is passed the node's
    current content and node stat. The output will be used to replace
    the node's content. If the new content is identical to the previous
    new content, no changes are made. Automatically performs, retries
    in the face of errors.

    @param client A connected txzookeeper client

    @param path A path to a node that will be modified

    @param change_function A python function that will receive two parameters
           the node_content and the current node stat, and will return the
           new node content. The function must not have side-effects as
           it will be called again in the event of various error conditions.
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
