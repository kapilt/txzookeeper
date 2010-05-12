import zookeeper


def deleteTree(path="/", handle=1):
    """
    Destroy all the nodes in zookeeper (typically under a chroot for testing)
    """
    for child in zookeeper.get_children(handle, path):
        if child == "zookeeper": # skip the metadata node
            continue
        child_path = "/"+("%s/%s"%(path, child)).strip("/")
        try:
            deleteTree(child_path, handle)
            zookeeper.delete(handle, child_path, -1)
        except zookeeper.ZooKeeperException, e:
            print "Error on path", child_path, e
