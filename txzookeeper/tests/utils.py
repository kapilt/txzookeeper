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


def deleteTree(path="/", handle=1):
    """
    Destroy all the nodes in zookeeper (typically under a chroot for testing)
    """
    for child in zookeeper.get_children(handle, path):
        if child == "zookeeper":  # skip the metadata node
            continue
        child_path = "/" + ("%s/%s" % (path, child)).strip("/")
        try:
            deleteTree(child_path, handle)
            zookeeper.delete(handle, child_path, -1)
        except zookeeper.ZooKeeperException, e:
            print "Error on path", child_path, e
