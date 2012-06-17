#
#  Copyright (C) 2010, 2011 Canonical Ltd. All Rights Reserved
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
#  Please see the files COPYING and COPYING.LESSER for the text of the
#  License.
#

import sys, re

from setuptools import find_packages, setup

long_description = """
Twisted API for Apache Zookeeper. Includes a distributed lock, and several
queue implementations.
"""

# Parse directly to avoid build-time dependencies on zookeeper, twisted, etc.
for line in open("txzookeeper/__init__.py"):
    m = re.match('version = "(.*)"', line)
    if m:
        version = m.group(1)
        break
else:
    sys.exit("error: can't find version information")

setup(
    name="txzookeeper",
    version=version,
    description="Twisted api for Apache Zookeeper",
    author="Juju Developers",
    author_email="juju@lists.ubuntu.com",
    url="https://launchpad.net/txzookeeper",
    license="LGPL",
    packages=find_packages(),
    test_suite="txzookeeper.tests.egg_test_runner",
    long_description=long_description,
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Information Technology",
        "Programming Language :: Python",
        "Topic :: Database",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)"
       ],
    )
