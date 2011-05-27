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
    author="Ensemble Developers",
    author_email="ensemble@lists.ubuntu.com",
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
        "License :: OSI Approved :: MIT License",
       ],
    )
