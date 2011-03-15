from setuptools import find_packages, setup

long_description = """
Twisted API for Apache Zookeeper. Includes a distributed lock, and several
queue implementations.
"""

setup(
    name="txzookeeper",
    version="0.2.1",
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
