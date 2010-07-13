from setuptools import find_packages, setup


from txzookeeper import version


long_description = """
Twisted API for Apache Zookeeper. Includes a distributed lock, and several
queue implementations.
"""

setup(
    name="txzookeeper",
    version=version,
    description="Twisted api for Apache Zookeeper",
    author="txZooKeeper Developers",
    author_email="txzookeeper@lists.launchpad.net",
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
