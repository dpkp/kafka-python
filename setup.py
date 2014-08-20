import sys

from setuptools import setup, Command

with open('VERSION', 'r') as v:
    __version__ = v.read().rstrip()

class Tox(Command):

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import tox
        sys.exit(tox.cmdline([]))


requires = [
    'kazoo>=2.0',
]

install_requires = requires + [
]

test_requires = requires + [
    "tox", 
    "mock",
]

setup(
    name="kafka-python-lf",
    version=__version__,
    install_requires=install_requires,
    tests_require=test_requires,
    cmdclass={"test": Tox},
    packages=["kafka"],
    author="David Arthur",
    author_email="mumrah@gmail.com",
    url="https://github.com/mumrah/kafka-python",
    license="Copyright 2012, David Arthur under Apache License, v2.0",
    description="Pure Python client for Apache Kafka",
    long_description="""
This module provides low-level protocol support for Apache Kafka as well as
high-level consumer and producer classes. Request batching is supported by the
protocol as well as broker-aware request routing. Gzip and Snappy compression
is also supported for message sets.
"""
)
