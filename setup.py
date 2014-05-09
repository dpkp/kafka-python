import sys

from setuptools import setup, Command


class Tox(Command):

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import tox
        sys.exit(tox.cmdline([]))


setup(
    name="kafka-python",
    version="0.9.1",

    tests_require=["tox", "mock"],
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
