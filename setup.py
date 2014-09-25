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

    @classmethod
    def run(cls):
        import tox
        sys.exit(tox.cmdline([]))


test_require = ['tox', 'mock']
if sys.version_info < (2, 7):
    test_require.append('unittest2')


setup(
    name="kafka-python",
    version=__version__,

    tests_require=test_require,
    cmdclass={"test": Tox},

    packages=[
        "kafka",
        "kafka.consumer",
        "kafka.partitioner",
        "kafka.producer",
    ],

    author="David Arthur",
    author_email="mumrah@gmail.com",
    url="https://github.com/mumrah/kafka-python",
    license="Apache License 2.0",
    description="Pure Python client for Apache Kafka",
    long_description="""
This module provides low-level protocol support for Apache Kafka as well as
high-level consumer and producer classes. Request batching is supported by the
protocol as well as broker-aware request routing. Gzip and Snappy compression
is also supported for message sets.
""",
    keywords="apache kafka",
    install_requires=['six'],
    classifiers = [
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ]
)
