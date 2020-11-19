import os
import sys

from setuptools import setup, Command, find_packages

# Pull version from source without importing
# since we can't import something we haven't built yet :)
exec(open('kafka/version.py').read())


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

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

setup(
    name="kafka-python",
    version=__version__,

    tests_require=test_require,
    extras_require={
        "crc32c": ["crc32c"],
        "lz4": ["lz4"],
        "snappy": ["python-snappy"],
        "zstd": ["python-zstandard"],
    },
    cmdclass={"test": Tox},
    packages=find_packages(exclude=['test']),
    author="Dana Powers",
    author_email="dana.powers@gmail.com",
    url="https://github.com/dpkp/kafka-python",
    license="Apache License 2.0",
    description="Pure Python client for Apache Kafka",
    long_description=README,
    keywords="apache kafka",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ]
)
