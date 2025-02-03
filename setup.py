import os
import sys

from setuptools import setup, Command, find_packages

# Pull version from source without importing
# since we can't import something we haven't built yet :)


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
    name="kafka-python-ng",
    python_requires=">=3.8",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    tests_require=test_require,
    extras_require={
        "crc32c": ["crc32c"],
        "lz4": ["lz4"],
        "snappy": ["python-snappy"],
        "zstd": ["zstandard"],
        "boto": ["botocore"],
    },
    cmdclass={"test": Tox},
    packages=find_packages(exclude=['test']),
    author="Dana Powers",
    author_email="dana.powers@gmail.com",
    maintainer="William Barnhart",
    maintainer_email="williambbarnhart@gmail.com",
    url="https://github.com/wbarnha/kafka-python-ng",
    license="Apache License 2.0",
    description="Pure Python client for Apache Kafka",
    long_description=README,
    keywords=[
        "apache kafka",
        "kafka",
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ]
)
