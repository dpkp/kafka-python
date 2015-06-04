import sys
import os
from setuptools import setup, Command

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
if sys.version_info < (2, 7):
    test_require.append('unittest2')

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

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
    long_description=README,
    keywords="apache kafka",
    install_requires=['six'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ]
)
