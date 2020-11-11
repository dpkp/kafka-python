Compatibility
-------------

.. image:: https://img.shields.io/badge/kafka-2.6%2C%202.5%2C%202.4%2C%202.3%2C%202.2%2C%202.1%2C%202.0%2C%201.1%2C%201.0%2C%200.11%2C%200.10%2C%200.9%2C%200.8-brightgreen.svg
    :target: https://kafka-python.readthedocs.io/compatibility.html
.. image:: https://img.shields.io/pypi/pyversions/kafka-python.svg
    :target: https://pypi.python.org/pypi/kafka-python

kafka-python is compatible with (and tested against) broker versions 2.6
through 0.8.0 . kafka-python is not compatible with the 0.8.2-beta release.

Because the kafka server protocol is backwards compatible, kafka-python is
expected to work with newer broker releases as well.

Although kafka-python is tested and expected to work on recent broker versions,
not all features are supported. Specifically, authentication codecs, and
transactional producer/consumer support are not fully implemented. PRs welcome!

kafka-python is tested on python 2.7, 3.4, 3.7, 3.8 and pypy2.7.

Builds and tests via Travis-CI.  See https://travis-ci.org/dpkp/kafka-python
