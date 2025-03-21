Compatibility
-------------

.. image:: https://img.shields.io/badge/kafka-4.0--0.8-brightgreen.svg
    :target: https://kafka-python.readthedocs.io/compatibility.html
.. image:: https://img.shields.io/pypi/pyversions/kafka-python.svg
    :target: https://pypi.python.org/pypi/kafka-python

kafka-python is compatible with (and tested against) broker versions 4.0
through 0.8.0 . kafka-python is not compatible with the 0.8.2-beta release.

Because the kafka server protocol is backwards compatible, kafka-python is
expected to work with newer broker releases as well.

Although kafka-python is tested and expected to work on recent broker versions,
not all features are supported. Specifically, transactional producer/consumer
support is not fully implemented. PRs welcome!

kafka-python is tested on python 2.7, and 3.8-3.13.

Builds and tests via Github Actions Workflows.  See https://github.com/dpkp/kafka-python/actions
