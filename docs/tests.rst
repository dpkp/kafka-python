Tests
=====

.. image:: https://coveralls.io/repos/dpkp/kafka-python/badge.svg?branch=master&service=github
    :target: https://coveralls.io/github/dpkp/kafka-python?branch=master
.. image:: https://travis-ci.org/dpkp/kafka-python.svg?branch=master
    :target: https://travis-ci.org/dpkp/kafka-python

Test environments are managed via tox. The test suite is run via pytest.
Individual tests are written using unittest, pytest, and in some cases,
doctest.

Linting is run via pylint, but is generally skipped on python2.6 and pypy
due to pylint compatibility / performance issues.

For test coverage details, see https://coveralls.io/github/dpkp/kafka-python

The test suite includes unit tests that mock network interfaces, as well as
integration tests that setup and teardown kafka broker (and zookeeper)
fixtures for client / consumer / producer testing.


Unit tests
------------------

To run the tests locally, install tox -- `pip install tox`
See http://tox.readthedocs.org/en/latest/install.html

Then simply run tox, optionally setting the python environment.
If unset, tox will loop through all environments.

.. code:: bash

    tox -e py27
    tox -e py35

    # run protocol tests only
    tox -- -v test.test_protocol

    # re-run the last failing test, dropping into pdb
    tox -e py27 -- --lf --pdb

    # see available (pytest) options
    tox -e py27 -- --help


Integration tests
-----------------

.. code:: bash

    KAFKA_VERSION=0.10.0.0 tox -e py27
    KAFKA_VERSION=0.8.2.2 tox -e py35


Integration tests start Kafka and Zookeeper fixtures. This requires downloading
kafka server binaries:

.. code:: bash

    ./build_integration.sh

By default, this will install 0.8.1.1, 0.8.2.2, 0.9.0.1, and 0.10.0.0 brokers into the
servers/ directory. To install a specific version, set `KAFKA_VERSION=1.2.3`:

.. code:: bash

    KAFKA_VERSION=0.8.0 ./build_integration.sh

Then run the tests against supported Kafka versions, simply set the `KAFKA_VERSION`
env variable to the server build you want to use for testing:

.. code:: bash

    KAFKA_VERSION=0.9.0.1 tox -e py27

To test against the kafka source tree, set KAFKA_VERSION=trunk
[optionally set SCALA_VERSION (defaults to 2.10)]

.. code:: bash

    SCALA_VERSION=2.11 KAFKA_VERSION=trunk ./build_integration.sh
    KAFKA_VERSION=trunk tox -e py35
