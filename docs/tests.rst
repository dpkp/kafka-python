Tests
=====

.. image:: https://coveralls.io/repos/dpkp/kafka-python/badge.svg?branch=master&service=github
    :target: https://coveralls.io/github/dpkp/kafka-python?branch=master
.. image:: https://travis-ci.org/dpkp/kafka-python.svg?branch=master
    :target: https://travis-ci.org/dpkp/kafka-python

Test environments are managed via tox. The test suite is run via pytest.

Linting is run via pylint, but is generally skipped on pypy due to pylint
compatibility / performance issues.

For test coverage details, see https://coveralls.io/github/dpkp/kafka-python

The test suite includes unit tests that mock network interfaces, as well as
integration tests that setup and teardown kafka broker (and zookeeper)
fixtures for client / consumer / producer testing.


Unit tests
------------------

To run the tests locally, install tox:

.. code:: bash

     pip install tox

For more details, see https://tox.readthedocs.io/en/latest/install.html

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

    KAFKA_VERSION=0.8.2.2 tox -e py27
    KAFKA_VERSION=1.0.1 tox -e py36


Integration tests start Kafka and Zookeeper fixtures. This requires downloading
kafka server binaries:

.. code:: bash

    ./build_integration.sh

By default, this will install the broker versions listed in build_integration.sh's `ALL_RELEASES`
into the servers/ directory. To install a specific version, set the `KAFKA_VERSION` variable:

.. code:: bash

    KAFKA_VERSION=1.0.1 ./build_integration.sh

Then to run the tests against a specific Kafka version, simply set the `KAFKA_VERSION`
env variable to the server build you want to use for testing:

.. code:: bash

    KAFKA_VERSION=1.0.1 tox -e py36

To test against the kafka source tree, set KAFKA_VERSION=trunk
[optionally set SCALA_VERSION (defaults to the value set in `build_integration.sh`)]

.. code:: bash

    SCALA_VERSION=2.12 KAFKA_VERSION=trunk ./build_integration.sh
    KAFKA_VERSION=trunk tox -e py36
