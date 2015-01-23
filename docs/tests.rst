Tests
=====

Run the unit tests
------------------

.. code:: bash

    tox


Run a subset of unit tests
--------------------------

.. code:: bash

    # run protocol tests only
    tox -- -v test.test_protocol

    # test with pypy only
    tox -e pypy

    # Run only 1 test, and use python 2.7
    tox -e py27 -- -v --with-id --collect-only

    # pick a test number from the list like #102
    tox -e py27 -- -v --with-id 102


Run the integration tests
-------------------------

The integration tests will actually start up real local Zookeeper
instance and Kafka brokers, and send messages in using the client.

First, get the kafka binaries for integration testing:

.. code:: bash

    ./build_integration.sh

By default, the build_integration.sh script will download binary
distributions for all supported kafka versions.
To test against the latest source build, set KAFKA_VERSION=trunk
and optionally set SCALA_VERSION (defaults to 2.8.0, but 2.10.1 is recommended)

.. code:: bash

    SCALA_VERSION=2.10.1 KAFKA_VERSION=trunk ./build_integration.sh

Then run the tests against supported Kafka versions, simply set the `KAFKA_VERSION`
env variable to the server build you want to use for testing:

.. code:: bash

    KAFKA_VERSION=0.8.0 tox
    KAFKA_VERSION=0.8.1 tox
    KAFKA_VERSION=0.8.1.1 tox
    KAFKA_VERSION=trunk tox
