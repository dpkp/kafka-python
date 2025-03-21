Tests
=====

.. image:: https://coveralls.io/repos/dpkp/kafka-python/badge.svg?branch=master&service=github
    :target: https://coveralls.io/github/dpkp/kafka-python?branch=master
.. image:: https://travis-ci.org/dpkp/kafka-python.svg?branch=master
    :target: https://travis-ci.org/dpkp/kafka-python

The test suite is run via pytest.

Linting is run via pylint, but is currently skipped during CI/CD due to
accumulated debt. We'd like to transition to ruff!

For test coverage details, see https://coveralls.io/github/dpkp/kafka-python
Coverage reporting is currently disabled as we have transitioned from travis
to GH Actions and have not yet re-enabled coveralls integration.

The test suite includes unit tests that mock network interfaces, as well as
integration tests that setup and teardown kafka broker (and zookeeper)
fixtures for client / consumer / producer testing.


Unit tests
------------------

To run the tests locally, install test dependencies:

.. code:: bash

     pip install -r requirements-dev.txt

Then simply run pytest (or make test) from your preferred python + virtualenv.

.. code:: bash

    # run protocol tests only (via pytest)
    pytest test/test_protocol.py

    # Run conn tests only (via make)
    PYTESTS=test/test_conn.py make test


Integration tests
-----------------

.. code:: bash

    KAFKA_VERSION=4.0.0 make test


Integration tests start Kafka and Zookeeper fixtures. Make will download
kafka server binaries automatically if needed.
