Tests
=====

.. image:: https://img.shields.io/github/actions/workflow/status/dpkp/kafka-python/python-package.yml
    :target: https://github.com/dpkp/kafka-python/actions/workflows/python-package.yml

The test suite is run via pytest.

Linting is run via pylint.

Test coverage details are currently published as an html build artifact.

The test suite includes unit tests that mock network interfaces, mock broker tests
that simulate request/receive network messaging, as well as integration tests that
setup and teardown kafka broker (and zookeeper where required) fixtures.


Unit tests
------------------

To run the tests locally, install test dependencies:

.. code:: bash

     pip install -r requirements-dev.txt

Then simply run pytest (or make test) from your preferred python + virtualenv.

.. code:: bash

    # run protocol tests only (via pytest)
    pytest test/protocol/

    # Run connection tests only (via make)
    PYTESTS=test/net/test_connection.py make test


Integration tests
-----------------

.. code:: bash

    # Download new broker files
    KAFKA_VERSION=4.3.0 make servers/4.3.0/kafka-bin
    # Run tests for previously-installed broker version
    KAFKA_VERSION=4.3.0 pytest -v test/integration/
    # Or install + run all tests
    KAFKA_VERSION=4.3.0 make test


Integration tests start Kafka (and Zookeeper where required) fixtures. These
require a functioning java install. Make will download the kafka server binaries
automatically if needed.
