Install
#######

Install with your favorite package manager

Latest Release
**************
Pip:

.. code:: bash

    pip install kafka-python

Releases are also listed at https://github.com/dpkp/kafka-python/releases


Bleeding-Edge
*************

.. code:: bash

    git clone https://github.com/dpkp/kafka-python
    pip install ./kafka-python


Optional crc32c install
***********************
Highly recommended if you are using Kafka 11+ brokers. For those `kafka-python`
uses a new message protocol version, that requires calculation of `crc32c`,
which differs from the `zlib.crc32` hash implementation. By default `kafka-python`
calculates it in pure python, which is quite slow. To speed it up we optionally
support https://pypi.python.org/pypi/crc32c package if it's installed.

.. code:: bash

    pip install 'kafka-python[crc32c]'


Optional ZSTD install
********************

To enable ZSTD compression/decompression, install python-zstandard:

>>> pip install 'kafka-python[zstd]'


Optional LZ4 install
********************

To enable LZ4 compression/decompression, install python-lz4:

>>> pip install 'kafka-python[lz4]'


Optional Snappy install
***********************

Install Development Libraries
=============================

Download and build Snappy from https://google.github.io/snappy/

Ubuntu:

.. code:: bash

    apt-get install libsnappy-dev

OSX:

.. code:: bash

    brew install snappy

From Source:

.. code:: bash

    wget https://github.com/google/snappy/releases/download/1.1.3/snappy-1.1.3.tar.gz
    tar xzvf snappy-1.1.3.tar.gz
    cd snappy-1.1.3
    ./configure
    make
    sudo make install

Install Python Module
=====================

Install the `python-snappy` module

.. code:: bash

    pip install 'kafka-python[snappy]'
