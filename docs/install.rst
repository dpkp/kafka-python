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


Optional LZ4 install
********************

To enable LZ4 compression/decompression, install python-lz4:

>>> pip install lz4


Optional Snappy install
***********************

Install Development Libraries
=============================

Download and build Snappy from http://code.google.com/p/snappy/downloads/list

Ubuntu:

.. code:: bash

    apt-get install libsnappy-dev

OSX:

.. code:: bash

    brew install snappy

From Source:

.. code:: bash

    wget http://snappy.googlecode.com/files/snappy-1.0.5.tar.gz
    tar xzvf snappy-1.0.5.tar.gz
    cd snappy-1.0.5
    ./configure
    make
    sudo make install

Install Python Module
=====================

Install the `python-snappy` module

.. code:: bash

    pip install python-snappy
