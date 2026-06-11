Install
#######

Package Install
***************

Releases are published to pypi and can be installed with pip (or uv, etc)

.. code:: bash

    pip install kafka-python

Releases are also listed at https://github.com/dpkp/kafka-python/releases


For latest bleed-edge code, install from github:

.. code:: bash

    pip install git+https://github.com/dpkp/kafka-python.git


Optional Installs
*****************

crc32c
======
Highly recommended for performance optimization. By default `kafka-python`
calculates record checksums in pure python, but the calculation is somewhat
CPU intensive. As throughput increases this can become a bottleneck. Installing
the optional ``crc32c`` dependency reduces the CPU cost of each check using
an optimized C library. See https://pypi.python.org/pypi/crc32c .

.. code:: bash

    pip install 'kafka-python[crc32c]'


zstd
====

To enable ZSTD compression/decompression, install `python-zstandard`:

>>> pip install 'kafka-python[zstd]'


lz4
===

To enable LZ4 compression/decompression, install `python-lz4`:

>>> pip install 'kafka-python[lz4]'


snappy
======

To enable Snappy compression/decompression, install `python-snappy`:

.. code:: bash

    pip install 'kafka-python[snappy]'


Note that python-snappy generally does not publish pre-compiled wheels,
so installation may require building the snappy library from source.
See https://google.github.io/snappy/ .
