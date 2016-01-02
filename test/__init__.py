import sys

if sys.version_info < (2, 7):
    import unittest2 as unittest  # pylint: disable=import-error
else:
    import unittest
