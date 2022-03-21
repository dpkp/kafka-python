from __future__ import absolute_import

# enum in stdlib as of py3.4
try:
    from enum import IntEnum  # pylint: disable=import-error
except ImportError:
    # vendored backport module
    from kafka.vendor.enum34 import IntEnum


class CoordinatorType(IntEnum):
    """Type of coordinator key type"""

    GROUP = 0
    TRANSACTION = 1