from __future__ import absolute_import

# enum in stdlib as of py3.4
try:
    from enum import IntEnum  # pylint: disable=import-error
except ImportError:
    # vendored backport module
    from kafka.vendor.enum34 import IntEnum

class ACLResourceType(IntEnum):
    """An enumerated type of config resources"""

    ANY = 1,
    BROKER = 4,
    DELEGATION_TOKEN = 6,
    GROUP = 3,
    TOPIC = 2,
    TRANSACTIONAL_ID = 5

class ACLOperation(IntEnum):
    """An enumerated type of acl operations"""

    ANY = 1,
    ALL = 2,
    READ = 3,
    WRITE = 4,
    CREATE = 5,
    DELETE = 6,
    ALTER = 7,
    DESCRIBE = 8,
    CLUSTER_ACTION = 9,
    DESCRIBE_CONFIGS = 10,
    ALTER_CONFIGS = 11,
    IDEMPOTENT_WRITE = 12


class ACLPermissionType(IntEnum):
    """An enumerated type of permissions"""

    ANY = 1,
    DENY = 2,
    ALLOW = 3

class ACLResourcePatternType(IntEnum):
    """An enumerated type of resource patterns"""

    ANY = 1,
    MATCH = 2,
    LITERAL = 3,
    PREFIXED = 4

class ACLResource(object):
    """A class for specifying config resources.
    Arguments:
        resource_type (ConfigResourceType): the type of kafka resource
        name (string): The name of the kafka resource
        configs ({key : value}): A  maps of config keys to values.
    """

    def __init__(
            self,
            resource_type,
            operation,
            permission_type,
            name=None,
            principal=None,
            host=None,
            pattern_type=ACLResourcePatternType.LITERAL
    ):
        if not isinstance(resource_type, ACLResourceType):
            resource_type = ACLResourceType[str(resource_type).upper()]  # pylint: disable-msg=unsubscriptable-object
        self.resource_type = resource_type
        if not isinstance(operation, ACLOperation):
            operation = ACLOperation[str(operation).upper()]  # pylint: disable-msg:unsubscriptable-object
        self.operation = operation
        if not isinstance(permission_type, ACLPermissionType):
            permission_type = ACLPermissionType[str(permission_type).upper()]  # pylint: disable-msg=unsubscriptable-object
        self.permission_type = permission_type
        self.name = name
        self.principal = principal
        self.host = host
        if not isinstance(pattern_type, ACLResourcePatternType):
            pattern_type = ACLResourcePatternType[str(pattern_type).upper()]  # pylint: disable-msg=unsubscriptable-object
        self.pattern_type = pattern_type
