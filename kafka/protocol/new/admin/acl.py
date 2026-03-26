from enum import IntEnum

from ..api_message import ApiMessage


class CreateAclsRequest(ApiMessage): pass
class CreateAclsResponse(ApiMessage): pass

class DeleteAclsRequest(ApiMessage): pass
class DeleteAclsResponse(ApiMessage): pass

class DescribeAclsRequest(ApiMessage): pass
class DescribeAclsResponse(ApiMessage): pass


class ACLResourceType(IntEnum):
    """Type of kafka resource to set ACL for

    The ANY value is only valid in a filter context
    """
    UNKNOWN = 0
    ANY = 1
    TOPIC = 2
    GROUP = 3
    CLUSTER = 4
    TRANSACTIONAL_ID = 5
    DELEGATION_TOKEN = 6


class ACLOperation(IntEnum):
    """Type of operation

    The ANY value is only valid in a filter context
    """
    UNKNOWN = 0
    ANY = 1
    ALL = 2
    READ = 3
    WRITE = 4
    CREATE = 5
    DELETE = 6
    ALTER = 7
    DESCRIBE = 8
    CLUSTER_ACTION = 9
    DESCRIBE_CONFIGS = 10
    ALTER_CONFIGS = 11
    IDEMPOTENT_WRITE = 12
    CREATE_TOKENS = 13
    DESCRIBE_TOKENS = 14


class ACLPermissionType(IntEnum):
    """An enumerated type of permissions

    The ANY value is only valid in a filter context
    """
    UNKNOWN = 0
    ANY = 1
    DENY = 2
    ALLOW = 3


class ACLResourcePatternType(IntEnum):
    """An enumerated type of resource patterns

    More details on the pattern types and how they work
    can be found in KIP-290 (Support for prefixed ACLs)
    https://cwiki.apache.org/confluence/display/KAFKA/KIP-290%3A+Support+for+Prefixed+ACLs
    """
    UNKNOWN = 0
    ANY = 1
    MATCH = 2
    LITERAL = 3
    PREFIXED = 4


__all__ = [
    'CreateAclsRequest', 'CreateAclsResponse',
    'DeleteAclsRequest', 'DeleteAclsResponse',
    'DescribeAclsRequest', 'DescribeAclsResponse',
    'ACLResourceType', 'ACLOperation',
    'ACLPermissionType', 'ACLResourcePatternType',
]
