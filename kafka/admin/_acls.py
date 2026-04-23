"""ACL management mixin for KafkaAdminClient.

Also defines ACL data types: ResourceType, ACLOperation, ACLPermissionType,
ACLResourcePatternType, ACLFilter, ACL, ResourcePatternFilter, ResourcePattern.
"""

from __future__ import annotations

import logging
from enum import IntEnum
from typing import TYPE_CHECKING

import kafka.errors as Errors
from kafka.errors import IllegalArgumentError
from kafka.protocol.admin import CreateAclsRequest, DeleteAclsRequest, DescribeAclsRequest

if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class ACLAdminMixin:
    """Mixin providing ACL management methods for KafkaAdminClient."""
    _manager: KafkaConnectionManager
    config: dict

    # ACL Helper for Metadata / DescribeGroups
    def _process_acl_operations(self, obj):
        if obj.get('authorized_operations', None) is not None:
            obj['authorized_operations'] = list(map(lambda acl: acl.name, valid_acl_operations(obj['authorized_operations'])))
        return obj

    def describe_acls(self, acl_filter):
        """Describe a set of ACLs

        Used to return a set of ACLs matching the supplied ACLFilter.
        The cluster must be configured with an authorizer for this to work, or
        you will get a SecurityDisabledError

        Arguments:
            acl_filter: an ACLFilter object

        Returns:
            tuple of a list of matching ACL objects and a KafkaError (NoError if successful)
        """
        min_version = 3 if acl_filter.resource_pattern.resource_type == ResourceType.USER else 0
        request = DescribeAclsRequest(
            min_version=min_version,
            resource_type_filter=acl_filter.resource_pattern.resource_type,
            resource_name_filter=acl_filter.resource_pattern.resource_name,
            pattern_type_filter=acl_filter.resource_pattern.pattern_type,
            principal_filter=acl_filter.principal,
            host_filter=acl_filter.host,
            operation=acl_filter.operation,
            permission_type=acl_filter.permission_type
        )
        response = self._manager.run(self._manager.send, request)
        return self._convert_describe_acls_response_to_acls(response)

    @staticmethod
    def _convert_describe_acls_response_to_acls(describe_response):
        """Convert a DescribeAclsResponse into a list of ACL objects and a KafkaError.

        Arguments:
            describe_response: The response object from the DescribeAclsRequest.

        Returns:
            A tuple of (list_of_acl_objects, error) where error is an instance
                 of KafkaError (NoError if successful).
        """
        error_type = Errors.for_code(describe_response.error_code)
        if error_type is not Errors.NoError:
            raise error_type(describe_response.error_message)
        acl_list = []
        for resource in describe_response.resources:
            for acl in resource.acls:
                acl_list.append(
                    ACL(
                        principal=acl.principal,
                        host=acl.host,
                        operation=ACLOperation(acl.operation),
                        permission_type=ACLPermissionType(acl.permission_type),
                        resource_pattern=ResourcePattern(
                            resource_type=ResourceType(resource.resource_type),
                            resource_name=resource.resource_name,
                            pattern_type=ACLResourcePatternType(resource.pattern_type))))
        return acl_list, Errors.NoError

    @staticmethod
    def _convert_create_acls_resource_request(acl):
        """Convert an ACL object into the CreateAclsRequest format."""
        _AclCreate = CreateAclsRequest.AclCreation
        return _AclCreate(
            resource_type=acl.resource_pattern.resource_type,
            resource_name=acl.resource_pattern.resource_name,
            resource_pattern_type=acl.resource_pattern.pattern_type,
            principal=acl.principal,
            host=acl.host,
            operation=acl.operation,
            permission_type=acl.permission_type
        )

    @staticmethod
    def _convert_create_acls_response_to_acls(acls, create_response):
        """Parse a CreateAclsResponse, returning a dict of successes and failures."""
        results = {'succeeded': [], 'failed': []}
        for i, result in enumerate(create_response.results):
            acl = acls[i]
            if result.error_code == 0:
                results['succeeded'].append(acl)
            else:
                results['failed'].append(Errors.for_code(result.error_code))
        return results

    def create_acls(self, acls):
        """Create a list of ACLs

        This endpoint only accepts a list of concrete ACL objects, no ACLFilters.
        Throws TopicAlreadyExistsError if topic is already present.

        Arguments:
            acls: a list of ACL objects

        Returns:
            dict of successes and failures
        """
        for acl in acls:
            if not isinstance(acl, ACL):
                raise IllegalArgumentError("acls must contain ACL objects")

        creations = [self._convert_create_acls_resource_request(acl) for acl in acls]
        min_version = 3 if any(creation.resource_type == ResourceType.USER for creation in creations) else 0
        request = CreateAclsRequest(creations=creations, min_version=min_version)
        response = self._manager.run(self._manager.send, request)
        return self._convert_create_acls_response_to_acls(acls, response)

    @staticmethod
    def _convert_delete_acls_resource_request(acl):
        """Convert an ACLFilter object into the DeleteAclsRequest format."""
        _AclsFilter = DeleteAclsRequest.DeleteAclsFilter
        return _AclsFilter(
            resource_type_filter=acl.resource_pattern.resource_type,
            resource_name_filter=acl.resource_pattern.resource_name,
            pattern_type_filter=acl.resource_pattern.pattern_type,
            principal_filter=acl.principal,
            host_filter=acl.host,
            operation=acl.operation,
            permission_type=acl.permission_type
        )

    @staticmethod
    def _convert_delete_acls_response_to_matching_acls(acl_filters, delete_response):
        """Parse a DeleteAclsResponse, returning a list of (filter, matched ACLs, error) tuples."""
        results = []
        for i, result in enumerate(delete_response.filter_results):
            acl_filter = acl_filters[i]
            error_type = Errors.for_code(result.error_code)
            matching_acls = []
            for acl in result.matching_acls:
                error = Errors.for_code(acl.error_code)
                matching_acls.append(
                    ACL(
                        principal=acl.principal,
                        host=acl.host,
                        operation=ACLOperation(acl.operation),
                        permission_type=ACLPermissionType(acl.permission_type),
                        resource_pattern=ResourcePattern(
                            resource_type=ResourceType(acl.resource_type),
                            resource_name=acl.resource_name,
                            pattern_type=ACLResourcePatternType(acl.pattern_type))))
            results.append((acl_filter, matching_acls, error_type))
        return results

    def delete_acls(self, acl_filters):
        """Delete a set of ACLs

        Deletes all ACLs matching the list of input ACLFilter

        Arguments:
            acl_filters: a list of ACLFilter

        Returns:
            a list of 3-tuples corresponding to the list of input filters.
                 The tuples hold (the input ACLFilter, list of affected ACLs, KafkaError instance)
        """
        for acl in acl_filters:
            if not isinstance(acl, ACLFilter):
                raise IllegalArgumentError("acl_filters must contain ACLFilter type objects")

        filters = [self._convert_delete_acls_resource_request(acl) for acl in acl_filters]
        min_version = 3 if any(_filter.resource_type_filter == ResourceType.USER for _filter in filters) else 0
        request = DeleteAclsRequest(filters=filters, min_version=min_version)
        response = self._manager.run(self._manager.send, request)
        return self._convert_delete_acls_response_to_matching_acls(acl_filters, response)


# ---------------------------------------------------------------------------
# ACL data types
# ---------------------------------------------------------------------------


class ResourceType(IntEnum):
    """Type of kafka resource to set ACL for.

    The ANY value is only valid in a filter context.
    """
    UNKNOWN = 0
    ANY = 1
    TOPIC = 2
    GROUP = 3
    CLUSTER = 4
    TRANSACTIONAL_ID = 5
    DELEGATION_TOKEN = 6
    USER = 7


class ACLOperation(IntEnum):
    """Type of operation.

    The ANY value is only valid in a filter context.
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
    """An enumerated type of permissions.

    The ANY value is only valid in a filter context.
    """
    UNKNOWN = 0
    ANY = 1
    DENY = 2
    ALLOW = 3


class ACLResourcePatternType(IntEnum):
    """An enumerated type of resource patterns.

    More details on the pattern types and how they work
    can be found in KIP-290 (Support for prefixed ACLs).
    """
    UNKNOWN = 0
    ANY = 1
    MATCH = 2
    LITERAL = 3
    PREFIXED = 4


class ResourcePatternFilter:
    def __init__(self, resource_type, resource_name, pattern_type):
        self.resource_type = resource_type
        self.resource_name = resource_name
        self.pattern_type = pattern_type
        self.validate()

    def validate(self):
        if not isinstance(self.resource_type, ResourceType):
            raise IllegalArgumentError("resource_type must be a ResourceType object")
        if not isinstance(self.pattern_type, ACLResourcePatternType):
            raise IllegalArgumentError("pattern_type must be an ACLResourcePatternType object")

    def __repr__(self):
        return "<ResourcePattern type={}, name={}, pattern={}>".format(
            self.resource_type.name, self.resource_name, self.pattern_type.name)

    def __eq__(self, other):
        return all((
            self.resource_type == other.resource_type,
            self.resource_name == other.resource_name,
            self.pattern_type == other.pattern_type,
        ))

    def __hash__(self):
        return hash((self.resource_type, self.resource_name, self.pattern_type))


class ResourcePattern(ResourcePatternFilter):
    """A resource pattern to apply the ACL to."""
    def __init__(self, resource_type, resource_name, pattern_type=ACLResourcePatternType.LITERAL):
        super().__init__(resource_type, resource_name, pattern_type)
        self.validate()

    def validate(self):
        if self.resource_type == ResourceType.ANY:
            raise IllegalArgumentError("resource_type cannot be ANY")
        if self.pattern_type in [ACLResourcePatternType.ANY, ACLResourcePatternType.MATCH]:
            raise IllegalArgumentError(
                "pattern_type cannot be {} on a concrete ResourcePattern".format(self.pattern_type.name))


class ACLFilter:
    """Represents a filter to use with describing and deleting ACLs."""
    def __init__(self, principal, host, operation, permission_type, resource_pattern):
        self.principal = principal
        self.host = host
        self.operation = operation
        self.permission_type = permission_type
        self.resource_pattern = resource_pattern
        self.validate()

    def validate(self):
        if not isinstance(self.operation, ACLOperation):
            raise IllegalArgumentError("operation must be an ACLOperation object, and cannot be ANY")
        if not isinstance(self.permission_type, ACLPermissionType):
            raise IllegalArgumentError("permission_type must be an ACLPermissionType object, and cannot be ANY")
        if not isinstance(self.resource_pattern, ResourcePatternFilter):
            raise IllegalArgumentError("resource_pattern must be a ResourcePatternFilter object")

    def __repr__(self):
        return "<ACL principal={principal}, resource={resource}, operation={operation}, type={type}, host={host}>".format(
            principal=self.principal, host=self.host, operation=self.operation.name,
            type=self.permission_type.name, resource=self.resource_pattern)

    def __eq__(self, other):
        return all((
            self.principal == other.principal, self.host == other.host,
            self.operation == other.operation, self.permission_type == other.permission_type,
            self.resource_pattern == other.resource_pattern))

    def __hash__(self):
        return hash((self.principal, self.host, self.operation, self.permission_type, self.resource_pattern))


class ACL(ACLFilter):
    """Represents a concrete ACL for a specific ResourcePattern."""
    def __init__(self, principal, host, operation, permission_type, resource_pattern):
        super().__init__(principal, host, operation, permission_type, resource_pattern)
        self.validate()

    def validate(self):
        if self.operation == ACLOperation.ANY:
            raise IllegalArgumentError("operation cannot be ANY")
        if self.permission_type == ACLPermissionType.ANY:
            raise IllegalArgumentError("permission_type cannot be ANY")
        if not isinstance(self.resource_pattern, ResourcePattern):
            raise IllegalArgumentError("resource_pattern must be a ResourcePattern object")


def valid_acl_operations(int_vals):
    return set([ACLOperation(v) for v in int_vals if v not in (0, 1, 2)])
