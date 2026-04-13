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


# ---------------------------------------------------------------------------
# ACL data types
# ---------------------------------------------------------------------------


class ResourceType(IntEnum):
    """Type of kafka resource to set ACL for.

    The ANY value is only valid in a filter context.
    """
    UNKNOWN = 0,
    ANY = 1,
    CLUSTER = 4,
    DELEGATION_TOKEN = 6,
    GROUP = 3,
    TOPIC = 2,
    TRANSACTIONAL_ID = 5


class ACLOperation(IntEnum):
    """Type of operation.

    The ANY value is only valid in a filter context.
    """
    UNKNOWN = 0,
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
    IDEMPOTENT_WRITE = 12,
    CREATE_TOKENS = 13,
    DESCRIBE_TOKENS = 14


class ACLPermissionType(IntEnum):
    """An enumerated type of permissions.

    The ANY value is only valid in a filter context.
    """
    UNKNOWN = 0,
    ANY = 1,
    DENY = 2,
    ALLOW = 3


class ACLResourcePatternType(IntEnum):
    """An enumerated type of resource patterns.

    More details on the pattern types and how they work
    can be found in KIP-290 (Support for prefixed ACLs).
    """
    UNKNOWN = 0,
    ANY = 1,
    MATCH = 2,
    LITERAL = 3,
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

if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class ACLAdminMixin:
    """Mixin providing ACL management methods for KafkaAdminClient."""
    _manager: KafkaConnectionManager
    _client: object
    config: dict

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

        version = self._client.api_version(DescribeAclsRequest, max_version=1)
        if version == 0:
            request = DescribeAclsRequest[version](
                resource_type_filter=acl_filter.resource_pattern.resource_type,
                resource_name_filter=acl_filter.resource_pattern.resource_name,
                principal_filter=acl_filter.principal,
                host_filter=acl_filter.host,
                operation=acl_filter.operation,
                permission_type=acl_filter.permission_type
            )
        elif version <= 1:
            request = DescribeAclsRequest[version](
                resource_type_filter=acl_filter.resource_pattern.resource_type,
                resource_name_filter=acl_filter.resource_pattern.resource_name,
                pattern_type_filter=acl_filter.resource_pattern.pattern_type,
                principal_filter=acl_filter.principal,
                host_filter=acl_filter.host,
                operation=acl_filter.operation,
                permission_type=acl_filter.permission_type
            )
        response = self._manager.run(self._manager.send(request))  # pylint: disable=E0606
        error_type = Errors.for_code(response.error_code)
        if error_type is not Errors.NoError:
            raise error_type(
                "Request '{}' failed with response '{}'."
                    .format(request, response))

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
        acl_list = []
        for resource in describe_response.resources:
            if describe_response.API_VERSION == 0:
                error_code, resource_type, resource_name, acls = resource
                resource_pattern_type = ACLResourcePatternType.LITERAL.value
            else:
                error_code, resource_type, resource_name, resource_pattern_type, acls = resource
            error_type = Errors.for_code(error_code)
            if error_type is not Errors.NoError:
                raise error_type(
                    "Request '{}' failed with response '{}'."
                        .format("DescribeAclsRequest", describe_response))
            for acl in acls:
                if describe_response.API_VERSION == 0:
                    principal, host, operation, permission_type = acl
                else:
                    principal, host, operation, permission_type = acl[:4]
                acl_list.append(
                    ACL(
                        principal=principal,
                        host=host,
                        operation=ACLOperation(operation),
                        permission_type=ACLPermissionType(permission_type),
                        resource_pattern=ResourcePattern(
                            resource_type=ResourceType(resource_type),
                            resource_name=resource_name,
                            pattern_type=ACLResourcePatternType(resource_pattern_type)
                        )
                    )
                )
        return acl_list, Errors.NoError

    @staticmethod
    def _convert_create_acls_resource_request_v0(acl):
        """Convert an ACL object into the CreateAclsRequest v0 format."""
        return (
            acl.resource_pattern.resource_type,
            acl.resource_pattern.resource_name,
            acl.principal,
            acl.host,
            acl.operation,
            acl.permission_type
        )

    @staticmethod
    def _convert_create_acls_resource_request_v1(acl):
        """Convert an ACL object into the CreateAclsRequest v1 format."""
        return (
            acl.resource_pattern.resource_type,
            acl.resource_pattern.resource_name,
            acl.resource_pattern.pattern_type,
            acl.principal,
            acl.host,
            acl.operation,
            acl.permission_type
        )

    @staticmethod
    def _convert_create_acls_response_to_acls(acls, create_response):
        """Parse a CreateAclsResponse, returning a dict of successes and failures."""
        acl_results = []
        for i, result in enumerate(create_response.results):
            error_type = Errors.for_code(result.error_code)
            acl = acls[i]
            acl_results.append({"acl": acl, "error": error_type})
        return acl_results

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

        version = self._client.api_version(CreateAclsRequest, max_version=1)
        if version == 0:
            request = CreateAclsRequest[version](
                creations=[self._convert_create_acls_resource_request_v0(acl) for acl in acls]
            )
        elif version <= 1:
            request = CreateAclsRequest[version](
                creations=[self._convert_create_acls_resource_request_v1(acl) for acl in acls]
            )
        response = self._manager.run(self._manager.send(request))  # pylint: disable=E0606
        return self._convert_create_acls_response_to_acls(acls, response)

    @staticmethod
    def _convert_delete_acls_resource_request_v0(acl):
        """Convert an ACLFilter object into the DeleteAclsRequest v0 format."""
        return (
            acl.resource_pattern.resource_type,
            acl.resource_pattern.resource_name,
            acl.principal,
            acl.host,
            acl.operation,
            acl.permission_type
        )

    @staticmethod
    def _convert_delete_acls_resource_request_v1(acl):
        """Convert an ACLFilter object into the DeleteAclsRequest v1 format."""
        return (
            acl.resource_pattern.resource_type,
            acl.resource_pattern.resource_name,
            acl.resource_pattern.pattern_type,
            acl.principal,
            acl.host,
            acl.operation,
            acl.permission_type
        )

    @staticmethod
    def _convert_delete_acls_response_to_matching_acls(acl_filters, delete_response):
        """Parse a DeleteAclsResponse, returning a list of (filter, matched ACLs, error) tuples."""
        results = []
        for i, result in enumerate(delete_response.filter_results):
            acl_filter = acl_filters[i]
            error_type = Errors.for_code(result.error_code)
            matching_acls = []
            for matching_acl in result.matching_acls:
                matching_error = Errors.for_code(matching_acl.error_code)
                if delete_response.API_VERSION == 0:
                    resource_type, resource_name, principal, host, operation, permission_type = matching_acl[1:]
                    resource_pattern_type = ACLResourcePatternType.LITERAL.value
                else:
                    resource_type, resource_name, resource_pattern_type, principal, host, operation, permission_type = matching_acl[1:]
                matching_acls.append(
                    ACL(
                        principal=principal,
                        host=host,
                        operation=ACLOperation(operation),
                        permission_type=ACLPermissionType(permission_type),
                        resource_pattern=ResourcePattern(
                            resource_type=ResourceType(resource_type),
                            resource_name=resource_name,
                            pattern_type=ACLResourcePatternType(resource_pattern_type)
                        )
                    )
                )
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

        version = self._client.api_version(DeleteAclsRequest, max_version=1)

        if version == 0:
            request = DeleteAclsRequest[version](
                filters=[self._convert_delete_acls_resource_request_v0(acl) for acl in acl_filters]
            )
        elif version <= 1:
            request = DeleteAclsRequest[version](
                filters=[self._convert_delete_acls_resource_request_v1(acl) for acl in acl_filters]
            )
        response = self._manager.run(self._manager.send(request))  # pylint: disable=E0606
        return self._convert_delete_acls_response_to_matching_acls(acl_filters, response)
