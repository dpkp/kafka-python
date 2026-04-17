from kafka.admin._acls import (
    ACL, ACLFilter, ACLOperation, ACLPermissionType, ACLResourcePatternType,
    ResourceType, ResourcePattern, ResourcePatternFilter
)


def _enum_names(enum_cls):
    return [e.name.lower() for e in enum_cls]


def add_acl_filter_args(parser):
    """Add arguments for building an ACLFilter (used by describe and delete)."""
    parser.add_argument('--principal', type=str, default=None, help='ACL principal (e.g. User:alice)')
    parser.add_argument('--host', type=str, default=None, help='ACL host (default: match any)')
    parser.add_argument('--operation', type=str, default='any',
                        choices=_enum_names(ACLOperation),
                        help='ACL operation (default: any)')
    parser.add_argument('--permission-type', type=str, default='any',
                        choices=_enum_names(ACLPermissionType),
                        help='ACL permission type (default: any)')
    parser.add_argument('--resource-type', type=str, default='any',
                        choices=_enum_names(ResourceType),
                        help='Resource type (default: any)')
    parser.add_argument('--resource-name', type=str, default=None, help='Resource name')
    parser.add_argument('--pattern-type', type=str, default='any',
                        choices=_enum_names(ACLResourcePatternType),
                        help='Resource pattern type (default: any)')


def add_acl_args(parser, required=False):
    """Add arguments for building a concrete ACL (used by create)."""
    parser.add_argument('--principal', type=str, required=required, help='ACL principal (e.g. User:alice)')
    parser.add_argument('--host', type=str, default='*', help='ACL host (default: *)')
    parser.add_argument('--operation', type=str, required=required,
                        choices=_enum_names(ACLOperation),
                        help='ACL operation')
    parser.add_argument('--permission-type', type=str, default='allow',
                        choices=_enum_names(ACLPermissionType),
                        help='ACL permission type (default: allow)')
    parser.add_argument('--resource-type', type=str, required=required,
                        choices=_enum_names(ResourceType),
                        help='Resource type')
    parser.add_argument('--resource-name', type=str, required=required, help='Resource name')
    parser.add_argument('--pattern-type', type=str, default='literal',
                        choices=_enum_names(ACLResourcePatternType),
                        help='Resource pattern type (default: literal)')


def acl_filter_from_args(args):
    """Build an ACLFilter from parsed CLI arguments."""
    return ACLFilter(
        principal=args.principal,
        host=args.host,
        operation=ACLOperation[args.operation.upper()],
        permission_type=ACLPermissionType[args.permission_type.upper()],
        resource_pattern=ResourcePatternFilter(
            resource_type=ResourceType[args.resource_type.upper()],
            resource_name=args.resource_name,
            pattern_type=ACLResourcePatternType[args.pattern_type.upper()],
        ),
    )


def acl_from_args(args):
    """Build an ACL from parsed CLI arguments."""
    return ACL(
        principal=args.principal,
        host=args.host or '*',
        operation=ACLOperation[args.operation.upper()],
        permission_type=ACLPermissionType[args.permission_type.upper()],
        resource_pattern=ResourcePattern(
            resource_type=ResourceType[args.resource_type.upper()],
            resource_name=args.resource_name,
            pattern_type=ACLResourcePatternType[args.pattern_type.upper()],
        ),
    )
