from kafka.admin import MemberToRemove


class RemoveGroupMembers:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser(
            'remove-members',
            help='Remove members from a consumer group')
        parser.add_argument('-g', '--group-id', type=str, required=True)
        parser.add_argument(
            '-m', '--member-id', type=str, action='append',
            dest='member_ids', default=[],
            help='Dynamic member id to remove (repeatable).')
        parser.add_argument(
            '-i', '--group-instance-id', type=str, action='append',
            dest='group_instance_ids', default=[],
            help='Static group.instance.id to remove (repeatable; '
                 'requires broker LeaveGroup v3+).')
        parser.add_argument(
            '--reason', type=str, default=None,
            help='Optional reason; sent to broker on LeaveGroup v5+ '
                 '(KIP-800). Applied to all members.')
        parser.add_argument(
            '--group-coordinator-id', type=int, default=None,
            help='Send directly to this broker id, skipping coordinator lookup')
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        members = [MemberToRemove(member_id=mid, reason=args.reason)
                   for mid in args.member_ids]
        members.extend(MemberToRemove(group_instance_id=gid, reason=args.reason)
                       for gid in args.group_instance_ids)
        if not members:
            raise ValueError(
                'At least one --member-id or --group-instance-id is required.')
        result = client.remove_group_members(
            args.group_id, members,
            group_coordinator_id=args.group_coordinator_id)
        return {m: err.__name__ for m, err in result.items()}
