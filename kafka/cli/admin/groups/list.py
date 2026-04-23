from kafka.admin import GroupState, GroupType


class ListGroups:
    COMMAND = 'list'
    HELP = 'List Groups'

    @classmethod
    def add_arguments(cls, parser):
        state_choices = sorted(s.value for s in GroupState)
        type_choices = sorted(t.value for t in GroupType)
        parser.add_argument(
            '--state', type=str, action='append', dest='states_filter', default=[],
            help='Filter by group state (repeatable). One of: '
                 + ', '.join(state_choices)
                 + '. Case-insensitive; names also accepted. '
                   'Requires broker >= 3.0 (KIP-518).')
        parser.add_argument(
            '--type', type=str, action='append', dest='types_filter', default=[],
            help='Filter by group type (repeatable). One of: '
                 + ', '.join(type_choices)
                 + '. Requires broker >= 4.0 (KIP-848).')

    @classmethod
    def command(cls, client, args):
        return client.list_groups(
            states_filter=args.states_filter or None,
            types_filter=args.types_filter or None)
