class ListGroups:
    COMMAND = 'list'
    HELP = 'List Groups'

    @classmethod
    def add_arguments(cls, parser):
        pass

    @classmethod
    def command(cls, client, args):
        return client.list_groups()
