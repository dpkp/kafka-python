class ListTopics:
    COMMAND = 'list'
    HELP = 'List Kafka Topics'

    @classmethod
    def add_arguments(cls, parser):
        pass

    @classmethod
    def command(cls, client, args):
        return client.list_topics()
