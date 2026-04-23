from .create import CreateTopic
from .delete import DeleteTopic
from .describe import DescribeTopics
from .list import ListTopics


class TopicsCommandGroup:
    GROUP = 'topics'
    HELP = 'Manage Kafka Topics'
    COMMANDS = [ListTopics, DescribeTopics, CreateTopic, DeleteTopic]
