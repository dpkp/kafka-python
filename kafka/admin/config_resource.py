from enum import IntEnum


class ConfigResourceType(IntEnum):
    """An enumerated type of config resources"""

    BROKER = 4,
    TOPIC = 2


class ConfigResource:
    """A class for specifying config resources.
    Arguments:
        resource_type (ConfigResourceType): the type of kafka resource
        name (string): The name of the kafka resource
        configs ({key : value}): A  maps of config keys to values.
    """

    def __init__(
            self,
            resource_type,
            name,
            configs=None
    ):
        if not isinstance(resource_type, (ConfigResourceType)):
            resource_type = ConfigResourceType[str(resource_type).upper()] # pylint: disable-msg=unsubscriptable-object
        self.resource_type = resource_type
        self.name = name
        self.configs = configs
