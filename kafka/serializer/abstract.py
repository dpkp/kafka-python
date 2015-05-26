import abc

class AbstractSerializer(object):
    """
    Abstract Serializer/Deserializer Interface, based on java client interface.

    Used to convert python data to/from raw kafka messages (bytes)

    Methods:
        serialize(topic, data): convert data to bytes for topic
        deserialize(topic, data): convert data from bytes for topic

    See Also:
        http://kafka.apache.org/082/javadoc/org/apache/kafka/common/serialization/Serializer.html
        http://kafka.apache.org/082/javadoc/org/apache/kafka/common/serialization/Deserializer.html
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, is_key=False, **configs):
        """
        Parameters:
            is_key (bool): True if this instance will be passed keys
                instead of values (default: False, expects values)

        Other Parameters:
            subclasses can optionally use keyword arguments to help configure
        """
        pass

    @abc.abstractmethod
    def serialize(self, topic, data):
        """
        Parameters:
            topic (str): topic associated with the data
            data (abstract): the python key or value that should be serialized

        Returns:
            serialized bytes that can be sent to the kafka cluster
        """
        # example: encode unicode strings as utf-8 bytes
        return data.encode('utf-8')

    @abc.abstractmethod
    def deserialize(self, topic, data):
        """
        Parameters:
            topic (str): topic associated with the data
            data (bytes): serialized bytes as received from kafka

        Returns:
            a deserialized python data structure, as determined by
            the subclass [e.g., str int float dict list ...]
        """
        # example: decode utf-8 bytes to unicode python strings
        return data.decode('utf-8')
