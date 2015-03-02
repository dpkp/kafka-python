from kafka.serializer.abstract import AbstractSerializer


class NoopSerializer(AbstractSerializer):
    """
    A noop serializer class that just returns what it gets.
    """
    def __init__(self, is_key=False, **configs):
        """
        Arguments:
            is_key (bool): whether this instance will serialize keys (True) instead
                of values (False); default: False
            encoding (str): a python str-to-bytes encoding codec; default: utf-8

        See Also:
            https://docs.python.org/2/library/codecs.html#standard-encodings
        """
        pass

    def serialize(self, topic, data):
        """
        Parameters:
            topic (str): topic associated with the data
            data (bytes): bytes -- no serialization required!

        Returns:
            the data, unaltered
        """
        return data

    def deserialize(self, topic, data):
        """
        Parameters:
            topic (str): topic associated with the data
            data (bytes): serialized bytes as received from kafka

        Returns:
            the data, unaltered
        """
        return data
