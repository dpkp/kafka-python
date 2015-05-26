import codecs

from kafka.serializer.abstract import AbstractSerializer


class StringSerializer(AbstractSerializer):
    """
    A simple serializer class that encodes python strings
    using the python codecs library
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
        self.is_key = is_key
        self.encoding = configs.get('encoding', 'utf-8')

        # lookup the encoding to verify and cache
        codecs.lookup(self.encoding)

    def serialize(self, topic, data):
        """
        Parameters:
            topic (str): topic associated with the data
            data (str): the python key or value that should be serialized

        Returns:
            bytes serialized using the configured encoding codec
        """
        return codecs.encode(data, self.encoding)

    def deserialize(self, topic, data):
        """
        Parameters:
            topic (str): topic associated with the data
            data (bytes): serialized bytes as received from kafka

        Returns:
            a string, deserialized using the configured encoding codec
        """
        return codecs.decode(data, self.encoding)
