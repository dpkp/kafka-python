class TestPackage:
    def test_top_level_namespace(self):
        import kafka as kafka1
        assert kafka1.KafkaConsumer.__name__ == "KafkaConsumer"
        assert kafka1.consumer.__name__ == "kafka.consumer"
        assert kafka1.codec.__name__ == "kafka.codec"

    def test_submodule_namespace(self):
        import kafka.net as net1
        assert net1.__name__ == "kafka.net"

        from kafka import net as net2
        assert net2.__name__ == "kafka.net"

        from kafka import KafkaConsumer as KafkaConsumer1
        assert KafkaConsumer1.__name__ == "KafkaConsumer"

        from kafka import KafkaConsumer as KafkaConsumer2
        assert KafkaConsumer2.__name__ == "KafkaConsumer"

        from kafka.codec import gzip_encode as gzip_encode1
        assert gzip_encode1.__name__ == "gzip_encode"

        from kafka.codec import snappy_encode
        assert snappy_encode.__name__ == "snappy_encode"
