class TestPackage:
    def test_top_level_namespace(self):
        import kafka as kafka1
        assert kafka1.KafkaConsumer.__name__ == "KafkaConsumer"
        assert kafka1.consumer.__name__ == "kafka.consumer"
        assert kafka1.codec.__name__ == "kafka.codec"

    def test_submodule_namespace(self):
        import kafka.client as client1
        assert client1.__name__ == "kafka.client"

        from kafka import client as client2
        assert client2.__name__ == "kafka.client"

        from kafka.client import SimpleClient as SimpleClient1
        assert SimpleClient1.__name__ == "SimpleClient"

        from kafka.codec import gzip_encode as gzip_encode1
        assert gzip_encode1.__name__ == "gzip_encode"

        from kafka import SimpleClient as SimpleClient2
        assert SimpleClient2.__name__ == "SimpleClient"

        from kafka.codec import snappy_encode
        assert snappy_encode.__name__ == "snappy_encode"
