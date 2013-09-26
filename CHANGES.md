# Notable Changes

* Changing auto_commit to False in [SimpleConsumer](kafka/consumer.py), until 0.8.1 is release offset commits are unsupported

* Adding fetch_size_bytes to SimpleConsumer constructor to allow for user-configurable fetch sizes

* Allow SimpleConsumer to automatically increase the fetch size if a partial message is read and no other messages were read during that fetch request. The increase factor is 1.5

* Exception classes moved to kafka.common
