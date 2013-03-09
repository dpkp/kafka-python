For 0.8, we have correlation id so we can potentially interleave requests/responses

There are a few levels of abstraction:

* Protocol support: encode/decode the requests/responses
* Socket support: send/recieve messages
* API support: higher level APIs such as: get_topic_metadata


# Methods of producing

* Round robbin (each message to the next partition)
* All-to-one (each message to one partition)
* All-to-all? (each message to every partition)
* Partitioned (run each message through a partitioning function)
** HashPartitioned
** FunctionPartition
