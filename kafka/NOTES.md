For 0.8, we have correlation id so we can potentially interleave requests/responses

There are a few levels of abstraction:

* Protocol support: encode/decode the requests/responses
* Socket support: send/recieve messages
* API support: higher level APIs such as: get_topic_metadata
