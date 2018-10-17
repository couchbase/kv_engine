# Control (opcode 0x5E)

Sent by the Consumer to the Producer in order to configure connection settings.

The request:

* Must have a key
* Must have a value

Valid Keys:

* "enable_noop" - Used to enable to tell the Producer that the Consumer supports detecting dead connections through the use of a noop. The value for this message should be set to either "true" or "false". See the page on [dead connections](../dead-connections.md) for more details. This parameter is available starting in Couchbase 3.0.

* "connection_buffer_size" - Used to tell the Producer the size of the Consumer side buffer in bytes which the Consumer is using for flow control. The value of this parameter should be an integer in string form between 1 and 2^32. See the page on [flow control]() for more details. This parameter is available starting in Couchbase 3.0.

* "set_noop_interval" - Sets the noop interval on the Producer. Values for this parameter should be an integer in string form between 20 and 10800. This allows the noop interval to be set between 20 seconds and 3 hours. This parameter should always be set when enabling noops to prevent the Consumer and Producer having a different noop interval. This parameter is available starting in Couchbase 3.0.1.

* "set_priority" - Sets the priority that the connection should have when sending data. The priority may be set to "high", "medium", or "low". High priority connections will send messages at a higher rate than medium and low priority connections. This parameter is availale starting in Couchbase 4.0.

* "enable_ext_metadata" - Enables sending extended meta data. This meta data is mainly used for internal server processes and will vary between different releases of Couchbase. See the documentation on [extended meta data](extended_meta/ext_meta_ver1.md) for more information on what will be sent. Each version of Couchbase will support a specific version of extended meta data. This parameter is available stating in Couchbase 4.0.

* "force_value_compression" - Compresses values using snappy compression before sending them. Clients need to negotiate for snappy using HELO as a prerequisite to using this parameter. This will be available from version 5.5 onwards.

* "supports_cursor_dropping" - Tells the server that the client can tolerate the server dropping the connection. The server will only do this if the client cannot read data from the stream fast enough and it is highly recommended to be used in all situations. We only support disabling cursor dropping for backwards compatibility. This parameter is available starting in Couchbase 4.5.

* "send_stream_end_on_client_close_stream" - Tells the server (DCP Producer) that the client expects "STREAM_END" msg when the client initiates the stream close. The value for this message should be set to 'true' if the client expects "STREAM_END", else the msg need not be sent (default is 'false'). That is, if the DCP client sends this control message during the connection set up, only then the producer sends the "STREAM_END" message for stream close initiated by the client. For all other causes of stream close, server by default sends the "STREAM_END" message when it closes the stream (say due to all items being streamed or due to an error condition). This is available only from Couchbase 5.5 and the older versions do not recognize the ctrl message and return error "EINVAL", thereby helping the clients to identify whether the server has this feature or not.

* "enable_expiry_opcode" - Tells the server that the client would like to receive expiry opcodes when an item is expired. The value of this should be set to 'true' for this control to occur, and 'false' will turn them off which is also its default state. Note by setting this to true, the delete time is required inside the expiration packets so delete times (and therefore delete_v2) is implicitly enabled. This option is available in versions from Couchbase 6.5.

* `enable_stream_id` = `true` - Tells the server that the client would
like to create multiple DCP streams for a vbucket. Once enabled the client must
provide a stream-id value to all stream-requests. Note that once enabled on a
producer, it cannot be disabled.


The following example shows the breakdown of the message:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x5E          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x04          | 0x00          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x04          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x65          | 0x6E          | 0x61          | 0x62          |
        +---------------+---------------+---------------+---------------+
      28| 0x6C          | 0x65          | 0x5F          | 0x6E          |
        +---------------+---------------+---------------+---------------+
      32| 0x6F          | 0x6F          | 0x70          | 0x74          |
        +---------------+---------------+---------------+---------------+
      36| 0x72          | 0x75          | 0x65          |         	
        +---------------+---------------+---------------+

    DCP_CONTROL command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x5E
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0005
    Total body   (8-11) : 0x00000004
    Opaque       (12-15): 0x00000001
    CAS          (16-23): 0x0000000000000000
	Key:		 (24-34): "enable_noop"
	Value:       (35-38): "true"

The producer will respond immediately with a message indicating whether or not the control message was accepted by the Producer.

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x51          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x04          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x04          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+

    DCP_CONTROL response
    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x5E
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000004
    Opaque       (12-15): 0x00000001
    CAS          (16-23): 0x0000000000000000

### Returns

A status code indicating whether or not the operation was successful.

### Errors

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If an invalid or incorrect control parameter was sent. Also, if the value of the control parameter is not valid.

**PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED (0x83)**

If the node this command is sent to does not support the control parameter sent.

**(Disconnect)**

A disconnect may happen for one of two reasons:

* If the connection state no longer exists on the server. The most likely reason this will happen is if another connection is made to the server from a different client with the same name as the current connection.
* If this command is sent to a consumer endpoint.
