### Stream End (opcode 0x55)

Sent to the consumer to indicate that the producer has no more messages to stream for the specified vbucket.

The request:
* Must have extras
* Must not have key
* Must not have value

The client should not send a reply to this command. The following example shows the breakdown of the message:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x55          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x04          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x04          |
        +---------------+---------------+---------------+---------------+
      12| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+

    DCP_STREAM_END command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x55
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000004
    Opaque       (12-15): 0xdeadbeef
    CAS          (16-23): 0x0000000000000000
      flag       (24-27): 0x00000000 (OK)

The flags field is used to specify to the consumer why the stream was closed and will have one of the following values:

* *OK (0x00)* - The stream has finished without error.
* *Closed (0x01)* - This indicates that the close stream command was invoked on this stream causing it to be closed by force.
* *State Changed (0x02)* - The state of the VBucket that is being streamed has changed to state that the consumer does not want to receive.
* *Disconnected (0x03)* - The stream is closing because the connection is being disconnected.
* *Too Slow (0x04)* - The stream is closing because the client cannot read from the stream fast enough. This is done to prevent the server from running out of resources trying while trying to serve the client. When the client is ready to read from the stream again it should reconnect. This flag is available starting in Couchbase 4.5.

### Returns

A status code indicating whether or not the operation was successful.

### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a stream does not exist for the vbucket specfied on this connection.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**(Disconnect)**

If this message is sent to a connection that is not a consumer.
