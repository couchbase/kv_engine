### Close Stream (opcode 0x52)

Sent to server controling an DCP stream to close the stream for a named vbucket as soon as possible.

The request:
* Must not extras
* Must not have key
* Must not have value

The layout of a message looks like:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x52          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
    DCP_CLOSE_STREAM command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x52
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0005
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0xdeadbeef
    CAS          (16-23): 0x0000000000000000

If received on the consumer side the consumer will close the stream for the specified vbucket imediately and let the producer know that the stream is closed as soon as it receives a message bound for that vbucket.

When received on the producer side:

(a) The producers that have received [send_stream_end_on_client_close_stream](./control.md) control message on the connection once and have accepted that, will send [STREAM_END](./stream-end.md) message to the consumer indicating that the stream was closed by force. That is, the producer may still receive response messages from the consumer for this stream until it receives [STREAM_END](./stream-end.md) message.

(b) The producers that have not received [send_stream_end_on_client_close_stream](./control.md) control message on the connection or the producers that have not accepted the control message (indicates the producer does not support sending [STREAM_END](./stream-end.md)), will stop sending messages to the consumer on this stream. The consumers should not expect any [STREAM_END](./stream-end.md) message.

Note: In case (b), there may be some lingering messages for the stream on the connection, that were sent before the producer saw close stream command and may reach the consumer after it receives the response to close stream. So it is advisable to use the [send_stream_end_on_client_close_stream](./control.md) control message and hence operate under case (a) on the producers that support it.

### Returns

A status code indicating whether or not the operation was successful.

### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a stream does not exist for the vbucket specfied on this connection.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_DCP_STREAMID_INVALID (0x8d)**

A close-stream request was made and either of the following situations was
detected.

* The request includes a stream-ID (non zero) and the stream-ID feature is not
 [enabled](control.md).
* The request does not include a stream-ID and the stream-ID feature is
 [enabled](control.md).

**(Disconnect)**

If the connection state no longer exists on the server. The most likely reason this will happen is if another connection is made to the server from a different client with the same name as the current connection.
