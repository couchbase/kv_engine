# Add Stream (opcode 0x51)

Sent to the consumer to tell the consumer to initiate a stream request with the producer.

The request:

* Must have a 4 byte extras section
* Must not have key
* Must not have value

Extra looks like:

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| flags                                                         |
       +---------------+---------------+---------------+---------------+

Flags is specified as a bitmask in network byte order with the
following bits defined:

* 0x01 (Takeover) - Specifies that the stream should send over all remaining data to the remote node and then set the remote nodes vbucket to active state and the source nodes vbucket to dead.
* 0x02 (Disk Only) - Specifies that the stream should only send items only if they are on disk. The first item sent is specified by the start sequence number and items will be sent up to the sequence number specified by the end sequence number or the last on disk item when the stream is created.
* 0x04 (Latest) - Specifies that the server should stream all mutations up to the current sequence number for that VBucket. The server will overwrite the value of the end sequence number field with the value of the latest sequence number.
* 0x08 (Removed in 5.0, use the NO_VALUE flag in DCP Open instead) (No Value) - Specifies that the server should stream only item key and metadata in the mutations and not stream the value of the item.
* 0x10 (Active VB Only) - Specifies that the server should add stream only if the vbucket is active. If the vbucket is not active, the request fails with error ENGINE_NOT_MY_VBUCKET.
* 0x20 (Strict VBUUID match) - Specifies that the server should check for vb_uuid match even at start_seqno 0 before adding the stream. Upon mismatch the sever should return ENGINE_ROLLBACK error.

The following example shows the breakdown of the message:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x51          | 0x00          | 0x00          |
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
      24| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
    DCP_ADD_STREAM command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x51
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0005
    Total body   (8-11) : 0x00000004
    Opaque       (12-15): 0x00000001
    CAS          (16-23): 0x0000000000000000
      flags      (24-27): 0x00000001 (takeover)

The DCP consumer will now try to set up an DCP stream with the producer, and once it is established it will respond with the following message:

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
      24| 0x00          | 0x00          | 0x10          | 0x00          |
        +---------------+---------------+---------------+---------------+
    DCP_ADD_STREAM response
    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x51
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000004
    Opaque       (12-15): 0x00000001
    CAS          (16-23): 0x0000000000000000
      opaque     (24,27): 0x00001000

The opaque field in the extra field of the response contains the
opaque value used by messages passing for that vbucket. The vbucket
identifier in the extra field is the vbucket identifier this response
belongs to.

### Returns

On success a unique identifier is returned in the opaque field. This identifier can be used to differentitate two different streams that were accessing the same vbucket. On failure an error code will be returned.

### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS (0x02)**

If a stream for this vbucket already exists on either the producer or the consumer for the connection this command was sent over.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET (0x07)**

If the consumer does not have the given VBucket then this error is returned.

**(Disconnect)**

A disconnect may happen for one of two reasons:

* If the connection state no longer exists on the server. The most likely reason this will happen is if another connection is made to the server from a different client with the same name as the current connection.
* If this command is sent to a producer endpoint.
