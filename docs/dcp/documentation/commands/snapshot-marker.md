### Snapshot Marker (opcode 0x56)

Sent by the producer to tell the consumer that a new snapshot is being sent. A snapshot is simply a series of commands that is guarenteed to contain a unique set of keys.

The request:
* Must have extras
* Must not have key
* Must not have value

The client should not send a reply to this command unless the ack flag is set. The following example shows the breakdown of the message:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x56          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x14          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      32| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      36| 0x00          | 0x00          | 0x00          | 0x08          |
        +---------------+---------------+---------------+---------------+
      40| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+

    DCP_SNAPSHOT_MARKER command
    Field           (offset) (value)
    Magic           (0)    : 0x80
    Opcode          (1)    : 0x56
    Key length      (2,3)  : 0x0000
    Extra length    (4)    : 0x14
    Data type       (5)    : 0x00
    Vbucket         (6,7)  : 0x0000
    Total body      (8-11) : 0x00000014
    Opaque          (12-15): 0xdeadbeef
    CAS             (16-23): 0x0000000000000000
      Start Seqno   (24-31): 0x0000000000000000
      End Seqno     (32-39): 0x0000000000000008
      Snapshot Type (40-43): 0x00000001 (disk)

Snapshot Type is defined as:

* 0x01 (memory) - Specifies that the snapshot contains in-meory items only.
* 0x02 (disk) - Specifies that the snapshot contains on-disk items only.
* 0x04 (checkpoint) - An internally used flag for intra-cluster replication to help to keep in-memory datastructures look similar.
* 0x08 (ack) - Specifies that this snapshot marker should return a response once the entire snapshot is received.


### Returns

This message will not return a response unless an error occurs or the ack flag is set.

### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a stream does not exist for the vbucket specfied on this connection.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**(Disconnect)**

If this message is sent to a connection that is not a consumer.
