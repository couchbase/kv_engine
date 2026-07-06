### Seqno Acknowledged (opcode 0x61)

Sent by the consumer to the producer to report the highest sequence
number that the consumer has prepared. This is part of the Synchronous
Replication (SyncWrites) protocol and allows the producer to track
replica acknowledgement progress for durability tracking.

The request:
* Must not have key
* Must not have value
* Must have an 8-byte extras section

Extra looks like:

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| prepared_seqno                                                |
       |                                                               |
       +---------------+---------------+---------------+---------------+
      Total 8 bytes

The `prepared_seqno` is stored in network byte order (big-endian).

The following example shows the breakdown of the message acknowledging
prepared seqno 4:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x61          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x08          | 0x00          | 0x02          | 0x10          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x08          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x12          | 0x10          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x00          | 0x04          |
        +---------------+---------------+---------------+---------------+

    DCP_SEQNO_ACKNOWLEDGED command
    Field           (offset) (value)
    Magic           (0)    : 0x80
    Opcode          (1)    : 0x61
    Key length      (2,3)  : 0x0000
    Extra length    (4)    : 0x08
    Data type       (5)    : 0x00
    Vbucket         (6,7)  : 0x0210
    Total body      (8-11) : 0x00000008
    Opaque          (12-15): 0x00001210
    CAS             (16-23): 0x0000000000000000
      prepared_seqno(24-31): 0x0000000000000004

### Prerequisites

SyncReplication must be enabled on the connection before `SeqnoAcknowledged`
messages will be accepted. The consumer enables this during connection
setup by sending two [Control](control.md) messages:

1. `consumer_name` — set to a non-empty string identifying this consumer
   to the producer.
2. `enable_sync_writes` = `true` — opts in to the SyncWrites protocol.

If SyncReplication is not enabled, the producer returns
`PROTOCOL_BINARY_RESPONSE_EINVAL`.

### Returns

The producer does not send a response to this message in the success
case. If the VBucket no longer exists on the producer a non-success
status may be returned, but this does not indicate a fatal error and the
connection should remain open.

### Errors

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

SyncReplication is not enabled on this connection, or no `consumer_name`
has been set.

**PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET (0x07)**

The VBucket specified does not exist on this node.
