### OSO Snapshot (opcode 0x65)

Sent by the producer to tell the consumer that a Out of Sequence Order snapshot
is to be transmitted or has now been completed.

A snapshot is simply a series of commands that is guaranteed to contain a unique set of keys.
An OSO snapshot is exactly this, but differs from a normal DCP snapshot (opcode 0x56) in that
it transmits the keys in an undefined order.

The OSO snapshot message contains a flags bit-field which has the following defined flags:

* 0x01: `start` This is the start of an OSO snapshot
* 0x02: `end` This is the end of an OSO snapshot

An OSO snapshot will only be transmitted if the client has enabled the feature using
DCP control `enable_out_of_order_snapshots`. A correctly written client that makes use of the OSO
snapshot should apply the following rules.

* During in the receipt (after the start and before the end) the client must track the greatest sequence number received (X) in addition to the greatest sequence number received before the start (Y).
* If the stream fails before receipt of the end, the client must resume from the sequence number Y.
* If the stream fails after the receipt of the end, the client can resume from sequence number X.
** resuming from sequence number X before the end of an OSO snapshot will result in the client receiving a partial/incorrect view of the vbucket.

The request:
* Must have extras
* Must not have key

### Encoding Examples

The following example shows the breakdown of the message:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x65          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x04          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+

    DCP_OSO_SNAPSHOT command
    Field           (offset) (value)
    Magic           (0)    : 0x80
    Opcode          (1)    : 0x65
    Key length      (2,3)  : 0x0000
    Extra length    (4)    : 0x4
    Data type       (5)    : 0x00
    Vbucket         (6,7)  : 0x0000
    Total body      (8-11) : 0x00000004
    Opaque          (12-15): 0xdeadbeef
    CAS             (16-23): 0x0000000000000000
      Flags         (24-27): 0x00000001 (start of a snapshot)
