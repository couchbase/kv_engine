### Seqno Advanced (opcode 0x64)

Sent by the producer to tell the consumer that the vbucket seqno has advanced due to an event that the consumer is not subscribed too.
For instance this might be sync-write prepares or mutations from a collection that the consumer is not subscribed too.

The consumer should use the seqno in this opcode to check weather they have now fully received a snapshot or not.
Note is also possible that this maybe the only mutation sent within a snapshot.

**This opcode will only be sent after a client has sent a HELLO opcode to the producer with collection enable set.**

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
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x00          | 0x04          |
        +---------------+---------------+---------------+---------------+

    DCP_SEQNO_ADVANCED command
    Field           (offset) (value)
    Magic           (0)    : 0x80
    Opcode          (1)    : 0x64
    Key length      (2,3)  : 0x0000
    Extra length    (4)    : 0x8
    Data type       (5)    : 0x00
    Vbucket         (6,7)  : 0x0000
    Total body      (8-11) : 0x00000008
    Opaque          (12-15): 0xdeadbeef
    CAS             (16-23): 0x0000000000000000
      by_seqno      (24-31): 0x0000000000000004 (start of a snapshot)

##### Extra Fields
**by_seqno**
Stores the value of the current high seqno of the vbucket.