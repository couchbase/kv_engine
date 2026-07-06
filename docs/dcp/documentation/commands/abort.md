### Abort (opcode 0x63)

Sent by the producer to the consumer to cancel a prepared SyncWrite. It
tells the consumer that the write with `prepared_seqno` could not be
durably committed and the prepare should be discarded. The abort is
itself assigned a sequence number `abort_seqno` so the consumer can
maintain a consistent seqno ordering.

The request:
* Must have a key
* Must not have value
* Must have a 16-byte extras section

Extra looks like:

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| prepared_seqno                                                |
       |                                                               |
       +---------------+---------------+---------------+---------------+
      8| abort_seqno                                                   |
       |                                                               |
       +---------------+---------------+---------------+---------------+
      Total 16 bytes

Both fields are stored in network byte order (big-endian).

The following example shows an abort of key "hello" where the prepare
was at seqno 4 and the abort lands at seqno 5:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x63          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
       4| 0x10          | 0x00          | 0x02          | 0x10          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x15          |
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
      32| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      36| 0x00          | 0x00          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
      40| 0x68 ('h')    | 0x65 ('e')    | 0x6c ('l')    | 0x6c ('l')    |
        +---------------+---------------+---------------+---------------+
      44| 0x6f ('o')    |
        +---------------+

    DCP_ABORT command
    Field           (offset) (value)
    Magic           (0)    : 0x80
    Opcode          (1)    : 0x63
    Key length      (2,3)  : 0x0005
    Extra length    (4)    : 0x10
    Data type       (5)    : 0x00
    Vbucket         (6,7)  : 0x0210
    Total body      (8-11) : 0x00000015
    Opaque          (12-15): 0x00001210
    CAS             (16-23): 0x0000000000000000
      prepared_seqno(24-31): 0x0000000000000004
      abort_seqno   (32-39): 0x0000000000000005
    Key             (40-44): hello

### Returns

The consumer should not send a reply to this command. Upon receiving an
abort the consumer must remove any in-memory or on-disk state associated
with the corresponding prepare.

### DCP buffer acknowledgement

An `Abort` message is subject to DCP flow control and must be included
in [Buffer Acknowledgement](buffer-ack.md) accounting. The size to
acknowledge is the full message size: 24-byte header + 16-byte extras +
key length.

### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

Returned if the consumer receives an abort for a key it has no prepare
for. The consumer should log this but it does not require disconnecting.