### Commit (opcode 0x62)

Sent by the producer to the consumer to finalise a prepared SyncWrite.
It tells the consumer that the write with `prepared_seqno` has been
durably committed and should be applied as a visible mutation at
`commit_seqno`.

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
      8| commit_seqno                                                  |
       |                                                               |
       +---------------+---------------+---------------+---------------+
      Total 16 bytes

Both fields are stored in network byte order (big-endian).

The following example shows a commit of key "hello" where the prepare
was at seqno 4 and the commit lands at seqno 5:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x62          | 0x00          | 0x05          |
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

    DCP_COMMIT command
    Field           (offset) (value)
    Magic           (0)    : 0x80
    Opcode          (1)    : 0x62
    Key length      (2,3)  : 0x0005
    Extra length    (4)    : 0x10
    Data type       (5)    : 0x00
    Vbucket         (6,7)  : 0x0210
    Total body      (8-11) : 0x00000015
    Opaque          (12-15): 0x00001210
    CAS             (16-23): 0x0000000000000000
      prepared_seqno(24-31): 0x0000000000000004
      commit_seqno  (32-39): 0x0000000000000005
    Key             (40-44): hello

### Returns

The consumer should not send a reply to this command. If the consumer
cannot apply the commit (e.g. it never received the matching prepare) it
should disconnect and reconnect to resync.

### DCP buffer acknowledgement

A `Commit` message is subject to DCP flow control and must be included
in [Buffer Acknowledgement](buffer-ack.md) accounting. The size to
acknowledge is the full message size: 24-byte header + 16-byte extras +
key length.
