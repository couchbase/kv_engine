### Prepare (0x60)

Tells the consumer that the message contains a prepare (first stage of a
durable write).

The request:
* Must have extras
* Must have key
* May have value

Extra looks like:

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| by_seqo                                                       |
       |                                                               |
       +---------------+---------------+---------------+---------------+
      8| rev seqno                                                     |
       |                                                               |
       +---------------+---------------+---------------+---------------+
     16| flags                                                         |
       +---------------+---------------+---------------+---------------+
     20| expiration                                                    |
       +---------------+---------------+---------------+---------------+
     24| lock_time                                                     |
       +---------------+---------------+---------------+---------------+
     28| NRU           | deleted       | durability    |
       +---------------+---------------+---------------+
       Total 31 bytes

NRU is an internal field used by the server and may safely be ignored by other consumers.

The consumer should not send a reply to this command. The following example shows the breakdown of the message:

The following message is a prepare of key "hello" with durability level of 'Majority'


      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x57          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
       4| 0x1f          | 0x00          | 0x02          | 0x10          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x29          |
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
      36| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
      40| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      44| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      48| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      52| 0x00          | 0x00          | 0x01          | 0x68 ('h')    |
        +---------------+---------------+---------------+---------------+
      56| 0x65 ('e')    | 0x6c ('l')    | 0x6c ('l')    | 0x6f ('o')    |
        +---------------+---------------+---------------+---------------+
      60| 0x77 ('w')    | 0x6f ('o')    | 0x72 ('r')    | 0x6c ('l')    |
        +---------------+---------------+---------------+---------------+
      64| 0x64 ('d')    |
        +---------------+
    DCP_PREPARE command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x60
    Key length   (2,3)  : 0x0005
    Extra length (4)    : 0x1f
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0210
    Total body   (8-11) : 0x00000029
    Opaque       (12-15): 0x00001210
    CAS          (16-23): 0x0000000000000000
      by seqno   (24-31): 0x0000000000000004
      rev seqno  (32-39): 0x0000000000000001
      flags      (40-43): 0x00000000
      expiration (44-47): 0x00000000
      lock time  (48-51): 0x00000000
      nru        (52)   : 0
      deleted    (53)   : 0
      durability (54)   : 1
    Key          (55-59): hello
    Value        (60-65): world

### Returns

This message will not return a response unless an error occurs.

### DCP buffer acknowledgement

When the producer sends a DCP prepare an issue (MB-46634) means extra 2 bytes are accounted for,
these bytes are not sent to the consumer, but the consumer must include them
in buffer acknowledgement messages.
