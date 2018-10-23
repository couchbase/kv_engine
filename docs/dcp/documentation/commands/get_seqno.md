## Get All vBucket Sequence Numbers (0x48)

The Get All vBucket Sequence Numbers command can be used to get the current high sequence numbers one could stream from of all vBuckets that are located on the server.

You may retrict the returned values to a certain vBucket state. The state is supplied as an extra field and is fully optional.

The request:
 * May have extras
 * Must not have key
 * Must not have value

The response:
 * Must not have extras
 * Must not have key
 * Must have value on Success

Optional Extra looks like:

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+
      0| vbucket_state |
       +---------------+
       Total 1 byte


The `vbucket_state` is one of:

 - active: `0x01`
 - replica: `0x02`
 - pending: `0x03`
 - dead: `0x04`


### Binary Implementation

#### Get All vBucket Sequence Numbers Binary Request

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0| 0x80          | 0x48          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
     4| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
     8| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
    12| 0xde          | 0xad          | 0xbe          | 0xef          |
      +---------------+---------------+---------------+---------------+
    16| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
    20| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+

    Header breakdown
    GET_ALL_VB_SEQNOS command
    Field        (offset) (value)
    Magic        (0)    : 0x80                (Request)
    Opcode       (1)    : 0x48                (Get All vBucket Sequence Numbers)
    Key length   (2,3)  : 0x0000              (0)
    Extra length (4)    : 0x00                (0)
    Data type    (5)    : 0x00                (Field not used)
    VBucket      (6,7)  : 0x0000              (Field not used)
    Total body   (8-11) : 0x00000000          (4)
    Opaque       (12-15): 0xdeadbeef          (3735928559)
    CAS          (16-23): 0x0000000000000000  (Field not used)


#### Get All vBucket Sequence Numbers Binary Request (restricted to a certain state)

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0| 0x80          | 0x48          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
     4| 0x01          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
     8| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
    12| 0xde          | 0xad          | 0xbe          | 0xef          |
      +---------------+---------------+---------------+---------------+
    16| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
    20| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
    24| 0x02          |
      +---------------+

    Header breakdown
    GET_ALL_VB_SEQNOS command
    Field        (offset) (value)
    Magic        (0)    : 0x80                (Request)
    Opcode       (1)    : 0x48                (Get All vBucket Sequence Numbers)
    Key length   (2,3)  : 0x0000              (0)
    Extra length (4)    : 0x01                (1)
    Data type    (5)    : 0x00                (Field not used)
    VBucket      (6,7)  : 0x0000              (Field not used)
    Total body   (8-11) : 0x00000000          (4)
    Opaque       (12-15): 0xdeadbeef          (3735928559)
    CAS          (16-23): 0x0000000000000000  (Field not used)
      vb state   (24)   : 0x02                (replica)

#### Get All vBucket Sequence Numbers Binary Response

If the command executes successful (see the status field), the following packet is returned from a server which has 4 vBuckets. The vBuckets in the response are sorted by their number.

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0| 0x81          | 0x48          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
     4| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
     8| 0x00          | 0x00          | 0x00          | 0x28          |
      +---------------+---------------+---------------+---------------+
    12| 0xde          | 0xad          | 0xbe          | 0xef          |
      +---------------+---------------+---------------+---------------+
    16| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
    20| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
    24| 0x00          | 0x0a          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
    28| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
    32| 0x54          | 0x32          | 0x00          | 0x0d          |
      +---------------+---------------+---------------+---------------+
    36| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
    40| 0x01          | 0x34          | 0x32          | 0x14          |
      +---------------+---------------+---------------+---------------+
    44| 0x00          | 0x7f          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
    48| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
    52| 0x00          | 0x04          | 0x02          | 0xd0          |
      +---------------+---------------+---------------+---------------+
    56| 0x00          | 0x00          | 0x00          | 0x00          |
      +---------------+---------------+---------------+---------------+
    60| 0x00          | 0x00          | 0x65          | 0x24          |
      +---------------+---------------+---------------+---------------+

    Header breakdown
    UPR_GET_SEQUENCE_NUMBERS response
    Field        (offset) (value)
    Magic        (0)    : 0x81                (Response)
    Opcode       (1)    : 0x48                (Get All vBucket Sequence Numbers)
    Key length   (2,3)  : 0x0000              (0)
    Extra length (4)    : 0x00                (0)
    Data type    (5)    : 0x00                (Field not used)
    Status       (6,7)  : 0x0000              (Success)
    Total body   (8-11) : 0x00000028          (40)
    Opaque       (12-15): 0xdeadbeef          (3735928559)
    CAS          (16-23): 0x0000000000000000  (Field not used)
      vb         (24,25): 0x000a              (10)
      vb seqno   (26-33): 0x0000000000005432  (21554)
      vb         (34,35): 0x000d              (13)
      vb seqno   (36-43): 0x0000000001343214  (20197908)
      vb         (44,45): 0x007f              (127)
      vb seqno   (46-53): 0x0000000000000004  (4)
      vb         (54,55): 0x02d0              (720)
      vb seqno   (56-63): 0x0000000000006524  (25892)

### Use Cases

The Get All vBucket Sequence Numbers command can be used to find out the most recent sequence number in order to start a stream request up to that sequence.
