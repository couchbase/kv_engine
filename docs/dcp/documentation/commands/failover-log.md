### Failover Log Request (opcode 0x54)

The Failover log request is used by the consumer to request all known failover ids a client may use to continue from. A failover id consists of the vbucket UUID and a sequence number. If a client can't find a known failover id, it should select the vbucket with the highest sequence number since that is the stream with the shortest path to completion.

The request:
* Must not have extras
* Must not have key
* Must not have value

The response:
* Must not have extras
* Must not have key
* Must have value on Success

The following example requests the failover log for vbucket 0:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x54          | 0x00          | 0x00          |
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
    UPR_GET_FAILOVER_LOG command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x54
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0xdeadbeef
    CAS          (16-23): 0x0000000000000000

If the command executes successful (see the status field), the following packet is returned from a server which have 4 different failover ids available:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x54          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x40          |
        +---------------+---------------+---------------+---------------+
      12| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0xfe          | 0xed          | 0xde          | 0xca          |
        +---------------+---------------+---------------+---------------+
      32| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      36| 0x00          | 0x00          | 0x54          | 0x32          |
        +---------------+---------------+---------------+---------------+
      40| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      44| 0x00          | 0xde          | 0xca          | 0xfe          |
        +---------------+---------------+---------------+---------------+
      48| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      52| 0x01          | 0x34          | 0x32          | 0x14          |
        +---------------+---------------+---------------+---------------+
      56| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      60| 0xfe          | 0xed          | 0xfa          | 0xce          |
        +---------------+---------------+---------------+---------------+
      64| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      68| 0x00          | 0x00          | 0x00          | 0x04          |
        +---------------+---------------+---------------+---------------+
      72| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      76| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      80| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      84| 0x00          | 0x00          | 0x65          | 0x24          |
        +---------------+---------------+---------------+---------------+
    UPR_GET_FAILOVER_LOG response
    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x54
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000040
    Opaque       (12-15): 0xdeadbeef
    CAS          (16-23): 0x0000000000000000
      vb UUID    (24-31): 0x00000000feeddeca
      vb seqno   (32-39): 0x0000000000005432
      vb UUID    (40-47): 0x0000000000decafe
      vb seqno   (48-55): 0x0000000001343214
      vb UUID    (56-63): 0x00000000feedface
      vb seqno   (64-71): 0x0000000000000004
      vb UUID    (72-79): 0x00000000deadbeef
      vb seqno   (80-87): 0x0000000000006524

### Returns

A failover log for the vbucket requested. The failover log will be in descending order of time meaning the oldest failover entry will be the last entry in the response and the newest entry will be the first entry in the response. On failure and error code is returned.

### Errors

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET (0x07)**

If the VBucket the stream is requested for does not exist.

**PROTOCOL_BINARY_RESPONSE_ENOMEM (0x82)**

If the failover log could not be sent to due a failure to allocate memory.

**(Disconnect)**

A disconnect may happen for one of two reasons:

* If the connection state no longer exists on the server. The most likely reason this will happen is if another connection is made to the server from a different client with the same name as the current connection.
* If this command is sent to a consumer endpoint.
