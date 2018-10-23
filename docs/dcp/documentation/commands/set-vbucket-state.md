### Set VBucket State (0x5b)

The Set VBucket message is used during the VBucket takeover process to hand off ownership of a VBucket between two nodes. The message format as well as the state values for this operation is below.

The request:
* Must have extras
* Must not have key
* Must not have value

Extra looks like:

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| state         |               |               |               |
       +---------------+---------------+---------------+---------------+
       Total 1 byte

State may have the following values:

* Active (0x01) - Changes the VBucket on the consumer side to active state.
* Replica (0x02) - Changes the VBucket on the consumer side to replica state.
* Pending (0x03) - Changes the VBucket on the consumer side to pending state.
* Dead (0x04) - Changes the VBucket on the consumer side to dead state.

The client should not send a reply to this command. The following example shows the breakdown of the message:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x5b          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x01          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
      12| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x04          |
        +---------------+

    DCP_SET_VBUCKET_STATE command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x5b
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x01
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000001
    Opaque       (12-15): 0xdeadbeef
    CAS          (16-23): 0x0000000000000000
      state      (24)   : 0x4 (dead)

### Returns

Returns success if the vbucket state is successfully changed or returns an error on failure.

### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a stream does not exist for the vbucket specfied on this connection.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**(Disconnect)**

If this message is sent to a connection that is not a consumer.
