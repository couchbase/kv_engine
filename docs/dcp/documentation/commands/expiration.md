### Expiration (0x59)

Tells the consumer that the message contains a key expiration.

The request:
* Must have extras
* Must have key
* Must not have value

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
     16| Metadata Size                 |
       +---------------+---------------*
       Total 18 bytes

The metadata is located after the items key.

The client should not send a reply to this command. The following example shows the breakdown of the message:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x59          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
       4| 0x12          | 0x00          | 0x02          | 0x10          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x17          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x12          | 0x10          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
      32| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      36| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
      40| 0x00          | 0x00          | 0x68 ('h')    | 0x65 ('e')    |
        +---------------+---------------+---------------+---------------+
      44| 0x6c ('l')    | 0x6c ('l')    | 0x6f ('o')    |
        +---------------+---------------+---------------+
    UPR_EXPIRATION command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x59
    Key length   (2,3)  : 0x0005
    Extra length (4)    : 0x12
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0210
    Total body   (8-11) : 0x00000017
    Opaque       (12-15): 0x00001210
    CAS          (16-23): 0x0000000000000000
      by seqno   (24-31): 0x0000000000000005
      rev seqno  (32-39): 0x0000000000000001
      nmeta      (40-41): 0x0000
    Key          (42-46): hello

### Returns

This message will not return a response unless an error occurs.

### Extended Meta Data Section
The extended meta data section is used to send extra meta data for a particular expiration. This section is at the very end, after the value. Its length will be set in the nmeta field.
* [**Ext_Meta**](extended_meta/ext_meta_ver1.md)

### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a stream does not exist for the vbucket specfied on this connection.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_ERANGE (0x22)**

If the by sequence number is lower than the current by sequence number contained on the consumer. The by sequence number should always be greater than the one the consumer currently has so if the by sequence number in the mutation message is less than or equal to the by sequence number on the consumer this error is returned.

**(Disconnect)**

If this message is sent to a connection that is not a consumer.
