### Expiration (0x59)

Tells the consumer that the message contains a key expiration.
For a general overview on how to trigger these messages, see
[expiry-opcode-output.md](../expiry-opcode-output.md).

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
      16| Delete Time                                                   |
        +---------------+-----------------------------------------------*
        Total 20 bytes

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
      40| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      44| 0x68 ('h')    | 0x65 ('e')    | 0x6c ('l')    | 0x6c ('l')    |
        +---------------+---------------+---------------+---------------+
      48| 0x6f ('o')    |
        +---------------*
    DCP_EXPIRATION command
    Field         (offset) (value)
    Magic         (0)    : 0x80
    Opcode        (1)    : 0x59
    Key length    (2,3)  : 0x0005
    Extra length  (4)    : 0x12
    Data type     (5)    : 0x00
    Vbucket       (6,7)  : 0x0210
    Total body    (8-11) : 0x00000017
    Opaque        (12-15): 0x00001210
    CAS           (16-23): 0x0000000000000000
      by seqno    (24-31): 0x0000000000000005
      rev seqno   (32-39): 0x0000000000000001
      delete time (40-43): 0x00000000
    Key           (44-48): hello

### Returns

This message will not return a response unless an error occurs.

### The meaning of datatype

The datatype field of the `DCP_EXPIRATION` describes the Value payload of the message
and not the datatype of the original document. This is important to consider when a
producer is opened with any combination of the no value or include xattr flags.

* [Datatype bits ](../../../BinaryProtocol.md#data-types)
* DCP without the value or xattr flags - datatype will never contain XATTR, but
  could be a combination of 0, JSON. If the client has enabled compression, the
  SNAPPY datatype bit may also be present.
* `DCP_OPEN_NO_VALUE` - datatype will always be 0
* `DCP_OPEN_NO_VALUE|DCP_OPEN_INCLUDE_XATTRS` - datatype will be 0 or XATTR.
* `DCP_OPEN_INCLUDE_XATTRS` - datatype could be many combinations. e.g. 0, XATTR or XATTR|JSON

### Delete Time

Receiving expiry opcodes requires 'include delete-times' so that packets sent
can use the V2 format.

The delete time field is used to time stamp when the item was deleted. This
field is mainly of use to clients building a replica of an active vbucket as
the delete time is persisted and used to allow deletions and expirations to
remain on disk for a fixed period.

### Collections Enabled

If the DCP producer is collection enabled, all keys with be prefixed with the
collection-ID of the document encoded as a 32-bit [unsigned LEB128(https://en.wikipedia.org/wiki/LEB128)]
value. The key-length field will include the bytes used by the collection-ID.

### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a stream does not exist for the vbucket specfied on this connection.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_ERANGE (0x22)**

If the by sequence number is lower than the current by sequence number contained on the consumer. The by sequence number should always be greater than the one the consumer currently has so if the by sequence number in the mutation message is less than or equal to the by sequence number on the consumer this error is returned.

**(Disconnect)**

If this message is sent to a connection that is not a consumer.
