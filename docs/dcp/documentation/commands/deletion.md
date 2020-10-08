### Deletion (0x58)

Tells the consumer that the message contains a key deletion. Two variants of a
deletion packet exist. The original 'V1' version and an updated 'V2' which
carries additional meta-data. The two variants are described throughout this
document as V1 and V2.

The request:
* Must have extras
* Must have key
* Must not have value

V1 Extra looks like:

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
     16| Extended Metadata (nmeta)     |
       +---------------+---------------*
       Total 18 bytes

The 'metadata' is located after the item's key.

V2 Extra looks like:

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
       +---------------+-----------------------------------------------+
     20| unused        |
       +---------------*
       Total 21 bytes

The client should not send a reply to this command. The following example shows
the breakdown of the message (V1 shown):

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x58          | 0x00          | 0x05          |
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
      40| 0x00          | 0x00          | 0x01          | 0x63 ('c')    |
        +---------------+---------------+---------------+---------------+
      44| 0x3a (':')    | 0x3a (':')    | 0x68 ('h')    | 0x65 ('e')    |
        +---------------+---------------+---------------+---------------+
      48| 0x6c ('l')    | 0x6c ('l')    | 0x6f ('o')    |
        +---------------+---------------+---------------+
    DCP_DELETION command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x58
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
    Key          (43-50): hello

### Returns

This message will not return a response unless an error occurs.

### The meaning of datatype

The datatype field of the `DCP_DELETION` describes the Value payload of the message
and not the datatype of the original document. This is important to consider when a
producer is opened with any combination of the no value or include xattr flags.

* [Datatype bits ](../../../BinaryProtocol.md#data-types)
* DCP without the value or xattr flags - datatype will never contain XATTR, but
  could be a combination of 0, JSON. If the client has enabled compression, the
  SNAPPY datatype bit may also be present.
* `DCP_OPEN_NO_VALUE` - datatype will always be 0
* `DCP_OPEN_NO_VALUE|DCP_OPEN_INCLUDE_XATTRS` - datatype will be 0 or XATTR. If
   the client has enabled compression, the SNAPPY datatype bit may also be present.
* `DCP_OPEN_INCLUDE_XATTRS` - datatype could be many combinations. e.g. 0, XATTR
  or XATTR|JSON. If the client has enabled compression, the SNAPPY datatype bit
  may also be present.

### Extended Meta Data Section

The extended meta data section is used to send extra meta data for a particular deletion. This section is at the very end, after the value. Its length will be set in the nmeta field.
* [**Ext_Meta**](extended_meta/ext_meta_ver1.md)

### Collections Enabled

If the DCP producer is collection enabled, all keys with be prefixed with the
collection-ID of the document encoded as a 32-bit [unsigned LEB128(https://en.wikipedia.org/wiki/LEB128)]
value. The key-length field will include the bytes used by the collection-ID.

### Delete Time

If the DCP channel is opened with 'include delete-times' then all deletions sent
will use the V2 format.

The delete time field is used to time stamp when the item was deleted. This
field is mainly of use to clients building a replica of an active vbucket as
the delete time is persisted and used to allow deletions to remain on disk for
a fixed period.

### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a stream does not exist for the vbucket specfied on this connection.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_ERANGE (0x22)**

If the by sequence number is lower than the current by sequence number contained on the consumer. The by sequence number should always be greater than the one the consumer currently has so if the by sequence number in the mutation message is less than or equal to the by sequence number on the consumer this error is returned.

**(Disconnect)**

If this message is sent to a connection that is not a consumer.
