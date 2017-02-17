
##Get Meta (getmeta) v5.0

The get meta command is used to fetch the meta data for a key.

The request:

* Must have a key
* Can have 1 byte extras.

The Get Meta command on success will return the document's

* Deleted flag (indicating if the document deleted).
* Flags
* Expiry time
* Sequence number

### Extras - version1

When the 1 byte extras field contains 0x01...

Prior to v4.6 specifiying version 1 in the extras would mean that GET META would return the document's conflict resolution mode.

v4.6 removed conflict resolution mode and GET META now ignores any V1 requests.

### Extras - version2

When the 1 byte extras field contains 0x02...

v5.0 adds the ability to request the datatype of the document by setting extras to 0x02.

* Returns a 1 byte datatype field in the response

####Binary Implementation

    Get Meta Binary Request

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       80      |       A0      |       00      |       05      |
      +---------------+---------------+---------------+---------------+
     4|       01      |       00      |       00      |       03      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       06      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    24|       01      |       6D      |       79      |       6B      |
      +---------------+---------------+---------------+---------------+
    28|       65      |       79      |
      +---------------+---------------+

    GET_META command
    Field        (offset) (value)
    Magic        (0)    : 0x80 (Request)
    Opcode       (1)    : 0xA0 (Get Meta)
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x01 (1)
    Data type    (5)    : 0x00
    VBucket      (6,7)  : 0x0003 (3)
    Total body   (8-11) : 0x00000006 (6)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              :
      ReqExtMeta (24)   : 0x02 (2)
    Key          (25-29): mykey

    Get Meta Binary Response

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       81      |       A0      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     4|       15      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       15      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       01      |
      +---------------+---------------+---------------+---------------+
    24|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    28|       00      |       00      |       00      |       01      |
      +---------------+---------------+---------------+---------------+
    32|       00      |       00      |       00      |       07      |
      +---------------+---------------+---------------+---------------+
    36|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    40|       00      |       00      |       00      |       09      |
      +---------------+---------------+---------------+---------------+
    44|       01      |
      +---------------+

    GET_META command
    Field        (offset) (value)
    Magic        (0)    : 0x81 (Response)
    Opcode       (1)    : 0xA0 (Get Meta)
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x15 (21)
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x00000015 (21)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000001 (1)
    Extras              :
      Deleted    (24-27): 0x00000000 (0)
      Flags      (28-31): 0x00000001 (1)
      Exptime    (32-35): 0x00000007 (7)
      Seqno      (36-43): 0x0000000000000009 (9)
      Datatype   (44)   : 0x03 (3)

###Extended Meta Data Section

The extras section in the response packet may contain 1 extra byte indicating the document's datatype. This 1 byte of extra meta information will be sent as part of the response only if the ReqExtMeta flag is sent and set to 0x02.

#### Datatype

The datatype byte indicates the document's type using a combination of the following flags.
* `PROTOCOL_BINARY_RAW_BYTES` = 0
* `PROTOCOL_BINARY_DATATYPE_JSON` = 1
* `PROTOCOL_BINARY_DATATYPE_COMPRESSED` = 2
* `PROTOCOL_BINARY_DATATYPE_XATTR` = 4

Thus a compressed JSON document would have the datatype of 0x03.

###Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a the key does not exist.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET (0x07)**

If the vbucket does not exist.
