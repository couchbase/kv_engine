
##Delete With Meta (delwithmeta) v3.5

The delete with meta command is used to delete data with metadata for a key. Meta data passed is cas, sequence number, flags and expiration along with an extended meta data section.

The request:

* Must have extras
* Must have key

####Binary Implementation

    Delete With Meta Binary Request

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       80      |       A8      |       00      |       05      |
      +---------------+---------------+---------------+---------------+
     4|       1D      |       00      |       00      |       03      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       14      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    24|       00      |       00      |       00      |       07      |
      +---------------+---------------+---------------+---------------+
    28|       00      |       00      |       00      |       0A      |
      +---------------+---------------+---------------+---------------+
    32|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    36|       00      |       00      |       00      |       14      |
      +---------------+---------------+---------------+---------------+
    40|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    44|       00      |       00      |       00      |       1E      |
      +---------------+---------------+---------------+---------------+
    48|       00      |       00      |       6D      |       79      |
      +---------------+---------------+---------------+---------------+
    52|       6B      |       65      |       79      |
      +---------------+---------------+---------------+

    DEL_WITH_META command
    Field        (offset) (value)
    Magic        (0)    : 0x80 (Request)
    Opcode       (1)    : 0xA8 (Delete With Meta)
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x1E (30)
    Data type    (5)    : 0x00
    VBucket      (6,7)  : 0x0003 (3)
    Total body   (8-11) : 0x00000023 (35)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              :
      Flags      (24-27): 0x00000007 (7)
      Expiration (28-31): 0x0000000A (10)
      Seqno      (32-39): 0x0000000000000014 (20)
      Cas        (40-47): 0x000000000000001E (30)
      Meta Len   (48-49): 0x0000 (0)
    Key          (50-54): mykey


    Delete With Meta Binary Response

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       81      |       A8      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     4|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       01      |
      +---------------+---------------+---------------+---------------+

    DEL_WITH_META command
    Field        (offset) (value)
    Magic        (0)    : 0x81 (Response)
    Opcode       (1)    : 0xA8 (Delete With Meta)
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000001 (1)


###Extended Meta Data Section

The extended meta data section is used to send extra meta data for a particular mutation. This section should come at the very end, after the key. Its length should be set in the nmeta field. A length of 0 means that there is no extended meta data section.

####Verison 1 (0x01)

In this version the extended meta data section has the following format:

    | version | id_1 | len_1 | field_1 | ... | id_n | len_n | field_n |

**Meta Data IDs:**

* 0x01 - adjusted time
* 0x02 - conflict resolution mode


###Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a the key does not exist.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET (0x07)**

If the vbucket does not exist.

**PROTOCOL_BINARY_RESPONSE_ENOMEM (0x82)**

If the server is permanently out of memory

**PROTOCOL_BINARY_RESPONSE_ETMPFAIL (0x86)**

If the server is currently warming up or we are temporarily out of memory.
