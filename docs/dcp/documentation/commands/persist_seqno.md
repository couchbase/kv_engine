
## Persist Sequence Number

The Persist Sequence Number command can be used to figure out when an item with a specific Sequence Number/VBucket UUID pair for a given VBucket is persisted into Couchbase.

#### Binary Implementation

    Persist Sequence Number Binary Request

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       80      |       B7      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     4|       08      |       00      |       00      |       0C      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       08      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    24|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    28|       DE      |       AD      |       BE      |       EF      |
      +---------------+---------------+---------------+---------------+
    32|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    36|       FE      |       ED      |       DE      |       CA      |
      +---------------+---------------+---------------+---------------+


    Header breakdown
    Persist Sequence Number command
    Field        (offset) (value)
    Magic        (0)    : 0x80                (Request)
    Opcode       (1)    : 0xB7                (Persist Sequence Number)
    Key length   (2,3)  : 0x000C              (Field not used)
    Extra length (4)    : 0x08                (8)
    Data type    (5)    : 0x00                (Field not used)
    VBucket      (6,7)  : 0x000C              (12)
    Total body   (8-11) : 0x00000008          (8)
    Opaque       (12-15): 0x00000000          (Field not used)
    CAS          (16-23): 0x0000000000000000  (Field not used)
	Seqno        (24-31): 0x00000000feeddeca  (4277001930)

    Persist Sequence Number Binary Response

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       81      |       B7      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     4|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+

    Header breakdown
    Takeover Stream command
    Field        (offset) (value)
    Magic        (0)    : 0x81 	              (Response)
    Opcode       (1)    : 0xB7                (Persist Sequence Number)
    Key length   (2,3)  : 0x0000              (Field not used)
    Extra length (4)    : 0x00                (0)
    Data type    (5)    : 0x00                (Field not used)
    Status       (6,7)  : 0x0000              (Success)
    Total body   (8-11) : 0x00000000          (0)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000  (Field not used)

##### Extra Fields

**Seqno**

Each item in Couchbase is assigned a monotinically increasing Sequence Number. This field should contain the sequence number of the item you want to have persisted. Note that all items with a sequence number below this one will be persisted as well.

##### Returns

A status code indicating whether or not the operation was successful

##### Errors

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET (0x07)**

If the consumer does not have the given VBucket then this error is returned.

**PROTOCOL_BINARY_RESPONSE_ETMPFAIL (0x86)**

If the operation does not complete within a certain amount of time this error code indicates a timeout.

##### Use Cases

The Persist Sequence Number command can be used in order to make sure that all up to a specified sequence number have been persisted to disk. This is useful when doing a rebalance ince before moving to the next VBucket we need to make sure that all items have been durable replicated to another node.


