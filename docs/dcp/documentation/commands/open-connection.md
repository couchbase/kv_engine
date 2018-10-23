# Open Connection (opcode 0x50)

Sent by an external entity to a producer or a consumer to create a logical channel.

The request:

* Must have an 8 byte extras section
* Must have key
* Can optionally have a value

Extra looks like:

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| Reserved                                                      |
       +---------------+---------------+---------------+---------------+
      4| flags                                                         |
       +---------------+---------------+---------------+---------------+

Flags are specified as a bitmask in network byte order with the following bits defined:

     0x1 - Type: Producer (bit set) / Consumer (bit cleared).
     0x2 - Type: Notifier (bit set). Note: both bits 1 and 2 should NOT be set.
     0x4 - Include XATTRs. Specifies that DCP_MUTATION and DCP_DELETION messages
           should include any XATTRs associated with the Document.
     0x8 - (No Value) - Specifies that the server should stream only item key
           and metadata in the mutations and not stream the value of the item
     0x10 - Collections - Specifies the client knows about collections, subsequent
            DCP streams will send collection data. Additionally the client may
            set a value in this request containing a JSON document that specifies
            a collections filter.
     0x20 - Include Delete Times - The client wishes to receive extra metadata regarding
            deletes. When a delete is persisted to disk, it is timestamped and purged
            from the vbucket after some interval. When 'include delete times' is enabled,
            deletes which are read from disk will have a timestamp value in the delete-time
            field, in-memory deletes will have a 0 value in the delete-time field. See DCP
            deletion command. Note when enabled on a consumer, the consumer expects the client
            to send the delete-time format DCP delete.

When setting the Producer or Consumer flag the sender is telling the server what type of connection will be created. For example, if the Producer type is set then the sender of the Open Connection message will be a Consumer.

The connection name is specified using the key field. When selecting a name the only requirement is that the name take up no more space than 256 bytes. It is recommended that the name uses that ASCII character set and uses alpha-numeric characters.

The following example shows the breakdown of the message:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x50          | 0x00          | 0x18          |
        +---------------+---------------+---------------+---------------+
       4| 0x08          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x20          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      32| 0x62 ('b')    | 0x75 ('u')    | 0x63 ('c')    | 0x6b ('k')    |
        +---------------+---------------+---------------+---------------+
      36| 0x65 ('e')    | 0x74 ('t')    | 0x73 ('s')    | 0x74 ('t')    |
        +---------------+---------------+---------------+---------------+
      40| 0x72 ('r')    | 0x65 ('e')    | 0x61 ('a')    | 0x6d ('m')    |
        +---------------+---------------+---------------+---------------+
      44| 0x20 (' ')    | 0x76 ('v')    | 0x62 ('b')    | 0x5b ('[')    |
        +---------------+---------------+---------------+---------------+
      48| 0x31 ('1')    | 0x30 ('0')    | 0x30 ('0')    | 0x2d ('-')    |
        +---------------+---------------+---------------+---------------+
      52| 0x31 ('1')    | 0x30 ('0')    | 0x35 ('5')    | 0x5d (']')    |
        +---------------+---------------+---------------+---------------+

    DCP_OPEN command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x50
    Key length   (2,3)  : 0x0018
    Extra length (4)    : 0x08
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000020
    Opaque       (12-15): 0x00000001
    CAS          (16-23): 0x0000000000000000
      seqno      (24-27): 0x00000000
      flags      (28-31): 0x00000000 (consumer)
    Key          (32-55): bucketstream vb[100-105]

Upon success, the following message is returned.

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x50          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+

    DCP_OPEN response
    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x50
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0x00000001
    CAS          (16-23): 0x0000000000000000

**Note:** If the name given in the open connection command is already being used by another established connection then the established connection will be closed and the new connection will be created successfully.

### Collections Filter

If the user specifies 0x10 (collections) in the open flags the user can optionally encode a filter.

#### Collections not enabled

A client that doesn't enable collections will only receive mutations in the default collection. That is
documents written by clients that don't enable collections (for example legacy clients written before Couchbase developed collections).

#### Collections enabled and no filter specified (i.e. no value)

The client receives "unfiltered" data, that is every mutation will be sent to the client.

#### Collections enabled and a filter specified

The value must encode a valid JSON document that specifies the filter information. For example if the bucket
has 10 collections named collection_0 through to collection_10 and the client only wants mutations against
collection_5 and collection_8 the following JSON filter data would be encoded.

```
{
    "collections" : ["collection_5", "collection_8"]
}
```

DCP streams that are created on a filtered DCP channel will have a lifetime of the filtered collections. For example if
collection_5 and collection_8 were deleted, DCP streams would auto close when the last collection is deleted as there's no more data
to send.

### Returns

A status code indicating whether or not the operation was successful.

### Errors

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**(Disconnect)**

If the connection could not be created due to an internal error. Check the server logs if this happens.
