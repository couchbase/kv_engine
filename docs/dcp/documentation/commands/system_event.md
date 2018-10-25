### System Event (0x5f)

Tells the consumer that the message contains a system event. The system event
message encodes event information that relates to the user's data, but is not
necessarily something they directly control.

The primary use-case for system events is collections, and thus the following
system events can occur:

* Collection creation.
* Collection drop.
* Collection flush (not yet supported).

A system event always encodes in the extras the seqno of the event and an
identifier for the event, the following events are defined (values shown).

* 0 - A collection has been created
* 1 - A collection has been dropped
* 2 - A collection has been flushed (not yet supported)

Each event is free to define if a key or value is present, and the format of each.


The request:
* Must have extras
* May have key
* May have value


Extra data for system event:

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| by_seqno ...                                                  |
       +---------------+---------------+---------------+---------------+
      4| ... by_seqno                                                  |
       +---------------+---------------+---------------+---------------+
      8| event                                                         |
       +---------------+---------------+---------------+---------------+
     12| version       |
       +---------------+
       Total 13 bytes

The consumer should not send a reply to this command.

### Example

The following example is showing a system event for the creation of
"mycollection" with a collection-uid of 8.

This would occur in response to set_collections receiving a JSON manifest as:

```
{
   "uid":"2",
   "scopes":[
      {
         "uid":"0",
         "name":"_default",
         "collections":[
            {
               "uid":"8",
               "name":"mycollection",
               "max_ttl" : 72000
            }
         ]
      }
   ]
}
```

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x5f          | 0x00          | 0x0c          |
        +---------------+---------------+---------------+---------------+
       4| 0x0D          | 0x00          | 0x02          | 0x10          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x2d          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x12          | 0x10          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x00          | 0x04          |
        +---------------+---------------+---------------+---------------+
      32| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      36| 0x01          | 0x6d ('m')    | 0x79 ('y')    | 0x63 ('c')    |
        +---------------+---------------+---------------+---------------+
      40| 0x6f ('o')    | 0x6c ('l')    | 0x6c ('l')    | 0x65 ('e')    |
        +---------------+---------------+---------------+---------------+
      44| 0x63 ('c')    | 0x74 ('t')    | 0x69 ('i')    | 0x6f ('o')    |
        +---------------+---------------+---------------+---------------+
      48| 0x6e ('n')    | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      52| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      56| 0x05          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      60| 0x08          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      64| 0x00          | 0x00          | 0x01          | 0x19          |
        +---------------+---------------+---------------+---------------+
      68| 0x40          |
        +---------------+
      Total 69 bytes (24 byte header + 13 byte extras + 12 byte key + 20 byte value)


          DCP_SYSTEM_EVENT command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x5F
    Key length   (2,3)  : 0x000C
    Extra length (4)    : 0x0D
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0210
    Total body   (8-11) : 0x0000002D
    Opaque       (12-15): 0x00001210
    CAS          (16-23): 0x0000000000000000
     by seqno    (24-31): 0x0000000000000004
     event       (32-35): 0x00000000
     version     (36):    0x01
    Key          (37-48): mycollection
    Value        (49-56): 0x0000000000000005 # manifest ID
                 (57-60): 0x00000008 # Collection-ID
                 (61-64): 0x00000000 # Scope-ID
                 (65-68): 0x00011940 # max_ttl



### Extras

The extras of a system event encodes

* The seqno at which the event occurred
* The id of the event
* A version for the event data - clients must read the version before assuming
  the value and key format.

### Value Data Definition

#### Create Collection

A create collection system event with version 0 or 1 has a key which is the name of
the created collection.

A create collection system event with version 0 contains a 16 byte value.
A create collection system event with version 1 contains a 20 byte value.


version 0:
* The ID of the manifest which generated the collection as a 8-byte integer (network endian)
* The ID of the new collection as a 4-byte integer (network endian)
* The Scope-ID that the new collection belongs to as a 4-byte integer (network endian)

version 1:
Contains version 0 +

* The max_ttl value of the collection as 4-byte integer  (network endian)

```
    struct collection_create_event_data_version0 {
       uint64_t manifest_uid;
       uint32_t scope_id;
       uint32_t collection_id;
    }

    struct collection_create_event_data_version1 {
       uint64_t manifest_uid;
       uint32_t scope_id;
       uint32_t collection_id;
       uint32_t max_ttl;
    }
```

#### Drop/Flush Collection

A drop or flush collection system event with version 0 contains:

* The ID of the manifest which dropped the collection as a 8-byte integer (network endian)
* The ID of the dropped collection as a 4-byte integer (network endian)

Note drop collection does not encode a collection name.

```
    struct collection_drop_event_data_version0 {
       uint64_t manifest_uid;
       uint32_t collection_id;
    }
```

### Returns

This message will not return a response unless an error occurs.

### Errors (from KV DCP consumer)

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a stream does not exist for the vbucket specfied on this connection.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_ERANGE (0x22)**

If the by sequence number is lower than the current by sequence number contained
on the consumer. The by sequence number should always be greater than the one
the consumer currently has so if the by sequence number in the message is less
than or equal to the by sequence number on the consumer this error is returned.

**(Disconnect)**

If this message is sent to a connection that is not a consumer.
