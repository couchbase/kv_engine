# System Event (0x5f)

Tells the consumer that the message contains a system event. The system event
message encodes event information that relates to the user's data, but is not
necessarily something they directly control.

A system event always encodes in the extras the seqno of the event and an
type identifier for the event, the following events are defined (values shown).

* 0 - A collection has been created
* 1 - A collection has been dropped
* 2 - Reserved for future use
* 3 - A scope has been created
* 4 - A scope has been droppped

Each event is free to define if a key or value is present, and the format of each.

The request:
* Must have extras
* May have key
* May have value

The receiver does not reply to this message.


## Extra data for system event:

The extras of a system event encodes

* The seqno at which the event occurred
* The type of the event
* A version for the event data - clients must read the version before assuming
  the value and key format

```
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
```

## Example Create Collection Event

The following example is showing a system event for the creation of
"mycollection" with a collection-uid of 8.

This would occur in response to set_collections receiving a JSON manifest like
the following:

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
      56| 0x02          | 0x00          | 0x00          | 0x00          |
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
    Value        (49-56): 0x0000000000000002 # manifest ID
                 (57-60): 0x00000008 # Collection-ID
                 (61-64): 0x00000000 # Scope-ID
                 (65-68): 0x00011940 # max_ttl


## Key and Value Data Definition

The Key and Value of the message are defined differently for each event.

### Create Collection

__Key__: A create collection system event with version 0 or 1 has a key which is the
name of the created collection.

__Value__: version 0 and version 1 values are defined.

#### version 0
* The ID of last completely processed manifest as a 8-byte integer (network endian)
* The Scope-ID that the new collection belongs to as a 4-byte integer (network endian)
* The ID of the new collection as a 4-byte integer (network endian)

```
    struct collection_create_event_data_version0 {
        uint64_t manifest_uid;
        uint32_t scope_id;
        uint32_t collection_id;
    }
```

#### version 1

* The same data as version 0 and
* The max_ttl value of the collection as 4-byte integer  (network endian)

```
    struct collection_create_event_data_version1 {
        uint64_t manifest_uid;
        uint32_t scope_id;
        uint32_t collection_id;
        uint32_t max_ttl;
    }
```

### Drop/Flush Collection

__Key__: A drop collection system event has no key.

__Value__: A drop or flush collection system event with version 0 contains:

* The ID of last completely processed manifest as a 8-byte integer (network endian)
* The Scope-ID that the dropped collection belonged to as a 4-byte integer (network endian)
* The ID of the dropped collection as a 4-byte integer (network endian)

```
    struct collection_drop_event_data_version0 {
        uint64_t manifest_uid;
        uint32_t scope_id;
        uint32_t collection_id;
    }
```

### Create Scope

__Key__: A create scope system event has a key which is the name of the new scope.

__Value__: Only version 0 value is defined.

* The ID of last completely processed manifest as a 8-byte integer (network endian)
* The ID of the new scope as a 4-byte integer (network endian)

```
    struct scope_create_event_data_version0 {
        uint64_t manifest_uid;
        uint32_t scope_id;
    }
```

### Drop Scope

__Key__: A drop scope system event has no key.

__Value__: Only version 0 value is defined.

* The ID of last completely processed manifest as a 8-byte integer (network endian)
* The ID of the dropped scope as a 4-byte integer (network endian)

```
    struct scope_drop_event_data_version0 {
       uint64_t manifest_uid;
       uint32_t scope_id;
    }
```
### Manifest ID

Each of the system events contains a manifest-ID which is generally intended for
use by KV replication. The manifest-ID field stores the ID of the last completely
processed manifest.

For example consider that the bucket has a current manifest (simplified view used):

```
manifest-id: 10
collections[a, b, c]
```

Now when the next collection manifest is processed by the bucket, the new manifest
can result in one of more system events being generated.

For example if the next manifest is:
```
manifest-id: 11
collections[a, b, c, d, e]
```

This creates two new collections (d and e) and two system events are generated by
each vbucket with increasing sequence numbers. Only the final system-event
generated will be 'stamped' with manifest-id 11. For example:

* create-collection "e" seqno 200, manifest-id 10
* create-collection "d" seqno 202, manifest-id 11

The manifest ID is primarily for use by memcached replication to better handle
an interruption during the processing of a new manifest that introduces multiple
changes. By only setting the manifest-ID to 11 on the final event, we do not
assume that the vbucket has reached manifest-ID 11 if it has yet to process
collection d.

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
