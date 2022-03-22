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

* 0x1: __DCP type__ requests a producer is created, if this bit is clear a consumer
  is created.
* 0x2: ___Invalid___ Should always be clear.
* 0x4: __Include XATTRs__ requests that DCP_MUTATION, DCP_DELETION and DCP_EXPIRATION
  (if [enabled](control.md)) messages should include any XATTRs associated with the Document.
* 0x8: __No value__ requests that DCP_MUTATION, DCP_DELETION and DCP_EXPIRATION
  (if [enabled](control.md)) messages do not include the Document.
* 0x20:  __Include delete times__ requests that DCP_DELETION messages include metadata
  regarding when a document was deleted. When a delete is persisted to disk, it
  is timestamped and purged from the vbucket after some interval. When 'include
  delete times' is enabled, deletes which are read from disk will have a
  timestamp value in the delete-time field, in-memory deletes will have a 0
  value in the delete-time field. See DCP deletion command. Note when enabled on
  a consumer, the consumer expects the client to send the delete-time format DCP
  delete.
* 0x40: __No value with underlying datatype__. Requests that the server preserves the original
  datatype when it strips off the document value.
* 0x100: __Include deleted user xattrs__. Requests that the server includes the document
  User Xattrs within deletion values.

When setting the Producer or Consumer flag the sender is telling the server what type of connection will be created. For example, if the Producer type is set then the sender of the Open Connection message will be a Consumer.

The connection name is specified using the key field. When selecting a name the only requirement is that the name take up no more space than 256 bytes. It is recommended that the name uses that ASCII character set and uses alpha-numeric characters. It is highly advantageous for improved supportability Couchbase Server that the connection names embed as much contextual information as possible from the client.

As of version 6.5, the _value_ can be used to specify additional information
about the connection to be opened. If non-empty, the value must be a JSON
object with the following supported keys:

* `consumer_name` The name the DCP consumer should use to identify itself with
   the associated DCP producer. Only valid if `DCP type` is `Consumer`.


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

## Collections

If the connection has enabled the collections feature, then the DCP producer or consumer will be collection enabled.

* A DCP producer with collections enabled is the only way to access documents
  that are not in the default collection.
* A DCP consumer with collections enabled can accept mutations, deletions and
  expirations from a collection aware producer.

### Returns

A status code indicating whether or not the operation was successful.

### Errors

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**(Disconnect)**

If the connection could not be created due to an internal error. Check the server logs if this happens.
