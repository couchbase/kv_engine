# Document

This document describes the properties for a document stored in Couchbase

## item_info

The `item_info` structure is used to pass document properties between
the underlying engine and the memcached core. The underlying engine may
choose a different format for its internal storage.

### CAS value

The CAS value for a document is a 64 bit value which is unique for each
mutation of a document (It is not guaranteed to be unique across documents
identified by a differerent name). It is used to uniquely refer to a given
version of a document.

The client should never try to assume anything by the CAS value apart from
that if the CAS value hasn't changed no other properties of the docmuent
has changed. If they differ, one or more properties has changed and the
CAS value won't tell you which one is the newest.

The CAS value is sent to the client in *network byte order*, so that both
a client and the server can log it in a deterministic way.

If the document is locked, a CAS value of -1 is used (we don't
reveal the real cas value of the document to the user). Trying to
acquire the lock generates a new CAS value (so that no one else know
the real cas value and may unlock the object by referencing its cas
value).

### VBucket UUID

The vbucket UUID is a 64 bit identifier which uniquely identifies the version
of a vbucket this document belongs to

### Sequence number

The sequence number is a 64 bit number which identifies this mutation within
the vbucket (with the vbucket uuid)

### Expiry time

The exiration time is a 32 bit value representing the number of seconds from
process startup for when the document will expire. The value *0* means never
expire.

### Number of bytes

This 32 bit number contains the _entire_ size of the blob of data stored
for the document. The blob may contain more than just the actual document
value (like extended attributes). The presence of other other fields being
part of this value is signalled through the datatype

### Flags

This 32 bit value contains the user specified flags

### Datatype

This 8 bit bitmask contains meta information about the document. See
[Data Types](BinaryProtocol.md#data-types) in the binary protocol.

### DocumentState

The DocumentState enum may hold two values:

Deleted - The document is deleted from the users perspective, and trying
to fetch the document will return KEY_NOT_FOUND unless one asks specifically
for deleted documents. The Deleted documents will not hang around forever
and may be reaped by the purger at any time (from the core's perspective.
That's an internal detail within the underlying engine).

Alive - There is nothing special with this document, and all access to
it should work just like it always did.

### nkey

This 16 bit number contains the number of bytes in the key

### key

This points to an array of bytes containing the key

### value

This is a single entry of an iovector containing the documents blob

## In memory layout of the document blob

As mentioned above the actual document blob may contain more information
than the document value, and is signalled through the datatype bitmask.

You should _always_ handle the bits from the least significant bit and
upwards as thats the order they will be allocated (and we don't know how
we'll change stuff in the future in the case where we encode more information
in the document blob).

### JSON

This bit means that the _document_ value is identified as JSON

### Snappy Compressed

This bit means that the _entire_ blob is compressed by using Snappy
compression.

### XAttr - extended attributes

All length fields are stored in memory in network byte order so that
the in-memory representation of the xattrs map directly to the
"on the wire" format used over DCP.

When the XATTR bit is set the first `uint32_t` in the body contains the
size of the entire XATTR section.


      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Total xattr length in network byte order                      |
        +---------------+---------------+---------------+---------------+

Following the length you'll find all of the xattr key-value pairs with
the following encoding:

    uint32_t length of next xattr pair
       xattr key in modified UTF-8
       0x00
       xattr value in modified UTF-8
       0x00

The 0x00 byte after the key saves us from storing a key length,
and the trailing 0x00 is just for convenience to allow us to use
string functions to search in them.

Example

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x00          | 0x00          | 0x00          | 0x72          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x21          |
        +---------------+---------------+---------------+---------------+
       8| 0x5f ('_')    | 0x73 ('s')    | 0x79 ('y')    | 0x6e ('n')    |
        +---------------+---------------+---------------+---------------+
      12| 0x63 ('c')    | 0x00          | 0x7b ('{')    | 0x22 ('"')    |
        +---------------+---------------+---------------+---------------+
      16| 0x63 ('c')    | 0x61 ('a')    | 0x73 ('s')    | 0x22 ('"')    |
        +---------------+---------------+---------------+---------------+
      20| 0x3a (':')    | 0x22 ('"')    | 0x64 ('d')    | 0x65 ('e')    |
        +---------------+---------------+---------------+---------------+
      24| 0x61 ('a')    | 0x64 ('d')    | 0x62 ('b')    | 0x65 ('e')    |
        +---------------+---------------+---------------+---------------+
      28| 0x65 ('e')    | 0x66 ('f')    | 0x63 ('c')    | 0x61 ('a')    |
        +---------------+---------------+---------------+---------------+
      32| 0x66 ('f')    | 0x65 ('e')    | 0x66 ('f')    | 0x65 ('e')    |
        +---------------+---------------+---------------+---------------+
      36| 0x65 ('e')    | 0x64 ('d')    | 0x22 ('"')    | 0x7d ('}')    |
        +---------------+---------------+---------------+---------------+
      40| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      44| 0x49          | 0x6d ('m')    | 0x65 ('e')    | 0x74 ('t')    |
        +---------------+---------------+---------------+---------------+
      48| 0x61 ('a')    | 0x00          | 0x7b ('{')    | 0x22 ('"')    |
        +---------------+---------------+---------------+---------------+
      52| 0x61 ('a')    | 0x75 ('u')    | 0x74 ('t')    | 0x68 ('h')    |
        +---------------+---------------+---------------+---------------+
      56| 0x6f ('o')    | 0x72 ('r')    | 0x22 ('"')    | 0x3a (':')    |
        +---------------+---------------+---------------+---------------+
      60| 0x22 ('"')    | 0x54 ('T')    | 0x72 ('r')    | 0x6f ('o')    |
        +---------------+---------------+---------------+---------------+
      64| 0x6e ('n')    | 0x64 ('d')    | 0x20 (' ')    | 0x4e ('N')    |
        +---------------+---------------+---------------+---------------+
      68| 0x6f ('o')    | 0x72 ('r')    | 0x62 ('b')    | 0x79 ('y')    |
        +---------------+---------------+---------------+---------------+
      72| 0x65 ('e')    | 0x22 ('"')    | 0x2c (',')    | 0x22 ('"')    |
        +---------------+---------------+---------------+---------------+
      76| 0x63 ('c')    | 0x6f ('o')    | 0x6e ('n')    | 0x74 ('t')    |
        +---------------+---------------+---------------+---------------+
      80| 0x65 ('e')    | 0x6e ('n')    | 0x74 ('t')    | 0x2d ('-')    |
        +---------------+---------------+---------------+---------------+
      84| 0x74 ('t')    | 0x79 ('y')    | 0x70 ('p')    | 0x65 ('e')    |
        +---------------+---------------+---------------+---------------+
      88| 0x22 ('"')    | 0x3a (':')    | 0x22 ('"')    | 0x61 ('a')    |
        +---------------+---------------+---------------+---------------+
      92| 0x70 ('p')    | 0x70 ('p')    | 0x6c ('l')    | 0x69 ('i')    |
        +---------------+---------------+---------------+---------------+
      96| 0x63 ('c')    | 0x61 ('a')    | 0x74 ('t')    | 0x69 ('i')    |
        +---------------+---------------+---------------+---------------+
     100| 0x6f ('o')    | 0x6e ('n')    | 0x2f ('/')    | 0x6f ('o')    |
        +---------------+---------------+---------------+---------------+
     104| 0x63 ('c')    | 0x74 ('t')    | 0x65 ('e')    | 0x74 ('t')    |
        +---------------+---------------+---------------+---------------+
     108| 0x2d ('-')    | 0x73 ('s')    | 0x74 ('t')    | 0x72 ('r')    |
        +---------------+---------------+---------------+---------------+
     112| 0x65 ('e')    | 0x61 ('a')    | 0x6d ('m')    | 0x22 ('"')    |
        +---------------+---------------+---------------+---------------+
     116| 0x7d ('}')    | 0x00          |
        +---------------+---------------+

     _sync with the value of {"cas":"deadbeefcafefeed"}
     meta with the value of {"author":"Trond Norbye","content-type":"application/octet-stream"}"
