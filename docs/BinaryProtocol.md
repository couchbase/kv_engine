# Couchbase Binary Protocol

## Introduction
Couchbase is a high performance NoSQL document store that has evolved from the
combination of [Memcached](http://memcached.org) and CouchDB. The Couchbase
binary protocol is based off of the Memcached binary protocol.

This document has been adapted from the Memcached 'Binary Protocol Revamped'
document and may make reference to parts of the protocol not implemented by
Couchbase Server.

### Conventions used in this document
The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be
interpreted as described in [RFC 2119](http://tools.ietf.org/html/rfc2119).

## Packet Structure
General format of a packet:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| HEADER                                                        |
        |                                                               |
        |                                                               |
        |                                                               |
        +---------------+---------------+---------------+---------------+
      24| COMMAND-SPECIFIC EXTRAS (as needed)                           |
        |  (note length in the extras length header field)              |
        +---------------+---------------+---------------+---------------+
       m| Key (as needed)                                               |
        |  (note length in key length header field)                     |
        +---------------+---------------+---------------+---------------+
       n| Value (as needed)                                             |
        |  (note length is total body length header field, minus        |
        |   sum of the extras and key length body fields)               |
        +---------------+---------------+---------------+---------------+
        Total 24 + x bytes (24 byte header, and x byte body)

### Request header

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Magic         | Opcode        | Key length                    |
        +---------------+---------------+---------------+---------------+
       4| Extras length | Data type     | vbucket id                    |
        +---------------+---------------+---------------+---------------+
       8| Total body length                                             |
        +---------------+---------------+---------------+---------------+
      12| Opaque                                                        |
        +---------------+---------------+---------------+---------------+
      16| CAS                                                           |
        |                                                               |
        +---------------+---------------+---------------+---------------+
        Total 24 bytes


#### Request header with "flexible framing extras"

Some commands may accept extra attributes which may be set in the
flexible framing extras section in the request packet. Such packets
is identified by using a different magic (0x08 intead of 0x80).
If enabled the header looks like:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Magic (0x08)  | Opcode        | Framing extras| Key Length    |
        +---------------+---------------+---------------+---------------+
       4| Extras length | Data type     | vbucket id                    |
        +---------------+---------------+---------------+---------------+
       8| Total body length                                             |
        +---------------+---------------+---------------+---------------+
      12| Opaque                                                        |
        +---------------+---------------+---------------+---------------+
      16| CAS                                                           |
        |                                                               |
        +---------------+---------------+---------------+---------------+
        Total 24 bytes

Following the header you'd now find the section containing the framing
extras (the size is specified in byte 2). Following the framing extras you'll
find the extras, then the key and finally the value. The size of the value
is total body length - key length - extras length - framing extras.

The framing extras is encoded as a series of variable-length `FrameInfo` objects.

Each `FrameInfo` consists of:

* 4 bits: *Object Identifier*. Encodes first 15 object IDs directly; with the 16th value (15) used
   as an escape to support an additional 256 IDs by combining the value of the next byte:
   * `0..14`: Identifier for this element.
   * `15`: Escape: ID is 15 + value of next byte.
* 4 bits: *Object Length*. Encodes sizes 0..14 directly; value 15 is
   used to encode sizes above 14 by combining the value of a following
   byte:
   * `0..14`: Size in bytes of the element data.
   * `15`: Escape: Size is 15 + value of next byte (after any object ID
   escape bytes).
* N Bytes: *Object data*.

##### ID:0 - reorder

Allow this command to be reordered. FrameInfo encoded as:

    Byte/     0       |
       /              |
      |0 1 2 3 4 5 6 7|
      +---------------+
     0|  ID:0 | Len:0 |

##### ID:1 - Durability Requirement

This command contains durability requirements. FrameInfo encoded as:

    Byte/     0           |
       /                  |
      |0 1 2 3 4 5 6 7    |
      +-------------------+
     0|  ID:1 | Len:1 or 3|


The size of the durability requirement is variable length. The first byte
contains the durability level by using the following table:

    0x01 = majority
    0x02 = majority and persist on master
    0x03 = persist to majority

The (optional) 2nd and 3rd byte contains the timeout specified in
milliseconds (network byte order). If omitted the default
timeout value configured on the server will be used.

##### ID:2 - DCP stream-ID

This command contains a DCP stream-ID as per the stream-request which created
the stream.

    Byte/     0       |
       /              |
      |0 1 2 3 4 5 6 7|
      +---------------+
     0|  ID:2 | Len:2 |

The 2nd and 3rd byte contain a network byte order (uint16) storing the stream
ID value which was specified in the DCP stream-request that created the stream.


### Response header

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Magic         | Opcode        | Key Length                    |
        +---------------+---------------+---------------+---------------+
       4| Extras length | Data type     | Status                        |
        +---------------+---------------+---------------+---------------+
       8| Total body length                                             |
        +---------------+---------------+---------------+---------------+
      12| Opaque                                                        |
        +---------------+---------------+---------------+---------------+
      16| CAS                                                           |
        |                                                               |
        +---------------+---------------+---------------+---------------+
        Total 24 bytes

#### Response header with "flexible framing extras"

When enabled by the client (via HELLO) the server is allowed to inject
flexible framing extensions in the response packet.

When the server decides to inject extra data in the packet, it does
so by using a different value for magic (0x18 instead of 0x81), which
tells the receiver that the current header looks like:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Magic (0x18)  | Opcode        | Framing extras| Key Length    |
        +---------------+---------------+---------------+---------------+
       4| Extras length | Data type     | Status                        |
        +---------------+---------------+---------------+---------------+
       8| Total body length                                             |
        +---------------+---------------+---------------+---------------+
      12| Opaque                                                        |
        +---------------+---------------+---------------+---------------+
      16| CAS                                                           |
        |                                                               |
        +---------------+---------------+---------------+---------------+
        Total 24 bytes

Following the header you'd now find the section containing the framing
extras (the size is specified in byte 2). Following the framing extras you'll
find the extras, then the key and finally the value. The size of the value
is total body length - key length - extras length - framing extras.

The framing extras is encoded as a series of variable-length `FrameInfo` objects.

Each `FrameInfo` consists of:

* 4 bits: *Object Identifier*. Encodes first 15 object IDs directly; with the 16th value (15) used
   as an escape to support an additional 256 IDs by combining the value of the next byte:
   * `0..14`: Identifier for this element.
   * `15`: Escape: ID is 15 + value of next byte.
* 4 bits: *Object Length*. Encodes sizes 0..14 directly; value 15 is
   used to encode sizes above 14 by combining the value of a following
   byte:
   * `0..14`: Size in bytes of the element data.
   * `15`: Escape: Size is 15 + value of next byte (after any object ID
   escape bytes).
* N Bytes: *Object data*.

For V1, only one object identifier is defined:

##### ID:0 - Server Recv->Send duration

Time (in microseconds) server spent on the operation. Measured from
receiving header from OS to when response given to OS.
Size: 2 bytes; encoded as variable-precision value (see below)

FrameInfo encoded as:

    Byte/     0       |       1       |       2       |
       /              |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+
     0|  ID:0 | Len:2 |  Server Recv->Send Duration   |

The duration in micros is encoded as:

    encoded =  (micros * 2) ^ (1.0 / 1.74)
    decoded =  (encoded ^ 1.74) / 2

### Header fields description

* Magic: Magic number identifying the package (See [Magic_Byte](#magic-byte))
* Opcode: Command code (See [Command Opcodes](#command-opcodes)
* Key length: Length in bytes of the text key that follows the command extras
* vbucket id (Request only): The virtual bucket for this command
* Status (Response Only): Status of the response (non-zero on error) (See
[Response Status](#response-status))
* Extras length: Length in bytes of the command extras
* Data type: Reserved for future use (See [Data Types](#data-types))
* Total body length: Length in bytes of extra + key + value
* Opaque: Will be copied back to you in the response
* CAS: Data version check

## Defined Values

### Magic Byte

| Raw  | Description                               |
| -----|-------------------------------------------|
| 0x80 | Request packet from client to server      |
| 0x81 | Response packet from server to client     |
| 0x18 | Response packet containing flex extras    |
| 0x82 | Request packet from server to client      |
| 0x83 | Response packet from client to server     |

Magic byte / version. For each version of the protocol, we'll use a different
request/response value pair. This is useful for protocol analyzers to
distinguish the nature of the packet from the direction which it is moving.
Note, it is common to run a memcached instance on a host that also runs an
application server. Such a host will both send and receive memcache packets.

The version should hopefully correspond only to different meanings of the
command byte. In an ideal world, we will not change the header format. As
reserved bytes are given defined meaning, the protocol version / magic byte
values should be incremented.

Traffic analysis tools are encouraged to identify memcache packets and provide
detailed interpretation if the magic bytes are recognized and otherwise to
provide a generic breakdown of the packet. Note, that the key and value
positions can always be identified even if the magic byte or command opcode are
not recognized.

### Response Status

Possible values of this two-byte field:

| Raw    | Description                           |
| -------|---------------------------------------|
| 0x0000 | No error                              |
| 0x0001 | Key not found                         |
| 0x0002 | Key exists                            |
| 0x0003 | Value too large                       |
| 0x0004 | Invalid arguments                     |
| 0x0005 | Item not stored                       |
| 0x0006 | Incr/Decr on a non-numeric value      |
| 0x0007 | The vbucket belongs to another server |
| 0x0008 | The connection is not connected to a bucket |
| 0x001f | The authentication context is stale, please re-authenticate |
| 0x0020 | Authentication error                  |
| 0x0021 | Authentication continue               |
| 0x0022 | The requested value is outside the legal ranges |
| 0x0023 | Rollback required                     |
| 0x0024 | No access                             |
| 0x0025 | The node is being initialized         |
| 0x0081 | Unknown command                       |
| 0x0082 | Out of memory                         |
| 0x0083 | Not supported                         |
| 0x0084 | Internal error                        |
| 0x0085 | Busy                                  |
| 0x0086 | Temporary failure                     |
| 0x0087 | XATTR invalid syntax                  |
| 0x0088 | Unknown collection                    |
| 0x0089 | No collections manifest               |
| 0x008a | Collections manifest not applied      |
| 0x008b | Client collections manifest ahead     |
| 0x008c | Unknown scope                         |
| 0x00a0 | Durability level invalid |
| 0x00a1 | Durability impossible |
| 0x00a2 | Synchronous write in progress |
| 0x00a3 | Synchronous write ambiguous |
| 0x00c0 | (Subdoc) The provided path does not exist in the document |
| 0x00c1 | (Subdoc) One of path components treats a non-dictionary as a dictionary, or a non-array as an array|
| 0x00c2 | (Subdoc) The path’s syntax was incorrect |
| 0x00c3 | (Subdoc) The path provided is too large; either the string is too long, or it contains too many components |
| 0x00c4 | (Subdoc) The document has too many levels to parse |
| 0x00c5 | (Subdoc) The value provided will invalidate the JSON if inserted |
| 0x00c6 | (Subdoc) The existing document is not valid JSON |
| 0x00c7 | (Subdoc) The existing number is out of the valid range for arithmetic ops |
| 0x00c8 | (Subdoc) The operation would result in a number outside the valid range |
| 0x00c9 | (Subdoc) The requested operation requires the path to not already exist, but it exists |
| 0x00ca | (Subdoc) Inserting the value would cause the document to be too deep |
| 0x00cb | (Subdoc) An invalid combination of commands was specified |
| 0x00cc | (Subdoc) Specified key was successfully found, but one or more path operations failed. Examine the individual lookup_result (MULTI_LOOKUP) / mutation_result (MULTI_MUTATION) structures for details. |
| 0x00cd | (Subdoc) Operation completed successfully on a deleted document |
| 0x00ce | (Subdoc) The flag combination doesn't make any sense |
| 0x00cf | (Subdoc) The key combination of the xattrs is not allowed |
| 0x00d0 | (Subdoc) The server don't know about the specified macro |
| 0x00d1 | (Subdoc) The server don't know about the specified virtual attribute |
| 0x00d2 | (Subdoc) Can't modify virtual attributes |
| 0x00d3 | (Subdoc) One or more paths in a multi-path command failed on a deleted document |
| 0x00d4 | (Subdoc) Invalid XATTR order (xattrs should come first) |

In addition to the above defined status codes, the range 0xff00 - 0xffff
is reserved for end-user applications (e.g. proxies). Couchbase itself will
not return them.

A well written client should provide logic to gracefully recover from
unknown error codes being returned. The status code falls into two main
categories: *Success* or *Failure*.

If the operation was successfully performed the status field is set to
`No error`. All other return values means that the operation was not
performed, and these error codes falls into two categories: *temporarily* and
*permanent*.

* A *temporary* failure is a problem that might go away by resending the
  operation. There is currently two status codes that may be returned for that:

  * `Busy` - The server is too busy to handle your request, please back off
  * `Temporary failure` - The server hit a problem that a retry might fix

* A *permanent* failure is a problem is a failure where the server expects the
  same result if the command is resent (unless an external actor act on the
  system (ex: define a user/bucket, change ownership of vbuckets etc). Some
  permanent failures may be resolved by simply running other commands (ex:
  `No bucket` may be resolved by selecting a bucket, `Authentication stale`
  may be resolved by re-authenticating to the server). Other permanent
  failures may require the connection to be re-established.

### Command Opcodes

Possible values of the one-byte field. See [Commands](#Commands) for more
information about a given command.

#### Opcodes for Magic 0x80 and 0x81 (client initiated messages)

| Raw  | Description                                             |
| -----|---------------------------------------------------------|
| 0x00 | [Get](#0x00-get)                                        |
| 0x01 | [Set](#0x01-set)                                        |
| 0x02 | [Add](#0x02-add)                                        |
| 0x03 | [Replace](#0x03-replace)                                |
| 0x04 | [Delete](#0x04-delete)                                  |
| 0x05 | [Increment](#0x05-increment)                            |
| 0x06 | [Decrement](#0x06-decrement)                            |
| 0x07 | [Quit](#0x07-quit)                                      |
| 0x08 | [Flush](#0x08-flush)                                    |
| 0x09 | [GetQ](#0x09-getq-get-quietly)                          |
| 0x0a | [No-op](#0x0a-no-op)                                    |
| 0x0b | [Version](#0x0b-version)                                |
| 0x0c | [GetK](#0x0c-getk-get-with-key)                         |
| 0x0d | [GetKQ](#0x0d-getkq-get-with-key-quietly)               |
| 0x0e | [Append](#0x0e-append)                                  |
| 0x0f | [Prepend](#0x0f-prepend)                                |
| 0x10 | [Stat](#0x10-stat)                                      |
| 0x11 | [SetQ](#0x11-setq-set-quietly)                          |
| 0x12 | [AddQ](#0x12-addq-add-quietly)                          |
| 0x13 | [ReplaceQ](#0x13-replaceq-replace-quietly)              |
| 0x14 | [DeleteQ](#0x14-deleteq-delete-quietly)                 |
| 0x15 | [IncrementQ](#0x15-incrementq-increment-quietly)        |
| 0x16 | [DecrementQ](#0x16-decrementq-decrement-quietly)        |
| 0x17 | [QuitQ](#0x17-quitq-quit-quietly)                       |
| 0x18 | [FlushQ](#0x18-flushq-flush-quietly)                    |
| 0x19 | [AppendQ](#0x19-appendq-append-quietly)                 |
| 0x1a | [PrependQ](#0x1a-prependq-prepend-quietly)              |
| 0x1b | [Verbosity](#0x1b-verbosity)                            |
| 0x1c | [Touch](#0x1c-touch)                                    |
| 0x1d | [GAT](#0x1d-gat-get-and-touch)                          |
| 0x1e | [GATQ](#0x1e-gatq-get-and-touch-quietly)                |
| 0x1f | [HELO](#0x1f-helo) |
| 0x20 | [SASL list mechs](sasl.md#0x20-list-mech)               |
| 0x21 | [SASL Auth](sasl.md#0x21-sasl-auth)                     |
| 0x22 | [SASL Step](sasl.md0x22-sasl-step)                      |
| 0x23 | Ioctl get |
| 0x24 | Ioctl set |
| 0x25 | Config validate |
| 0x26 | Config reload |
| 0x27 | Audit put |
| 0x28 | Audit config reload |
| 0x29 | Shutdown |
| 0x30 | RGet                                                    |
| 0x31 | RSet                                                    |
| 0x32 | RSetQ                                                   |
| 0x33 | RAppend                                                 |
| 0x34 | RAppendQ                                                |
| 0x35 | RPrepend                                                |
| 0x36 | RPrependQ                                               |
| 0x37 | RDelete                                                 |
| 0x38 | RDeleteQ                                                |
| 0x39 | RIncr                                                   |
| 0x3a | RIncrQ                                                  |
| 0x3b | RDecr                                                   |
| 0x3c | RDecrQ                                                  |
| 0x3d | [Set VBucket](#0x3d-set-vbucket)                        |
| 0x3e | [Get VBucket](#0x3e-get-vbucket)                        |
| 0x3f | [Del VBucket](#0x3f-del-vbucket)                        |
| 0x40 | TAP Connect - TAP removed in 5.0                       |
| 0x41 | TAP Mutation - TAP removed in 5.0                      |
| 0x42 | TAP Delete - TAP removed in 5.0                        |
| 0x43 | TAP Flush - TAP removed in 5.0                         |
| 0x44 | TAP Opaque - TAP removed in 5.0                        |
| 0x45 | TAP VBucket Set) - TAP removed in 5.0                  |
| 0x46 | TAP Checkout Start - TAP removed in 5.0                |
| 0x47 | TAP Checkpoint End - TAP removed in 5.0                |
| 0x48 | Get all vb seqnos |
| 0x50 | [Dcp Open](dcp/commands/open-connection.md) |
| 0x51 | [Dcp add stream](dcp/commands/add-stream.md) |
| 0x52 | [Dcp close stream](dcp/commands/close-stream.md) |
| 0x53 | [Dcp stream req](dcp/commands/stream-request.md) |
| 0x54 | [Dcp get failover log](dcp/commands/failover-log.md) |
| 0x55 | [Dcp stream end](dcp/commands/stream-end.md) |
| 0x56 | [Dcp snapshot marker](dcp/commands/snapshot-marker.md) |
| 0x57 | [Dcp mutation](dcp/commands/mutation.md) |
| 0x58 | [Dcp deletion](dcp/commands/deletion.md) |
| 0x59 | [Dcp expiration](dcp/commands/expiration.md) |
| 0x5a | [UNUSED (was Dcp flush)](dcp/commands/flush.md) |
| 0x5b | [Dcp set vbucket state](dcp/commands/set-vbucket-state.md) |
| 0x5c | [Dcp noop](dcp/commands/no-op.md) |
| 0x5d | [Dcp buffer acknowledgement](dcp/commands/buffer-ack.md) |
| 0x5e | [Dcp control](dcp/commands/control.md) |
| 0x5f | [Dcp system event](dcp/commands/system_event.md) |
| 0x80 | Stop persistence |
| 0x81 | Start persistence |
| 0x82 | Set param |
| 0x83 | Get replica |
| 0x85 | Create bucket |
| 0x86 | Delete bucket |
| 0x87 | [List buckets](#0x87-list-buckets) |
| 0x89 | [Select bucket](#0x89-select-bucket) |
| 0x8a | Assume role |
| 0x91 | Observe seqno |
| 0x92 | [Observe](#92-observe) |
| 0x93 | Evict key |
| 0x94 | Get locked |
| 0x95 | Unlock key |
| 0x96 | Get Failover Log |
| 0x97 | Last closed checkpoint |
| 0x9e | Deregister tap client - TAP removed in 5.0 |
| 0x9f | Reset replication chain |
| 0xa0 | Get meta |
| 0xa1 | Getq meta |
| 0xa2 | Set with meta |
| 0xa3 | Setq with meta |
| 0xa4 | Add with meta |
| 0xa5 | Addq with meta |
| 0xa6 | Snapshot vb states |
| 0xa7 | Vbucket batch count |
| 0xa8 | Del with meta |
| 0xa9 | Delq with meta |
| 0xaa | Create checkpoint |
| 0xac | Notify vbucket update |
| 0xad | Enable traffic |
| 0xae | Disable traffic |
| 0xb0 | Change vb filter |
| 0xb1 | Checkpoint persistence |
| 0xb2 | Return meta |
| 0xb3 | Compact db |
| 0xb4 | Set cluster config |
| 0xb5 | Get cluster config |
| 0xb6 | Get random key |
| 0xb7 | Seqno persistence |
| 0xb8 | Get keys |
| 0xb9 | Collections: set manifest |
| 0xba | Collections: get manifest |
| 0xbb | Collections: get collection id |
| 0xc1 | Set drift counter state |
| 0xc2 | Get adjusted time |
| 0xc5 | Subdoc get |
| 0xc6 | Subdoc exists |
| 0xc7 | Subdoc dict add |
| 0xc8 | Subdoc dict upsert |
| 0xc9 | Subdoc delete |
| 0xca | Subdoc replace |
| 0xcb | Subdoc array push last |
| 0xcc | Subdoc array push first |
| 0xcd | Subdoc array insert |
| 0xce | Subdoc array add unique |
| 0xcf | Subdoc counter |
| 0xd0 | Subdoc multi lookup |
| 0xd1 | Subdoc multi mutation |
| 0xd2 | Subdoc get count |
| 0xf0 | Scrub |
| 0xf1 | Isasl refresh |
| 0xf2 | Ssl certs refresh |
| 0xf3 | Get cmd timer |
| 0xf4 | [Set ctrl token](#0xf4-set-ctrl-token)  |
| 0xf5 | [Get ctrl token](#0xf5-get-ctrl-token) |
| 0xf6 | [Update External User Permissions](ExternalAuthProvider.md#updateexternaluserpermission) |
| 0xf7 | RBAC refresh |
| 0xf8 | AUTH provider |
| 0xf9 | Get Active External Users |
| 0xfe | [Get error map](#0xf5-get-error-map) |

As a convention all of the commands ending with "Q" for Quiet. A quiet version
of a command will omit responses that are considered uninteresting. Whether a
given response is interesting is dependent upon the command. See the
descriptions of the set commands (Section 4.2) and set commands (Section 4.3)
for examples of commands that include quiet variants.

#### Opcodes for Magic 0x82 and 0x83 (server initiated messages)

| Raw  | Description                                             |
| -----|---------------------------------------------------------|
| 0x01 | [ClustermapChangeNotification](#0x01-clustermap-change-notification) |
| 0x02 | [Authenticate](#0x02-authenticate) |
| 0x03 | [ActiveExternalUsers](#0x03-active-external-users) |

### Data Types

Possible values of the one-byte field which is a bit-filed.

| Bit  | Description |
| -----|-------------|
| 0x01 | JSON |
| 0x02 | Snappy compressed |
| 0x04 | Extended attributes (XATTR) |

If no bits is set the datatype is considered to be RAW. In order to utilize
the datatype bits the client needs to notify the server that it supports
datatype bits by performing a successful HELLO with the DATATYPE feature.

## Client Commands

### Introduction
The communication is initially initiated by a request being sent from the
client to the server, and the server will respond to the request with
zero or multiple packets for each request. The client is always the
initiator of communication unless it enables the [`duplex`](#0x1f-helo)
feature. When (explicitly) enabled by the client, the server may also
send requests to the client and expect a response being returned.

The command opcode defines the layout of the body of the message.

The current protocol dictates that the server won't start processing
the next command until the current command is completely processed
(due to the lack of barriers or any other primitives to enforce
execution order). The protocol defines some "quiet commands" which
won't send responses in certain cases (success for mutations,
not found for gets etc). The client would know that such commands
was executed when it encounters the _response_ for the next _command_
requested issued by the client.


#### Example
The following figure illustrates the packet layout for a packet with an error
message.


      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x09          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x4e ('N')    | 0x6f ('o')    | 0x74 ('t')    | 0x20 (' ')    |
        +---------------+---------------+---------------+---------------+
      28| 0x66 ('f')    | 0x6f ('o')    | 0x75 ('u')    | 0x6e ('n')    |
        +---------------+---------------+---------------+---------------+
      32| 0x64 ('d')    |
        +---------------+
        Total 33 bytes (24 byte header, and 9 bytes value)


    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x00
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0001
    Total body   (8-11) : 0x00000009
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              : None
    Key                 : None
    Value        (24-32): The textual string "Not found"

### 0x00 Get
### 0x09 GetQ: Get quietly
### 0x0c GetK: Get with key
### 0x0d GetKQ: Get with key quietly

Request:

* MUST NOT have extras.
* MUST have key.
* MUST NOT have value.

Response (if found):

* MUST have extras.
* MAY have key.
* MAY have value.

Extra data for the get commands:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Flags                                                         |
        +---------------+---------------+---------------+---------------+
        Total 4 bytes

The get command gets a single key. The getq command will not send a response
on a cache miss. Getk and getkq differs from get and getq by adding the key
into the response packet.

Clients should implement multi-get (still important for reducing network
roundtrips!) as n pipelined requests, the first n-1 being getq/getkq, the last
being a regular get/getk. That way you're guaranteed to get a response, and
you know when the server's done. You can also do the naive thing and send n
pipelined get/getks, but then you could potentially get back a lot of
"NOT_FOUND" error code packets. Alternatively, you can send 'n' getq/getkqs,
followed by a 'noop' command.

#### Example

To request the data associated with the key "Hello" the following fields must
be specified in the packet.

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x00          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x48 ('H')    | 0x65 ('e')    | 0x6c ('l')    | 0x6c ('l')    |
        +---------------+---------------+---------------+---------------+
      28| 0x6f ('o')    |
        +---------------+
       Total 29 bytes (24 byte header, and 5 bytes key)


    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x00
    Key length   (2,3)  : 0x0005
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    VBucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000005
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              : None
    Key          (24-29): The textual string: "Hello"
    Value               : None


If the item exist on the server the following packet is returned, otherwise a
packet with status code != 0 will be returned (See [Response Status](#response-status))

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x04          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x09          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
      24| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      28| 0x57 ('W')    | 0x6f ('o')    | 0x72 ('r')    | 0x6c ('l')    |
        +---------------+---------------+---------------+---------------+
      32| 0x64 ('d')    |
        +---------------+
       Total 33 bytes (24 byte header, 4 byte extras and 5 byte value)


    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x00
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000009
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000001
    Extras              :
      Flags      (24-27): 0xdeadbeef
    Key                 : None
    Value        (28-32): The textual string "World"


The response packet for a getk and getkq request differs from get(q) in that
the key is present:


     Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x00          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
       4| 0x04          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x09          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
      24| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      28| 0x48 ('H')    | 0x65 ('e')    | 0x6c ('l')    | 0x6c ('l')    |
        +---------------+---------------+---------------+---------------+
      32| 0x6f ('o')    | 0x57 ('W')    | 0x6f ('o')    | 0x72 ('r')    |
        +---------------+---------------+---------------+---------------+
      36| 0x6c ('l')    | 0x64 ('d')    |
        +---------------+---------------+
       Total 38 bytes (24 byte header, 4 byte extras, 5 byte key
                        and 5 byte value)


    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x00
    Key length   (2,3)  : 0x0005
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000009
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000001
    Extras              :
      Flags      (24-27): 0xdeadbeef
    Key          (28-32): The textual string: "Hello"
    Value        (33-37): The textual string: "World"

### 0x01 Set
### 0x11 SetQ: Set quietly
### 0x02 Add
### 0x12 AddQ: Add quietly
### 0x03 Replace
### 0x13 ReplaceQ: Replace quietly

Request:

* MUST have extras.
* MUST have key.
* MAY have value.

Extra data for set/add/replace:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Flags                                                         |
        +---------------+---------------+---------------+---------------+
       4| Expiration                                                    |
        +---------------+---------------+---------------+---------------+
        Total 8 bytes

Response:

* MUST have CAS
* MAY have extras
* MUST NOT have key
* MUST NOT have value

Constraints:

* If the Data Version Check (CAS) is nonzero, the requested operation MUST
only succeed if the item exists and has a CAS value identical to the provided
value.
* Add MUST fail if the item already exist.
* Replace MUST fail if the item doesn't exist.
* Set should store the data unconditionally if the item exists or not.

Quiet mutations only return responses on failure. Success is considered the
general case and is suppressed when in quiet mode, but errors should not be
allowed to go unnoticed.

If the client enabled the Mutation Seqno feature the extras should be
set to 16 bytes containing the vbucket UUID in the first 8 bytes (network
byte order) followed by 8 bytes sequence number (network byte order):

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
      24| Vbucket UUID                                                  |
      28|                                                               |
        +---------------+---------------+---------------+---------------+
      32| Seqno                                                         |
      36|                                                               |
        +---------------+---------------+---------------+---------------+
      Total 16 bytes

#### Example
The following figure shows an add-command for with key = "Hello", value =
"World", flags = 0xdeadbeef and expiry: in two hours.

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x02          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
       4| 0x08          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x12          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x0e          | 0x10          |
        +---------------+---------------+---------------+---------------+
      32| 0x48 ('H')    | 0x65 ('e')    | 0x6c ('l')    | 0x6c ('l')    |
        +---------------+---------------+---------------+---------------+
      36| 0x6f ('o')    | 0x57 ('W')    | 0x6f ('o')    | 0x72 ('r')    |
        +---------------+---------------+---------------+---------------+
      40| 0x6c ('l')    | 0x64 ('d')    |
        +---------------+---------------+
       Total 42 bytes (24 byte header, 8 byte extras, 5 byte key and
                        5 byte value)


    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x02
    Key length   (2,3)  : 0x0005
    Extra length (4)    : 0x08
    Data type    (5)    : 0x00
    VBucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000012
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              :
      Flags      (24-27): 0xdeadbeef
      Expiry     (28-31): 0x00000e10
    Key          (32-36): The textual string "Hello"
    Value        (37-41): The textual string "World"


The result of the operation is signaled through the status code. If the
command succeeds, the CAS value for the item is returned in the CAS-field of
the packet:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x02          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
       Total 24 bytes


    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x02
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000001
    Extras              : None
    Key                 : None
    Value               : None

### 0x04 Delete
### 0x14 DeleteQ: Delete Quietly

Request:

* MUST NOT have extras.
* MUST have key.
* MUST NOT have value.

Response:

* MAY have extras
* MUST NOT have key
* MUST NOT have value

Delete the item with the specific key. Quiet deletions only return responses
on failure. Success is considered the general case and is suppressed when in
quiet mode, but errors should not be allowed to go unnoticed.

If the client enabled the Mutation Seqno feature the extras should be
set to 16 bytes containing the vbucket UUID in the first 8 bytes (network
byte order) followed by 8 bytes sequence number (network byte order):

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
      24| Vbucket UUID                                                  |
      28|                                                               |
        +---------------+---------------+---------------+---------------+
      32| Seqno                                                         |
      36|                                                               |
        +---------------+---------------+---------------+---------------+
      Total 16 bytes

#### Example
The following figure shows a delete message for the item "Hello".

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x04          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x48 ('H')    | 0x65 ('e')    | 0x6c ('l')    | 0x6c ('l')    |
        +---------------+---------------+---------------+---------------+
      28| 0x6f ('o')    |
        +---------------+
       Total 29 bytes (24 byte header, 5 byte value)


   Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x04
    Key length   (2,3)  : 0x0005
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    VBucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000005
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              : None
    Key                 : The textual string "Hello"
    Value               : None


The result of the operation is signaled through the status code.

### 0x05 Increment
### 0x15 IncrementQ: Increment quietly
### 0x06 Decrement
### 0x16 DecrementQ: Decrement quietly

Request:

* MUST have extras.
* MUST have key.
* MUST NOT have value.

Extra data for incr/decr:

     Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Amount to add / subtract                                      |
        |                                                               |
        +---------------+---------------+---------------+---------------+
       8| Initial value                                                 |
        |                                                               |
        +---------------+---------------+---------------+---------------+
      16| Expiration                                                    |
        +---------------+---------------+---------------+---------------+
        Total 20 bytes

Response:

* MAY have extras.
* MUST NOT have key.
* MUST have value.

If the client enabled the Mutation Seqno feature the extras should be
set to 16 bytes containing the vbucket UUID in the first 8 bytes (network
byte order) followed by 8 bytes sequence number (network byte order):

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
      24| Vbucket UUID                                                  |
      28|                                                               |
        +---------------+---------------+---------------+---------------+
      32| Seqno                                                         |
      36|                                                               |
        +---------------+---------------+---------------+---------------+
      Total 16 bytes

The body contains the value:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 64-bit unsigned response.                                     |
        |                                                               |
        +---------------+---------------+---------------+---------------+
        Total 8 bytes

These commands will either add or remove the specified amount to the requested
counter. If you want to set the value of the counter with add/set/replace, the
objects data must be the ascii representation of the value and not the byte
values of a 64 bit integer.

If the counter does not exist, one of two things may happen:

If the expiration value is all one-bits (0xffffffff), the
operation will fail with NOT_FOUND.
2. For all other expiration values, the operation will succeed by
seeding the value for this key with the provided initial value to expire with
the provided expiration time. The flags will be set to zero.
Decrementing a counter will never result in a "negative value" (or cause the
counter to "wrap"). instead the counter is set to 0. Incrementing the counter
may cause the counter to wrap.

Quiet increment / decrement only return responses on failure. Success is
considered the general case and is suppressed when in quiet mode, but errors
should not be allowed to go unnoticed.


#### Example
The following figure shows an incr-command for key = "counter", delta = 0x01,
initial = 0x00 which expires in two hours.

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x05          | 0x00          | 0x07          |
        +---------------+---------------+---------------+---------------+
       4| 0x14          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x1b          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
      32| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      36| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      40| 0x00          | 0x00          | 0x0e          | 0x10          |
        +---------------+---------------+---------------+---------------+
      44| 0x63 ('c')    | 0x6f ('o')    | 0x75 ('u')    | 0x6e ('n')    |
        +---------------+---------------+---------------+---------------+
      48| 0x74 ('t')    | 0x65 ('e')    | 0x72 ('r')    |
        +---------------+---------------+---------------+
        Total 51 bytes (24 byte header, 20 byte extras, 7 byte key)


    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x05
    Key length   (2,3)  : 0x0007
    Extra length (4)    : 0x14
    Data type    (5)    : 0x00
    VBucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x0000001b
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              :
      Delta      (24-31): 0x0000000000000001
      Initial    (32-39): 0x0000000000000000
      Expiration (40-43): 0x00000e10
    Key                 : Textual string "counter"
    Value               : None


If the key doesn't exist, the server will respond with the initial value. If
not the incremented value will be returned. Let's assume that the key didn't
exist, so the initial value is returned:


     Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x05          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x08          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
        Total 32 bytes (24 byte header, 8 byte value)


    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x05
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000008
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000005
    Extras              : None
    Key                 : None
    Value               : 0x0000000000000000


### 0x07 Quit
### 0x17 QuitQ: Quit quietly

Request:

* MUST NOT have extras.
* MUST NOT have key.
* MUST NOT have value.

Response:

* MUST NOT have extras.
* MUST NOT have key.
* MUST NOT have value.

Close the connection to the server. Quiet quit should not receive a response
packet.

#### Example
      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x07          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
        Total 24 bytes


   Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x07
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    VBucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              : None
    Key                 : None
    Value               : None


The response-packet contains no extra data, and the result of the operation is
signaled through the status code. The server will then close the connection.

### 0x08 Flush
### 0x18 FlushQ: Flush quietly

Request:

* MAY have extras.
* MUST NOT have key.
* MUST NOT have value.

Extra data for flush:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Expiration                                                    |
        +---------------+---------------+---------------+---------------+
      Total 4 bytes

If extras is present it must be set to 0

Response:

* MUST NOT have extras.
* MUST NOT have key.
* MUST NOT have value.

Flush the items in the cache now or some time in the future as specified by
the expiration field. Quiet flush only return responses on failure. Success is
considered the general case and is suppressed when in quiet mode, but errors
should not be allowed to go unnoticed.

Note: Is it not recommended that you flush a bucket directly with Couchbase
Server (Especially with Couchbase Buckets). Instead you should instigate a
flush using the Cluster Manager's REST API.

#### Example
To flush the cache (delete all items) in two hours, the set the following
values in the request

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x08          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x04          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x04          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x0e          | 0x10          |
        +---------------+---------------+---------------+---------------+
        Total 28 bytes (24 byte header, 4 byte body)


    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x08
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    VBucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000004
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              :
      Expiry     (24-27): 0x000e10
    Key                 : None
    Value               : None

The result of the operation is signaled through the status code.

### 0x0a No-op

Request:

* MUST NOT have extras.
* MUST NOT have key.
* MUST NOT have value.

Response:

* MUST NOT have extras.
* MUST NOT have key.
* MUST NOT have value.

Used as a keep alive.

####Example

Noop request:


      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x0a          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
        Total 24 bytes


    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x0a
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    VBucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              : None
    Key                 : None
    Value               : None


The result of the operation is signaled through the status code.

### 0x0b Version

Request:

* MUST NOT have extras.
* MUST NOT have key.
* MUST NOT have value.

Response:

* MUST NOT have extras.
* MUST NOT have key.
* MUST have value.

Request the server version.

The server responds with a packet containing the version string in the body
with the following format: "x.y.z"

#### Example

Request:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x0b          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
        Total 24 bytes


    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x0b
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    VBucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              : None


Response:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x0b          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x31 ('1')    | 0x2e ('.')    | 0x33 ('3')    | 0x2e ('.')    |
        +---------------+---------------+---------------+---------------+
      28| 0x31 ('1')    |
        +---------------+
        Total 29 bytes (24 byte header, 5 byte body)

    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x0b
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000005
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              : None
    Key                 : None
    Value               : Textual string "1.3.1"

### 0x0e Append
### 0x19 AppendQ: Append quietly
### 0x0f Prepend
### 0x1a PrependQ: Prepend quietly

Request:

* MUST NOT have extras.
* MUST have key.
* MUST have value.

Response:

* MAY have extras.
* MUST NOT have key.
* MUST NOT have value.
* MUST have CAS

These commands will either append or prepend the specified value to the
requested key.

If the client enabled the Mutation Seqno feature the extras should be
set to 16 bytes containing the vbucket UUID in the first 8 bytes (network
byte order) followed by 8 bytes sequence number (network byte order):

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
      24| Vbucket UUID                                                  |
      28|                                                               |
        +---------------+---------------+---------------+---------------+
      32| Seqno                                                         |
      36|                                                               |
        +---------------+---------------+---------------+---------------+
      Total 16 bytes

#### Example

The following example appends '!' to the 'Hello' key.

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x0e          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x06          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x48 ('H')    | 0x65 ('e')    | 0x6c ('l')    | 0x6c ('l')    |
        +---------------+---------------+---------------+---------------+
      28| 0x6f ('o')    | 0x21 ('!')    |
        +---------------+---------------+
        Total 30 bytes (24 byte header, 5 byte key, 1 byte value)


    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x0e
    Key length   (2,3)  : 0x0005
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    VBucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000006
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              : None
    Key          (24-28): The textual string "Hello"
    Value        (29)   : "!"


The result of the operation is signaled through the status code.

### 0x10 Stat
Request:

* MUST NOT have extras.
* MAY have key.
* MUST NOT have value.

Response:

* MUST NOT have extras.
* MAY have key.
* MAY have value.

Request server statistics. Without a key specified the server will respond
with a "default" set of statistics information. Each piece of statistical
information is returned in its own packet (key contains the name of the
statistical item and the body contains the value in ASCII format). The
sequence of return packets is terminated with a packet that contains no key
and no value.

#### Example

The following example requests all statistics from the server

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x10          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
        Total 24 bytes


    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x10
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    VBucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              : None
    Key                 : None
    Value               : None


The server will send each value in a separate packet with an "empty" packet
no key / no value) to terminate the sequence. Each of the response packets
look like the following example:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x10          | 0x00          | 0x03          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x07          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x70 ('p')    | 0x69 ('i')    | 0x64 ('d')    | 0x33 ('3')    |
        +---------------+---------------+---------------+---------------+
      28| 0x30 ('0')    | 0x37 ('7')    | 0x38 ('8')    |
        +---------------+---------------+---------------+
        Total 31 bytes (24 byte header, 3 byte key, 4 byte body)


    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x10
    Key length   (2,3)  : 0x0003
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000007
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Exstras             : None
    Key                 : The textual string "pid"
    Value               : The textual string "3078"


### 0x1b Verbosity

Request:

* MUST have extras.
* MUST NOT have key.
* MUST NOT have value.

Extra data for flush:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Verbosity                                                     |
        +---------------+---------------+---------------+---------------+
      Total 4 bytes

Response:

* MUST NOT have extras.
* MUST NOT have key.
* MUST NOT have value.

Set the verbosity level of the server. This may cause your memcached server to
generate more or less output.

#### Example
To set the verbosity level to two, set the following values in the request

**TODO: add me**

The result of the operation is signaled through the status code.

### 0x1c Touch
### 0x1d GAT: Get and touch
### 0x1e GATQ: Get and touch quietly

Request:

* MUST have extras.
* MUST have key.
* MUST NOT have value.

Extra data for touch/gat:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Expiration                                                    |
        +---------------+---------------+---------------+---------------+
        Total 4 bytes

Touch is used to set a new expiration time for an existing item. GAT (Get and
touch) and GATQ will return the value for the object if it is present in the
cache.

Example
**TODO: add me**

### 0x1f HELO

The `HELO` command may be used to enable / disable features on the server
similar to the [EHLO command in SMTP](https://www.ietf.org/rfc/rfc2821.txt).
A well written client should identify itself by sending the `HELO` command
providing its name and version in the "User agent name".

Request:

* MUST NOT have extra
* SHOULD have key
* SHOULD have value

The key should contain the agent name. If the first character is '{' the
server will try to parse the key as JSON and look for the following two
attributes:

 * "a" - which is the name of the agent
 * "i" - 33 bytes connection identifier

The value should contain the various features to enable. Each feature is
a two byte value in network byte order.

The following features is defined:

| Value  | Feature |
|--------|---------|
| 0x0001 | Datatype |
| 0x0002 | TLS |
| 0x0003 | TCP Nodelay |
| 0x0004 | Mutation seqno |
| 0x0005 | TCP Delay |
| 0x0006 | XATTR |
| 0x0007 | XERROR |
| 0x0008 | Select bucket |
| 0x0009 | Collections |
| 0x000a | Snappy |
| 0x000b | JSON |
| 0x000c | Duplex |
| 0x000d | Clustermap change notification |
| 0x000e | Unordered Execution |
| 0x000f | Tracing |
| 0x0010 | AltRequest support |
| 0x0011 | SyncReplication support |

* `Datatype` - The client understands the 'non-null' values in the
  [datatype field](#data-types). The server expects the client to fill
   in the correct values in the datatype field, and accepts that the server
   returns the value encoded as specified in the datatype field.
   This value is no longer used by the server and replaced with with
   individual features for the various data types to use.
* `TLS` - The client wants to TLS and send STARTTLS
* `TCP Nodelay` - The client requests the server to set TCP NODELAY on the
  socket used by this connection.
* `Mutation seqno` - The client requests the server to add the sequence number
  for a mutation to the response packet used in mutations.
* `TCP Delay` - The client requests the server to set TCP DELAY on the socket
  used by this connection.
* `XATTR` - The client requests the server to add XATTRs to the stream for
            commands where it makes sense (GetWithMeta, SetWithMeta,
            DcpMutation etc)
* `XERROR` - The client requests the server to send extended error codes
             instead of disconnecting the client when new errors occur
             (note that some errors may be remapped to more generic
             error codes instead of disconnecting)
* `Select bucket` - This is purely informational (it does not enable/disable
                    anything on the server). It may be used from the client
                    to know if it should be able to run select bucket or not
                    (select bucket was a privileged command pre-spock. In
                    spock all users may run select bucket, but only to a
                    bucket they have access to).
* `Collections` - The client wants to enable support for Collections
* `Snappy` - The client wants to enable support for Snappy compression. A
             client with support for Snappy compression must update the
             datatype filed in the requests with the bit representing
             SNAPPY when sending snappy compressed data to the server.
             It _must_ be able to receive data from the server compressed
             with SNAPPY identified by the bit being set in the datatype
             field.
* `JSON` - The client wants to enable support for JSON. The client _must_
           set this bit when storing JSON documents on the server. The server
           will set the appropriate bit in the datatype field when returning
           such documents to the client.
* `Duplex` - The client allows for full duplex on the socket. This means
             that the server may send requests back to the client.
             These messages is identified by the magic values of
             0x82 (request) and 0x83 (response). See the document
             [Duplex](Duplex.md) for more information.
* `Clustermap change notification` - The client wants the server to
  notify the client with new cluster maps whenever ns_server push
  them to memcached. (note that this notification is subject to
  deduplication of the vbucket map received as part of not my vbucket)
* `UnorderedExecution` - The client allows the server to reorder the
  execution of commands. See the document
  [UnorderedExecution](UnorderedExecution.md) for more information
* `Tracing` - The client wants the server to include tracing information
  in the response packet
* `AltRequest support` This is purely informational (it does not enable/disable
                      anything on the server). It may be used from the client to
                      know if it may send the alternative request packet (magic
                      0x08) containing FrameInfo segments.
* `SyncReplication` This is purely informational (it does not enable/disable
                    anything on the server). It may be used from the client to
                    know if it may use synchronous replication tags in the
                    mutation requests.

Response:

* MUST NOT have extras.
* MUST NOT have key.
* MAY have value.

The server replies with all of the features the client requested that
the server agreed to enable.

#### Example

The following example requests all defined features from the server
specifying the user agent "mchello v1.0".


      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x1f          | 0x00          | 0x0c          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x16          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x6d ('m')    | 0x63 ('c')    | 0x68 ('h')    | 0x65 ('e')    |
        +---------------+---------------+---------------+---------------+
      28| 0x6c ('l')    | 0x6c ('l')    | 0x6f ('o')    | 0x20 (' ')    |
        +---------------+---------------+---------------+---------------+
      32| 0x76 ('v')    | 0x31 ('1')    | 0x2e ('.')    | 0x30 ('0')    |
        +---------------+---------------+---------------+---------------+
      36| 0x00          | 0x01          | 0x00          | 0x02          |
        +---------------+---------------+---------------+---------------+
      40| 0x00          | 0x03          | 0x00          | 0x04          |
        +---------------+---------------+---------------+---------------+
      44| 0x00          | 0x05          |
        +---------------+---------------+
        Total 46 bytes (24 bytes header, 12 bytes key and 10 value)

    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x1f
    Key length   (2,3)  : 0x000c
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000016
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Key          (24-35): The textual string "mchello v1.0"
    Body                :
                 (36-37): Datatype
                 (38-39): TLS
                 (40-41): TCP NODELAY
                 (42-43): Mutation seqno
                 (44-45): TCP DELAY


The following example shows that the server agreed to enable 0x0003 and
0x0004.


       Byte/     0       |       1       |       2       |       3       |
          /              |               |               |               |
         |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
         +---------------+---------------+---------------+---------------+
        0| 0x81          | 0x1f          | 0x00          | 0x00          |
         +---------------+---------------+---------------+---------------+
        4| 0x00          | 0x00          | 0x00          | 0x00          |
         +---------------+---------------+---------------+---------------+
        8| 0x00          | 0x00          | 0x00          | 0x04          |
         +---------------+---------------+---------------+---------------+
       12| 0x00          | 0x00          | 0x00          | 0x00          |
         +---------------+---------------+---------------+---------------+
       16| 0x00          | 0x00          | 0x00          | 0x00          |
         +---------------+---------------+---------------+---------------+
       20| 0x00          | 0x00          | 0x00          | 0x00          |
         +---------------+---------------+---------------+---------------+
       24| 0x00          | 0x03          | 0x00          | 0x04          |
         +---------------+---------------+---------------+---------------+
         Total 28 bytes (24 bytes header and 4 value)

     Field        (offset) (value)
     Magic        (0)    : 0x81
     Opcode       (1)    : 0x1f
     Key length   (2,3)  : 0x0000
     Extra length (4)    : 0x00
     Data type    (5)    : 0x00
     Status       (6,7)  : 0x0000
     Total body   (8-11) : 0x00000004
     Opaque       (12-15): 0x00000000
     CAS          (16-23): 0x0000000000000000
     Body                :
                  (24-25): TCP NODELAY
                  (26-27): Mutation seqno


### 0x3d Set VBucket

The `set vbucket` command is used to set the state of vbucket.

Request:

* MUST have extra
* MUST NOT have key
* MUST NOT have value

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| vbucket state in network byte order                           |
        +---------------+---------------+---------------+---------------+
        Total 4 bytes

State may be one of:

    1 - Active
    2 - Replica
    3 - Pending
    4 - Dead

Response:

* MUST NOT have extras
* MUST NOT have key
* MUST NOT have value

### 0x3e Get VBucket

The `get vbucket` command is used to get the current state of a vbucket

Reuest:

* MUST NOT have extras
* MUST NOT have key
* MUST NOT have value

Response:

* MUST NOT have extras
* MUST NOT have key
* MUST have value

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| vbucket state in network byte order                           |
        +---------------+---------------+---------------+---------------+
        Total 4 bytes

State may be one of:

    1 - Active
    2 - Replica
    3 - Pending
    4 - Dead

### 0x3f Del VBucket

The `del vbucket` command is used to delete a vbucket

Reuest:

* MUST NOT have extras
* MUST NOT have key
* MUST NOT have value

Response:

* MUST NOT have extras
* MUST NOT have key
* MUST NOT have value

### 0x87 List Buckets

The `list buckets` command is used to list all of the buckets available
for the connection in it's current state (performing authentication may
invalidate the list).

Request:

* MUST NOT have extra
* MUST NOT have key
* MUST NOT have value

Response:

* MUST NOT have extras.
* MUST NOT have key.
* MAY have value.

In the response packet all of the available buckets is listed and
separated by a single space character.

#### Example

The following example shows a successful list bucket operation.

Request:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x87          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0xef          | 0xbe          | 0xad          | 0xde          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
        Total 24 bytes

    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x87 (LIST_BUCKETS)
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0xefbeadde
    CAS          (16-23): 0x0000000000000000

Response:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x87          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x1b          |
        +---------------+---------------+---------------+---------------+
      12| 0xef          | 0xbe          | 0xad          | 0xde          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x65 ('e')    | 0x6e ('n')    | 0x67 ('g')    | 0x69 ('i')    |
        +---------------+---------------+---------------+---------------+
      28| 0x6e ('n')    | 0x65 ('e')    | 0x65 ('e')    | 0x72 ('r')    |
        +---------------+---------------+---------------+---------------+
      32| 0x69 ('i')    | 0x6e ('n')    | 0x67 ('g')    | 0x20 (' ')    |
        +---------------+---------------+---------------+---------------+
      36| 0x6d ('m')    | 0x61 ('a')    | 0x72 ('r')    | 0x6b ('k')    |
        +---------------+---------------+---------------+---------------+
      40| 0x65 ('e')    | 0x74 ('t')    | 0x69 ('i')    | 0x6e ('n')    |
        +---------------+---------------+---------------+---------------+
      44| 0x67 ('g')    | 0x20 (' ')    | 0x73 ('s')    | 0x61 ('a')    |
        +---------------+---------------+---------------+---------------+
      48| 0x6c ('l')    | 0x65 ('e')    | 0x73 ('s')    |
        +---------------+---------------+---------------+
        Total 51 bytes (24 bytes header and 27 value)

    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x87 (LIST_BUCKETS)
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000 (Success)
    Total body   (8-11) : 0x0000001b
    Opaque       (12-15): 0xdeadbeef
    CAS          (16-23): 0x0000000000000000
    Body                :
                        : engineering
                        : marketing
                        : sales



### 0x89 Select Bucket

The `select bucket` command is used to bind the connection to a given bucket
on the server.

Request:

* MUST NOT have extra
* MUST have key
* MUST NOT have value

Response:

* MUST NOT have extras.
* MUST NOT have key.
* MAY have value.

#### Example

The following example tries to select the bucket named engineering

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x89          | 0x00          | 0x0b          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x0b          |
        +---------------+---------------+---------------+---------------+
      12| 0xef          | 0xbe          | 0xad          | 0xde          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x65 ('e')    | 0x6e ('n')    | 0x67 ('g')    | 0x69 ('i')    |
        +---------------+---------------+---------------+---------------+
      28| 0x6e ('n')    | 0x65 ('e')    | 0x65 ('e')    | 0x72 ('r')    |
        +---------------+---------------+---------------+---------------+
      32| 0x69 ('i')    | 0x6e ('n')    | 0x67 ('g')    |
        +---------------+---------------+---------------+
        Total 35 bytes (24 bytes header, 11 bytes key)

    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x89 (SELECT_BUCKET)
    Key length   (2,3)  : 0x000b
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x0000000b
    Opaque       (12-15): 0xefbeadde
    CAS          (16-23): 0x0000000000000000
    Key          (24-34): The textual string "engineering"


### 0x90 Observe

The `observe` command is used to observe the status for keys

Request:

* MUST NOT have extra
* MUST NOT have key
* MUST have value

Response:

* MUST NOT have extras.
* MUST NOT have key.
* MAY have value.

The value in the request is encoded (in network byte order) with
two bytes representing the vbucket id followed by two bytes
representing the key length followed by the key (One may send
multiple entries in a single command).

### 0x96 Get Failover Log

The Failover log request is used by a MCBP client to request all known failover
ids for a given VBucket. A failover id consists of the vbucket UUID and a
sequence number.
This is the MCBP variant of the DCP Get Failover Log command (opcode 0x54).

The request:
* Must not have extras
* Must not have key
* Must not have value

The response:
* Must not have extras
* Must not have key
* Must have value on Success

The following example requests the failover log for vbucket 0:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x96          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
    GET_FAILOVER_LOG command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x96
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0xdeadbeef
    CAS          (16-23): 0x0000000000000000

If the command executes successful (see the status field), the following packet
is returned from a server which have 4 different failover ids available:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x96          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x40          |
        +---------------+---------------+---------------+---------------+
      12| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0xfe          | 0xed          | 0xde          | 0xca          |
        +---------------+---------------+---------------+---------------+
      32| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      36| 0x00          | 0x00          | 0x54          | 0x32          |
        +---------------+---------------+---------------+---------------+
      40| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      44| 0x00          | 0xde          | 0xca          | 0xfe          |
        +---------------+---------------+---------------+---------------+
      48| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      52| 0x01          | 0x34          | 0x32          | 0x14          |
        +---------------+---------------+---------------+---------------+
      56| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      60| 0xfe          | 0xed          | 0xfa          | 0xce          |
        +---------------+---------------+---------------+---------------+
      64| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      68| 0x00          | 0x00          | 0x00          | 0x04          |
        +---------------+---------------+---------------+---------------+
      72| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      76| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      80| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      84| 0x00          | 0x00          | 0x65          | 0x24          |
        +---------------+---------------+---------------+---------------+
    GET_FAILOVER_LOG response
    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x96
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000040
    Opaque       (12-15): 0xdeadbeef
    CAS          (16-23): 0x0000000000000000
      vb UUID    (24-31): 0x00000000feeddeca
      vb seqno   (32-39): 0x0000000000005432
      vb UUID    (40-47): 0x0000000000decafe
      vb seqno   (48-55): 0x0000000001343214
      vb UUID    (56-63): 0x00000000feedface
      vb seqno   (64-71): 0x0000000000000004
      vb UUID    (72-79): 0x00000000deadbeef
      vb seqno   (80-87): 0x0000000000006524

#### Returns

A failover log for the vbucket requested. The failover log will be in
descending order of time meaning the oldest failover entry will be the last
entry in the response and the newest entry will be the first entry in the
response. On failure an error code is returned.

#### Errors

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET (0x07)**

If the VBucket the failover log is requested for does not exist.

**PROTOCOL_BINARY_RESPONSE_ENOMEM (0x82)**

If the failover log could not be sent to due a failure to allocate memory.

### 0xf4 Set Ctrl Token

The `set ctrl token` will be used by ns_server and ns_server alone
to set the session cas token in memcached which will be used to
recognize the particular instance on ns_server.
The previous token will be passed in the cas section of the request
header for the CAS operation, and the new token will be part of extras.

Request:

* MUST have extra
* MUST NOT have key
* MUST NOT have value

The response to this request will include the cas as it were set,
and a SUCCESS as status, or a KEY_EEXISTS with the existing token in
memcached if the CAS operation were to fail.

### 0xf5 Get Ctrl Token

The `get ctrl token` command will be used by ns_server to fetch the current
session cas token held in memcached.

Request:

* MUST NOT have extra
* MUST NOT have key
* MUST NOT have value

Response:

* MUST NOT have extra
* MUST NOT have key
* MUST NOT have value

The response to this request will include the token currently held in
memcached in the cas field of the header.

## Server Commands

The following chapter describes the packet layout for the various server
commands.

### 0x01 Clustermap Change Notification

The server will push the new cluster map to the clients iff the client
subscibes to clustermap notifications (see [HELO](#0x1f-helo))

The request:
* Must have extras
* Must not have key
* Must have value

The revision number of the clustermap is stored with 4 bytes in the extras
(network byte order), and the full clustermap is sent in the value field.

The server does not need a reply to the message (it is silently dropped without
any kind of validation).

### 0x02 Authenticate

See [External Auth Provider](ExternalAuthProvider.md#authentication-request).

### 0x03 Active External Users

See [External Auth Provider](ExternalAuthProvider.md#activeexternalusers-request).

### 0xfe Get error map

The `get error map` is used from clients to retrieve the server defined logic
on how to deal with "unknown" errors. If a client connects to a newer server
version than it was tested with, that server may return error codes the client
don't know about. The returned error map contains information on how the
client should behave when it receives one of those error messages.

Request:

* MUST NOT have extra
* MUST NOT have key
* MUST have value

Response:

* MUST NOT have extra
* MUST NOT have key
* MUST have value

The value in the request contains the version of the error map the client
requests (16 bits encoded in network byte order). This version number should
indicate the highest version number of the error map the client is able
to understand. The server will return a JSON-formatted error map
which is formatted to either the version requested by the client, or
a lower version (thus, clients must be ready to parse lower version
formats).

See [ErrorMap.mp](ErrorMap.md) for more information.
