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

##### ID:0 - barrier

No commands may be executed in parallel (received on the same connection) as
this command (the command before MUST be completed before execution of
this command is started, and this command MUST be completed before
execution of the next command is started). FrameInfo encoded as:

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

The (optional) 2nd and 3rd byte contains the timeout specified in milliseconds
(network byte order). If the timeout is omitted the default timeout value
configured on the server will be used.

If _timeout_ is specified, the valid range is 1..65535. Values `0x0` and
`0xffff` are reserved and will result in the request failing with
`Status::Einval` (0x4) if used.

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

##### ID:3 - Available (was OpenTracing which was only a prototype)

ID 3 was used for the OpenTracing prototype which never got released
as a full feature

##### ID:4 - Impersonate user

Request the server to execute the command as the provided user username (must
be present) to identify users defined outside Couchbase (ldap) the username
must be prefixed with `^` (ex: `^trond`). Local users do not need a prefix.

The authenticated user must possess the `impersonate` privilege in order
to utilize the feature (otherwise an error will be returned), and the
effective privilege set when executing the command is an intersection of
the authenticated users privilege set and the impersonated persons privilege
set.

##### ID:5 - Preserve TTL

If the request modifies an existing document the expiry time from the
existing document should be used instead of the TTL provided. If document
don't exist the provided TTL should be used. The frame info contains no
value (length = 0).

##### ID:6 - Impersonate users extra privilege

Extra privilege to inject into the impersonated users privilege set (
the authenticated process must hold the privilege in its effective set
to avoid privilege escalation)

Multiple privileges may be added by adding multiple frame info entries

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

##### ID:1 - Read Units Used

The amount of read units used by the command (only added if the value
is non-zero)

Size: 2 bytes

FrameInfo encoded as:

    Byte/     0       |       1       |       2       |
       /              |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+
     0|  ID:1 | Len:2 | Number of read units          |

##### ID:2 - Write Units Used

The amount of write units used by the command (only added if the value is
non-zero)

Size: 2 bytes

FrameInfo encoded as:

    Byte/     0       |       1       |       2       |
       /              |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+
     0|  ID:2 | Len:2 | Number of write units         |

##### ID:3 - Throttle duration

Time (in microseconds) the command was throttled on the server.
Size: 2 bytes; encoded as variable-precision value (see below)

FrameInfo encoded as:

    Byte/     0       |       1       |       2       |
       /              |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+
     0|  ID:3 | Len:2 |       Throttle Duration       |

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

Possible values of this two-byte are defined in the following source code file
[status.h](../include/mcbp/protocol/status.h).

In addition to the defined status codes, the range 0xff00 - 0xffff
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
| 0x22 | [SASL Step](sasl.md#0x22-sasl-step)                      |
| 0x23 | Ioctl get |
| 0x24 | Ioctl set |
| 0x25 | Config validate |
| 0x26 | Config reload |
| 0x27 | Audit put |
| 0x28 | Audit config reload |
| 0x29 | Shutdown |
| 0x2a | [SetBucketThrottleProperties](#0x2a-set-bucket-throttle-properties) |
| 0x2b | [SetBucketDataLimitExceeded](#0x2b-set-bucket-data-limit-exceeded) |
| 0x2c | [SetNodeThrottleProperties](#0x2c-set-node-throttle-properties) |
| 0x30 | RGet (not supported) |
| 0x31 | RSet (not supported) |
| 0x32 | RSetQ (not supported) |
| 0x33 | RAppend (not supported) |
| 0x34 | RAppendQ (not supported) |
| 0x35 | RPrepend (not supported) |
| 0x36 | RPrependQ (not supported) |
| 0x37 | RDelete (not supported) |
| 0x38 | RDeleteQ (not supported) |
| 0x39 | RIncr (not supported) |
| 0x3a | RIncrQ (not supported) |
| 0x3b | RDecr (not supported) |
| 0x3c | RDecrQ (not supported) |
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
| 0x49 | [GetEx](#0x49-getex) |
| 0x4a | [GetExReplica](#0x50-getexreplica) |
| 0x50 | [Dcp Open](dcp/documentation/commands/open-connection.md) |
| 0x51 | [Dcp add stream](dcp/documentation/commands/add-stream.md) |
| 0x52 | [Dcp close stream](dcp/documentation/commands/close-stream.md) |
| 0x53 | [Dcp stream req](dcp/documentation/commands/stream-request.md) |
| 0x54 | [Dcp get failover log](dcp/documentation/commands/failover-log.md) |
| 0x55 | [Dcp stream end](dcp/documentation/commands/stream-end.md) |
| 0x56 | [Dcp snapshot marker](dcp/documentation/commands/snapshot-marker.md) |
| 0x57 | [Dcp mutation](dcp/documentation/commands/mutation.md) |
| 0x58 | [Dcp deletion](dcp/documentation/commands/deletion.md) |
| 0x59 | [Dcp expiration](dcp/documentation/commands/expiration.md) |
| 0x5a | [Dcp flush](dcp/documentation/commands/flush.md) (obsolete) |
| 0x5b | [Dcp set vbucket state](dcp/documentation/commands/set-vbucket-state.md) |
| 0x5c | [Dcp noop](dcp/documentation/commands/no-op.md) |
| 0x5d | [Dcp buffer acknowledgement](dcp/documentation/commands/buffer-ack.md) |
| 0x5e | [Dcp control](dcp/documentation/commands/control.md) |
| 0x5f | [Dcp system event](dcp/documentation/commands/system_event.md) |
| 0x60 | [Dcp Prepare](dcp/documentation/commands/prepare.md) |
| 0x61 | DcpSeqnoAcknowledged |
| 0x62 | DcpCommit |
| 0x63 | DcpAbort |
| 0x64 | [DcpSeqnoAdvanced](dcp/documentation/commands/seqno-advanced.md) |
| 0x65 | [Dcp Out of Sequence Order snapshot](dcp/documentation/commands/oso_snapshot.md) |
| 0x70 | [Get Fusion Storage Snapshot](./fusion.md#0x70---Get-Fusion-Storage-Snapshot) |
| 0x71 | [Release Fusion Storage Snapshot](./fusion.md#0x71---Release-Fusion-Storage-Snapshot) |
| 0x72 | [Mount Fusion Vbucket](./fusion.md#0x72---Mount-Fusion-Vbucket) |
| 0x73 | [Unmount Fusion Vbucket](./fusion.md#0x73---Unmount-Fusion-Vbucket) |
| 0x74 | [Sync Fusion Logstore](./fusion.md#0x74---Sync-Fusion-Logstore) |
| 0x75 | [Start Fusion Uploader](./fusion.md#0x75---Start-Fusion-Uploader) |
| 0x76 | [Stop Fusion Uploader](./fusion.md#0x76---Stop-Fusion-Uploader) |
| 0x80 | Stop persistence |
| 0x81 | Start persistence |
| 0x82 | Set param |
| 0x83 | Get replica |
| 0x85 | [Create bucket](#0x85-create-bucket) |
| 0x86 | [Delete bucket](#0x86-delete-bucket) |
| 0x87 | [List buckets](#0x87-list-buckets) |
| 0x89 | [Select bucket](#0x89-select-bucket) |
| 0x8a | [PauseBucket](#0x8a-pause-bucket) |
| 0x8b | [ResumeBucket](#0x8b-resume-bucket) |
| 0x91 | Observe seqno |
| 0x92 | [Observe](#0x92-observe) |
| 0x93 | Evict key |
| 0x94 | [Get locked](#0x94-get-locked) |
| 0x95 | [Unlock key](#0x95-unlock-key) |
| 0x96 | Get Failover Log |
| 0x97 | Last closed checkpoint |
| 0x9e | Deregister tap client - TAP removed in 5.0 |
| 0x9f | Reset replication chain (obsolete) |
| 0xa0 | Get meta |
| 0xa1 | Getq meta |
| 0xa2 | [Set with meta](../engines/ep/docs/protocol/set_with_meta.md) |
| 0xa3 | [Setq with meta](../engines/ep/docs/protocol/set_with_meta.md) |
| 0xa4 | [Add with meta](../engines/ep/docs/protocol/set_with_meta.md)|
| 0xa5 | [Addq with meta](../engines/ep/docs/protocol/set_with_meta.md) |
| 0xa6 | Snapshot vb states (obsolete) |
| 0xa7 | Vbucket batch count (obsolete) |
| 0xa8 | [Del with meta](../engines/ep/docs/protocol/del_with_meta.md) |
| 0xa9 | [Delq with meta](../engines/ep/docs/protocol/del_with_meta.md) |
| 0xaa | Create checkpoint |
| 0xac | Notify vbucket update (obsolete) |
| 0xad | Enable traffic |
| 0xae | Disable traffic |
| 0xb0 | Change vb filter (obsolete) |
| 0xb1 | Checkpoint persistence |
| 0xb2 | Return meta |
| 0xb3 | Compact db |
| 0xb4 | Set cluster config |
| 0xb5 | [Get cluster config](#0xb5-get-cluster-config) |
| 0xb6 | [Get random key](#0xb6-get-random-key)|
| 0xb7 | Seqno persistence |
| 0xb8 | [Get keys](../engines/ep/docs/protocol/get_keys.md) |
| 0xb9 | [Collections: set manifest](Collections.md#0xb9---Set-Collections-Manifest) |
| 0xba | [Collections: get manifest](Collections.md#0xba---Get-Collections-Manifest) |
| 0xbb | [Collections: get collection id](Collections.md#0xbb---Get-Collections-ID) |
| 0xbc | [Collections: get scope id](Collections.md#0xbc---Get-Scope-ID) |
| 0xc1 | Set drift counter state (obsolete) |
| 0xc2 | Get adjusted time (obsolete) |
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
| 0xd3 | Subdoc replace body with xattr (see https://docs.google.com/document/d/1vaQJxIA5nhWJqji7X2R1xQDZadb5PabfKAid1kVe65o ) |
| 0xda | [Create RangeScan](range_scans/range_scan_create.md) |
| 0xdb | [Continue RangeScan](range_scans/range_scan_continue.md) |
| 0xdc | [Cancel RangeScan](range_scans/range_scan_cancel.md) |
| 0xe0 | [PrepareSnapshot](Snapshots.md#preparesnapshot) |
| 0xe1 | [ReleaseSnapshot](Snapshots.md#releasesnapshot)|
| 0xe2 | [DownloadSnapshot](Snapshots.md#downloadsnapshot)|
| 0xe3 | [GetFileFragment](Snapshots.md#getfilefragment)|
| 0xf0 | Scrub (deprecated memcached bucket only) |
| 0xf1 | [ISASL refresh](#f1-isasl-refresh) |
| 0xf2 | Ssl certs refresh (Not supported) |
| 0xf3 | Get cmd timer |   
| 0xf4 | [Set ctrl token](#0xf4-set-ctrl-token) |
| 0xf5 | [Get ctrl token](#0xf5-get-ctrl-token) |
| 0xf6 | [Update External User Permissions](ExternalAuthProvider.md#updateexternaluserpermission) |
| 0xf7 | [RBAC refresh](#0xf7-rbac-refresh) |
| 0xf8 | [AUTH provider](#0xf8-auth-provider) |
| 0xfb | Drop Privilege (for testing) |
| 0xfc | Adjust time of day (for testing) |
| 0xfd | EwouldblockCtl (for testing) |
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
* MAY have value.

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

If a value is present it must be a JSON payload (and the JSON datatype set)
which adds additional parameters and arguments to the stats call. It is up
to each stat subgroup to define the schema. As long as the provided payload
is _valid json_, the server will silently (from the clients perspective)
ignore unknown elements in the provided JSON.

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

#### Stat(dcp)

The DCP stats group accepts the following parameters, specified as a JSON value.

```json
{
  // Optional format for the Producer/Consumer stream statistics.
  // The legacy format is used if not specified.
  // - "legacy" - every stream statistic becomes an output statistic
  // - "json" - all stream statistics become a single output statistic
  // formatted as JSON
  // - "skip" - no  stream statistics are generated
  "stream_format": "legacy|json|skip",
  "filter": {
    // Include statistics for DCP connections with specified username only.
    "user": "string",
    // Include statistics for DCP connections with specified port only.
    "port": "int"
  }
}
```

For details on retrieving Fusion-specific statistics, see
[Fusion Stats](fusion.md#0x10---fusion-stats).

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

See [feature.h](../include/mcbp/protocol/feature.h) for the defined features/

Response:

* MUST NOT have extras.
* MUST NOT have key.
* MAY have value.

The server replies with all the features the client requested that
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

### 0x2a Set Bucket Throttle Properties

The `Set Bucket Throttle Properties` command is used to set bucket-specific 
throttle properties.

The command should be used from the throttle manager to push new throttle
limits to the various nodes in the cluster. 

Request:

* MUST NOT extra
* MUST have key (the name of the bucket)
* MUST have value
* MUST NOT set CAS
* Datatype MUST be set to JSON
* Require the BucketThrottleManagement privilege

The value contains a JSON document documented in
[Bucket Properties](Throttling.md#bucket-properties))

The successful return message carries no extra userdata.

### 0x2b Set Bucket Data Limit Exceeded

The `Set Bucket Data Limit Exceeded` command is used to inform memcached
that the data limit of the bucket is exceeded and that clients are no longer
allowed to store additional data (The same command is also used to clear
the setting). Once set, the clients may only get and delete data. Note
that this setting does NOT affect DCP traffic.

Request:

* MUST have extra
* MUST have key (the name of the bucket)
* MUST NOT have value
* MUST NOT set CAS
* Datatype MUST be set to RAW
* Require the BucketThrottleManagement privilege

The extra section is encoded in the following way:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Code in network byte order    |               |               |
        +---------------+---------------+---------------+---------------+
        Total 2 bytes

To clear (and allow data ingress) use "Success" (0)

The following values may be set to disallow data ingress (and the error
message is used in the reply to the client):

    BucketSizeLimitExceeded (0x35)
    BucketResidentRatioTooLow (0x36)
    BucketDataSizeTooBig (0x37)
    BucketDiskSpaceTooLow (0x38)

The successful return message carries no extra userdata.

### 0x2c Set Node Throttle Properties

The `Set Node Throttle Properties` command is used to set node-specific
throttle properties.

The command should be used from the throttle manager to push new throttle
limits to the various nodes in the cluster.

Request:

* MUST NOT extra
* MUST NOT have key
* MUST have value
* MUST NOT set CAS
* Datatype MUST be set to JSON
* Require the BucketThrottleManagement privilege

The value contains a JSON document documented in 
[Node Properties](Throttling.md#node-properties))

The successful return message carries no extra userdata.

### 0x3d Set VBucket

The `set vbucket` command is used to set the state of vbucket.

Request:

* MUST have extra
* MUST NOT have key
* MAY have value

Extras contain a single byte containing the vbucket state and may be one of:

    1 - Active
    2 - Replica
    3 - Pending
    4 - Dead

If a value is present the datatype must be set to JSON and the payload
contains extra information for the vbucket (As it is JSON and may change
over time it is not documented in this document).

All other values for datatype is invalid.

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

The `del vbucket` command is used to delete a vbucket.

The command works synchronously or asynchronously.

When a synchronous delete is requested, the command only returns once the
vbucket is considered completely deleted by the bucket, e.g. all memory and disk
artefacts removed.

An asynchronous delete returns quickly and subsequent operations will fail with
not-my-vbucket, but the vbucket artefacts may still remain in memory and
on-disk.

Request:

* MUST NOT have extras
* MUST NOT have key
* MAY have a value

Response:

* MUST NOT have extras
* MUST NOT have key
* MUST NOT have value

To request a synchronous vbucket delete the request value must be set to the
following 7-byte ascii string (the request length set to 7).

      Byte/     0       |
         /              |
        |0 1 2 3 4 5 6 7|
        +---------------+
       0|a s y n c = 0  |
        +---------------+
        Total 7 bytes

### 0x49 GetEx
### 0x50 GetExReplica

GetEx and GetExReplica are used to retrieve an item from the server.
GetEx retrieves the item from the active vbucket, while GetExReplica
retrieves the item from a replica vbucket.

In order to use these commands the client must enable the following
features on the connection:

* SnappyEverywhere
* XATTR
* Datatype JSON

Request:

* MUST NOT have extras.
* MUST have key.
* MUST NOT have value.

Response:

* MUST have extras (4 bytes; the flags for the item).
* MUST NOT have key
* MAY have value

The result of the operation is signaled through the status code and
upon success the value contains the full data for the item, including
any extended attributes (XATTRs) and the value. If the datatype is
set to SNAPPY the *entire value* is compressed using Snappy. If the
datatype contains XATTR the extended attributes is stored as part
of the value. See the section "XAttr - extended attributes" in
Documents.md for more information on the encoding.

### 0x84 SetChronicleAuthToken

Sets the Chronicle Authetication Token for the given bucket.

The request has:
* No extras
* No key
* A value (JSON object encoding the arguments)
* datatype must be JSON and client must enable JSON when issuing HELO

The value is a JSON object, detailed below.

## JSON definition

The following keys are accepted input. All keys are mandatory.

* The uuid to of the snapshot being released
    * `"token"`
    * The value is a string

### Examples

```
{
  "token": "some-token"
}
```

### Returns

The call returns Status::Success, an error code otherwise.

### Errors

**Status::Einval (0x04)**

Input validation failure (e.g. incorrect arg format). The returned error context
will contain details.

**Status::Einternal (0x84)**

This status code is used for unexpected internal failure.

### 0x85 Create Bucket

The `create bucket` command is used to create a new bucket in the system.
The connection must hold the "Node Supervisor" privilege to run this command.

Request:

* MUST NOT have extra
* MUST have key
* MUST have value

The key contains the name of the bucket to create, and the value contains
the properties for the bucket to create in the format:

    "module\0configuration"

(Note that `\0configuration` may be omitted if no configuration is needed).

Module may be one of the following:
   * nobucket.so
   * ep.so
   * ewouldblock_engine.so

The status of the operation will be returned in the status code of the
response message.

### 0x86 Delete Bucket

The `delete bucket` command is used to delete a given bucket. The connection
must hold the "Node Supervisor privilege"

Request:

* MUST NOT have extra
* MUST have key
* MAY have value

The key holds the name of the bucket to delete, and the value _MAY_ contain
a JSON payload with the following fields:

    force - Boolean value indicating if we should bypass graceful shutdown
    type  - String value indicating the bucket type to delete (fail if the
            bucket exists but is of a different type).

`type` may be one of:
    * Memcached
    * Couchbase
    * ClusterConfigOnly
    * EWouldBlock
    * No Bucket

`Delete bucket` will wait for all ongoing commands to complete and all
associated clients to disconnect from the bucket before the bucket will
be shut down.

The status of the operation will be returned in the status code of the
response message.

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
on the server. The special name "@no bucket@" may be used to disassociate
the connection from all buckets.

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


### 0x8a Pause Bucket

The `Pause Bucket` command is used to inform memcached to pause all traffic
(apart from a few exceptions - see below) on the specified bucket, and to
complete any outstanding disk modification operations.

On a successful request, `Pause Bucket` will:

1. Wait for all in-flight client requests to complete.
2. Disconnect all connections associated with the bucket.
3. Instruct the disk subsystem to quiesce.
4. Prevent any clients from associating with the bucket (via `Select Bucket`).

After a successful `Pause Bucket` command, the bucket cannot be selected
(via `Select Bucket`) and hence all "normal" commands operating on the selected
bucket cannot be issued. Only the following administrative commands can be
issued to the paused bucket:

* `Stat("bucket_details [bucket_name]")` - to determine the state of the
bucket / monitor the progress of `Pause Bucket` command.
* `Resume Bucket` - to unpause the bucket and return to the Ready state.
* `Delete Bucket`

Given `Pause Bucket` requires in-flight IO to complete, it can take
an arbitrary amount of time to complete (although it should typically be fast
if the bucket is idle). As such, it is possible to _cancel_ an in-flight
`Pause Bucket` command by issuing a `Resume Bucket` command (on a different
connection, given `Pause Bucket` is blocking). This will cancel the runnng pause at the next
cancellation point, returning the bucket to the running state.

#### Request:

* MUST NOT have extra
* MUST have key
* MUST NOT have value

#### Response:

* MUST NOT have extras.
* MUST NOT have key.
* MAY have value.

The key in the request is the name of the bucket to pause.

The value in the response is empty on a successful pause (status: Success), or
on failure a textual description of why the request failed.

### 0x8b Resume Bucket

The `Resume Bucket` command is used to inform memcached to resume all traffic
on the specified bucket - normally after a bucket has been paused via
[Pause Bucket](#0x8a-pause-bucket).

After successful execution the bucket can again be selected by clients via
[Select bucket](#0x89-select-bucket) and operations performed.

#### Request:

* MUST NOT have extra
* MUST have key
* MUST NOT have value

#### Response:

* MUST NOT have extras.
* MUST NOT have key.
* MAY have value.

The key in the request is the name of the bucket to resume.

The value in the response is empty on a successful resume (status: Success),
or on failure a textual description of why the request failed.

### 0x92 Observe

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

### 0x94 Get locked

Locks the item for mutations for a specified period of time. Locking an item
updates the CAS and the new CAS value is returned. The item becomes unlocked:
* on a successful mutation
* when the timeout expires
* when manually unlocked
* on node shutdown, as the locked status is NOT persisted to disk

The locked status is maintained on a best-effort basis; a failover/rebalance
may also cause it to be lost. The intended use is as a helper to a CAS
operation, so that concurrent threads don't proceed to update the same item.

Request:

* MAY have extras.
* MUST have key.
* MUST NOT have value.

Extra data:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Expiration (seconds)                                          |
        +---------------+---------------+---------------+---------------+
        Total 4 bytes

Response (if found):

* MUST have extras.
* MAY have key.
* MAY have value.

Extras defines a 4-byte value which represent the duration in seconds that the
lock will remain held, the lock expiration time. After this amount of time has
elapsed the server will automatically release the lock, the lock is released in
a lazy manner when certain functions happen to visit the locked document. The
extras is optional and if not included in the request, the request will behave as
if a value of 0 was specified.

When the command executes the expiration input is checked as follows.

* If expiration is 0 then server will use the default timeout.
* If expiration is > maximum then the server will use the default timeout.
* Else input expiration is used.

The default and maximum expiration values are defined by the configuration,
parameters getl_default_timeout and getl_max_timeout.

Added in CB Server v1.6.3 (d86d4d9c746266449d39cbb89904b86a4e6e6796).

### 0x95 Unlock key

A key locked via the Get locked command can be unlocked explicitly before the
expiry of the lock timeout by means of the Unlock key command.

Request:

* MUST NOT have extras.
* MUST have key.
* MUST NOT have value.
* CAS MUST match locked CAS

Response (if found):

* MUST have extras.
* MAY have key.
* MAY have value.

If the key is found locked and the CAS matches the request CAS,
the key is unlocked and Success is returned.
If the key is found locked and the CAS does NOT match, Locked is returned.
If the key is found NOT locked, NotLocked is returned.
If the key is not found (after disk lookup), KeyEnoent is returned.

      Found? ─────► [KeyEnoent]
        │      no
        │ yes
        ▼
     Locked? ─────► [NotLocked*]
        │      no
        │ yes
        ▼
    CAS match? ────► [Locked*]
        │       no
        │ yes
        ▼
    [Success]

For clients which do not enable XError, ETmpfail is returned instead of
Locked/NotLocked.

Note: The locked status is NOT persisted to disk, hence the metadata of
locked items are not evicted even in full eviction mode.

Added in CB Server v1.6.3 (d201a5ccd800fc78e0b10bfcc7638e7708d87bba).

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

### 0xb5 Get Cluster Config

The `get cluster config` is used to get the cached cluster configuration from
memcached instead of getting it directly from ns_server via the REST interface.

Request:

* MAY have extras
* MUST NOT have key
* MUST NOT have value
* CAS MUST be set to 0

The command returns the cluster configuration for the _selected_ bucket.
If no bucket is selected, the global cluster configuration is returned if
the client holds the "System Settings" privilege.

If the command contains extras, the extras section contains a revision number
the server should use for deduplication:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|                                                               |
       4| Epoch in network byte order (signed int 64 bits)              |
        +---------------+---------------+---------------+---------------+
       8|                                                               |
      12| Revision in network byte order (signed int 64 bits)           |
        +---------------+---------------+---------------+---------------+
        Total 16 bytes

The command will also update the "known cluster configuration" for the client which is
used for deduplication in the case of a "not my vbucket" response.

Response:

If the request contained a revision number, the actual clustermap in the
response will only be present iff the server has a revision which is newer
than the revision in the request.

If successful, the command responds with the cluster configuration.

### 0xb6 Get Random Key

The `get random key` command allows the client to retrieve a key from any valid
collection (or default collection for non-collection enabled connections).

The command will search resident items only using a randomised vbucket as a
start point and then randomised hash-tables buckets for searching within a
vbucket.

Note that the command will only search a vbucket, if the collection's item count
of the vbucket is greater than zero. The collection item count is only
updated when a mutation is persisted and persistence executes asynchronously.
A Get Random Key issued after any store operation will be guaranteed success
until the asynchronous persistence task has updated the collection statistics.
This is also true when the store uses durability.

Request:

* MAY have extras if client has enabled collections (see HELO)
* MUST NOT have key
* MUST NOT have value

If the command contains an extra section it must encode a collection-ID as a
4 byte network order integer. The extras is only required when the client has
enabled collection using [HELO](#0x1f-helo).

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| collection-id in network byte order                           |
        +---------------+---------------+---------------+---------------+
        Total 4 bytes

It is possible to request extended attributes to be returned, and in order
to do so the command contains an extra section it must encode a collection-ID
as a 4 byte network order integer followed by a single byte set to a value != 0.


      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| collection-id in network byte order                           |
        +---------------+---------------+---------------+---------------+
       4|     0xff      |
        +---------------+
        Total 5 bytes


Response:

If successful the command responds with the randomly found key and value.

* MUST NOT have extra
* MUST have key
* MUST have value

Errors:

PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)

The request cannot find a key (collection empty or not resident).

PROTOCOL_BINARY_RESPONSE_UNKNOWN_COLLECTION (0x88)

The collection does not exist

PROTOCOL_BINARY_RESPONSE_EACCESS (0x24)

The caller lacks the correct privilege to read documents

### 0xf1 ISASL refresh

The `ISASL refresh` command is used to instruct memcached to read the
password database from file. The connection must hold the "Node Supervisor"
privilege.

Request:
* MUST NOT have extra
* MUST NOT have key
* MUST have value
* CAS must be 0

Response:
* Status code indicate the result of the operation

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

### 0xf7 RBAC Refresh

The `RBAC Refresh` command is used to instruct memcached to read the
RBAC database from file. The connection must hold the "Node Supervisor"
privilege.

Request:
* MUST NOT have extra
* MUST NOT have key
* MUST have value
* CAS must be 0

Response:
* Status code indicate the result of the operation

### 0xf8 Auth provider

The `AUTH provider` command is used to register this connection as an
auth provider to the system. The connection must hold the "Node supervisor"
privilege and the connection MUST enable Duplex mode prior to trying
to register as an Auth Provider.

Request:
   * MUST NOT have extra
   * MUST NOT have key
   * MUST have value
   * CAS must be 0

Response:
   * Status code indicate the result of the operation

See [External Auth Provider](ExternalAuthProvider.md) for more
information about auth provider

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

## Server Commands

The following chapter describes the packet layout for the various server
commands.

### 0x01 Clustermap Change Notification

The server will push the new cluster map to the clients iff the client
subscribes to clustermap notifications (see [HELO](#0x1f-helo)). The client
may either subscribe to full (contains the cluster map) or brief (does
not contain the actual cluster map) notifications.

The request:
* Must have extras
* May have key
* May have value

The key contains the name of the bucket (empty indicates the global config).

The layout of the extras section depends on the version of the request.
The version is inferred from the length of the extras.

If the extras section is 4 bytes long, the extras contain a single
unsigned 32-bit integer in network byte order. This is the revision
number of the clustermap. This version of the request does not support
the concept of revision epoch.

If the extras section is 16 bytes long, the extras contain two
signed 64-bit integers in network byte order. The first 8 bytes are the
revision epoch, and the second 8 bytes are the revision number.
The epoch value may be negative to indicate the epoch is not yet
initialized. The revision value is never negative.

The value (if present) contains the full cluster map.

The server does not need a reply to the message (it is silently dropped without
any kind of validation).

### 0x02 Authenticate

See [External Auth Provider](ExternalAuthProvider.md#authentication-request).

### 0x03 Active External Users

See [External Auth Provider](ExternalAuthProvider.md#activeexternalusers-request).

