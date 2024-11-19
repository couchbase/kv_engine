### Snapshot Marker (opcode 0x56)

Sent by the producer to tell the consumer that a new snapshot is being sent.

Until 7.2 a snapshot is a series of commands that is guaranteed to contain a unique set of keys. 7.2 introduced DCP change streams and now DCP can produce snapshots which include duplicated keys, a flag in the snapshot type field indicates if this has occurred.

There are more than one version of this message, which differ in the definition of the extras.

* V1 extras contains snapshot-type and the start and end seqnos.
* V2 extras contains a 1 byte version field defining how the value is to be encoded.

For V2 the following version codes are defined and determines the rest of the message encoding.

The request:
* Must have extras
* Must not have key
* Can only have a value if V2 format is in use (extra_len = 1)

The client should not send a reply to this command unless the ack flag is set.

#### Version byte values:

##### 0x00

_V2.0_ the value contains `start seqno`, `end seqno`, `snapshot-type`, `max visible seqno` and `high completed seqno`.
The `high completed seqno` should only be considered valid for use when the flags field has the `disk` flag set.

Snapshot Type is a bit field and stores the following flags:

| Flag | Name   | Description |
|------|---|------------|
| 0x01 | Memory | Specifies that the snapshot contains in-memory items only. |
| 0x02 | Disk | Specifies that the snapshot contains on-disk items only. |
| 0x04 | Checkpoint | An internally used flag for intra-cluster replication to help to keep in-memory data structures look similar. |
| 0x08 | Ack | Specifies that this snapshot marker should return a response once the entire snapshot is received |
| 0x10 | History | The snapshot represents a view of history, for collections which have history=true, this snapshot will not deduplicate the mutations of those collections |
| 0x20 | May Duplicate Keys | The snapshot may contain duplicate keys, breaking a snapshots "unique key" definition which has existed since DCPs inception. Without this flag the snapshot contains a set of unique keys |

##### 0x01

_V2.1_ is removed from the spec, it was never used.

##### 0x02

_V2.2_ extends _V2.0_ to include an 8-byte field for the server purge-seqno. Clients
can use this purge-seqno to mitigate rollback (by presenting the purge-seqno in
stream-requests).

The full spec of v2.2 is.

* 0:  1b version
* 1:  8b Start Seqno
* 9:  8b End Seqno
* 17: 4b Snapshot Type
* 21: 8b Max Visible Seqno
* 29: 8b High Completed Seqno
* 37: 8b Purge Seqno

### Encoding Examples

The following example shows the breakdown of the V1 message:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x56          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x14          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      32| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      36| 0x00          | 0x00          | 0x00          | 0x08          |
        +---------------+---------------+---------------+---------------+
      40| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+

    DCP_SNAPSHOT_MARKER command
    Field           (offset) (value)
    Magic           (0)    : 0x80
    Opcode          (1)    : 0x56
    Key length      (2,3)  : 0x0000
    Extra length    (4)    : 0x14
    Data type       (5)    : 0x00
    Vbucket         (6,7)  : 0x0000
    Total body      (8-11) : 0x00000014
    Opaque          (12-15): 0xdeadbeef
    CAS             (16-23): 0x0000000000000000
      Start Seqno   (24-31): 0x0000000000000000
      End Seqno     (32-39): 0x0000000000000008
      Snapshot Type (40-43): 0x00000001 (disk)

The following example shows the breakdown of the V2 message, when the version byte is 0.

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x56          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x14          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0xde          | 0xad          | 0xbe          | 0xef          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x01          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      32| 0x01          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      36| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      40| 0x08          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      44| 0x02          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      48| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      52| 0x08          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      56| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      60| 0x07          |
        +---------------+

    DCP_SNAPSHOT_MARKER command
    Field                  (offset) (value)
    Magic                  (0)    : 0x80
    Opcode                 (1)    : 0x56
    Key length             (2,3)  : 0x0000
    Extra length           (4)    : 0x01
    Data type              (5)    : 0x00
    Vbucket                (6,7)  : 0x0000
    Total body             (8-11) : 0x00000025
    Opaque                 (12-15): 0xdeadbeef
    CAS                    (16-23): 0x0000000000000000
      Version              (24):    0x00
      Start Seqno          (25-32): 0x0000000000000001
      End Seqno            (33-40): 0x0000000000000008
      Snapshot Type        (41-44): 0x00000002 (disk)
      Max Visible Seqno    (45-52): 0x0000000000000008
      High Completed Seqno (53-60): 0x0000000000000007

### Returns

This message will not return a response unless an error occurs or the ack flag is set.

### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a stream does not exist for the vbucket specfied on this connection.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**(Disconnect)**

If this message is sent to a connection that is not a consumer.

### Implementation notes

The implementation of DCP has lead to some inconsistencies in the way that the
snapshot marker assigns the value of "Start Seqno" depending on the context.

Note that [stream-request](stream-request.md) defines "Start Seqno" to be
maximum sequence number that the client has received. A request with a start
seqno number of X, means "I have X, please start my stream at the sequence
number after X".

#### Memory snapshot.start-seqno

A stream which is transferring in-memory checkpoint data may set the snapshot
start seqno in one of three different ways:

1) The first snapshot-marker of the stream will have the start-seqno set to the
   requested start-seqno.
2) A snapshot-marker with the CHK marker flag set will set the start-seqno to
   the snapStartSeqno of the Checkpoint object. This sequence number is the
   sequence number of the first item that belongs to this snapshot. The client
   may or may not see the mutation with that seqno due to either de-dupe or
   filtering for collections or durable writes. This behaviour is consistent
   with the behaviour of disk snapshots that are sent from the CheckpointManager
   rather than backfill.
3) A snapshot without the CHK marker flag set will set the start-seqno to the
   seqno of the first mutation that will follow the marker. This matches with
   the semantics of stream-request where the start-seqno is something the client
   already has.

In the case of 3, a client may perform a stream-request with a start-seqno of X,
but due  to de-duplication X+n is the first sequence number available
(from memory), the client will receive:

* TX `stream-request{start-seqno=X}`
* RX `stream-request-response{success}`
* RX `snapshot-marker{start=X+n, end=Y, flags=0x1}`
* RX `mutation{seqno:X+n}`

#### Disk snapshot-marker.start-seqno

A stream which is transferring disk snapshot data may set the snapshot start
seqno in one of three different ways:

1) The first snapshot-marker of the stream will have the start-seqno set to the
   requested start-seqno regardless of whether or not the snapshot comes from 
   backfill or is sent from the CheckpointManager.
2) Snapshot markers from backfills other than the first in the stream will send
   the starting sequence number of the backfill as the start-seqno
3) Snapshots sent from the CheckpointManager will set the start-seqno to the 
   snapshot start seqno of the Checkpoint object. The client may or may not see
   the mutation with that seqno due to de-dupe. This is required as
   SyncReplication consumers need Disk snapshot start-seqnos to reflect the full
   extent of the original snapshot rather than just the set of mutations sent.

In the case of 1, this is not consistent with the in-memory case, but is
consistent with stream-request semantics. For example:

* TX `stream-request{start-seqno=X}`
* RX `stream-request-response{success}`
* RX `snapshot-marker{start=X, end=Y, flags=0x2}`
* RX `mutation{seqno:X+n}`

Note: A stream could at any time switch from memory to disk if the client is
deemed to be slow.
