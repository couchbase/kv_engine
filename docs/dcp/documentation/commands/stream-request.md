### Stream Request (opcode 0x53)

Sent by the consumer side to the producer specifying that the consumer wants to create a vbucket stream. In order to initial a stream for a vbucket the consumer must send the following command below. In order to initiate multiple stream the consumer needs to send multiple commands. The value specified in opaque in the stream request packet will be used as opaque field in all commands sent for the stream.

* A stream-request can be configured to use collections by encoding a
[JSON](stream-request-value.md) object in the request.
* Stream-request resumption must also include a collection's manifest-UID in the
[value](stream-request-value.md).

The request:
* Must have extras
* Must not have key
* [May have a value](stream-request-value.md)

Extra looks like:

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| Flags                                                         |
       +---------------+---------------+---------------+---------------+
      4| RESERVED                                                      |
       +---------------+---------------+---------------+---------------+
      8| Start sequence number                                         |
       |                                                               |
       +---------------+---------------+---------------+---------------+
     16| End sequence number                                           |
       |                                                               |
       +---------------+---------------+---------------+---------------+
     24| VBucket UUID                                                  |
       |                                                               |
       +---------------+---------------+---------------+---------------+
     32| Snapshot Start Seqno                                          |
       |                                                               |
       +---------------+---------------+---------------+---------------+
     40| Snapshot End Seqno                                            |
       |                                                               |
       +---------------+---------------+---------------+---------------+
       Total 48 bytes

* **Flags** - Used to specify extra information added in the extra section for modifying what the stream send. The available flags for this message are the same ones specified for the [Add Stream](add-stream.md) message.
* **Start By Seqno** - Specified the last by sequence number that has been seen by the consumer.
* **End By Seqno** - Specifies that the stream should be closed when the sequence number with this ID has been sent.
* **VBucket UUID** - A unique identifier that is generated that is assigned to each VBucket. This number is generated on an unclean shutdown or when a Vbucket becomes active.
* **Snapshot Start Seqno** - Set to the same value as the start seqno by default, in case it is a retry because the stream request didn't return all expected results use the start sequence of the last partial snapshot that was received.
* **Snapshot End Seqno** - Set to the same value as the start seqno by default, in case it is a retry because the stream request didn't  return all expected results, use the end sequence of the last partial snapshot that was received.

Set VBucket UUID to 0 to receive all data from the specified vbucket. This may be thought of as a special case where the consumer don't have any data at all (to eliminate the need of requesting the failover log first).

The response:
* Must not have extras
* Must not have key
* May have value

On an "OK" response the failover log is included. The "rollback" response contains the sequence number to roll back to.

The following example tries to initiate a stream for vbucket 0 that continues from a given point in time, but the server can't continue from that point and tells the client to roll back to a different sequence. The client retries with the information that the server replied with, and the stream is established successfully.

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x53          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x30          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x30          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x10          | 0x00          |
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
      36| 0x00          | 0xff          | 0xee          | 0xdd          |
        +---------------+---------------+---------------+---------------+
      40| 0xff          | 0xff          | 0xff          | 0xff          |
        +---------------+---------------+---------------+---------------+
      44| 0xff          | 0xff          | 0xff          | 0xff          |
        +---------------+---------------+---------------+---------------+
      48| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      52| 0xfe          | 0xed          | 0xde          | 0xca          |
        +---------------+---------------+---------------+---------------+
      56| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      60| 0x00          | 0xff          | 0xee          | 0xdd          |
        +---------------+---------------+---------------+---------------+
      64| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      68| 0x00          | 0xff          | 0xee          | 0xff          |
        +---------------+---------------+---------------+---------------+
    DCP_STREAM_REQ command
    Field                  (offset) (value)
    Magic                  (0)    : 0x80
    Opcode                 (1)    : 0x53
    Key length             (2,3)  : 0x0000
    Extra length           (4)    : 0x30
    Data type              (5)    : 0x00
    Vbucket                (6,7)  : 0x0000
    Total body             (8-11) : 0x00000030
    Opaque                 (12-15): 0x00001000
    CAS                    (16-23): 0x0000000000000000
      flags                (24-27): 0x00000000
      reserved             (28-31): 0x00000000
      start seqno          (32-39): 0x0000000000ffeedd
      end seqno            (40-47): 0xffffffffffffffff
      vb UUID              (48-55): 0x00000000feeddeca
      snapshot start seqno (56-63): 0x0000000000000000
      snapshot end seqno   (64-71): 0x0000000000ffeeff


      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x53          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x23          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x08          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x10          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+

    DCP_STREAM_REQ response
    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x53
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0023 (Rollback)
    Total body   (8-11) : 0x00000008
    Opaque       (12-15): 0x00001000
    CAS          (16-23): 0x0000000000000000
      rollback # (24-31): 0x0000000000000000

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x53          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x30          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x30          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x10          | 0x00          |
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
      36| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      40| 0xff          | 0xff          | 0xff          | 0xff          |
        +---------------+---------------+---------------+---------------+
      44| 0xff          | 0xff          | 0xff          | 0xff          |
        +---------------+---------------+---------------+---------------+
      48| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      52| 0xfe          | 0xed          | 0xde          | 0xca          |
        +---------------+---------------+---------------+---------------+
      56| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      60| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+

    DCP_STREAM_REQ command
    Field                  (offset) (value)
    Magic                  (0)    : 0x80
    Opcode                 (1)    : 0x53
    Key length             (2,3)  : 0x0000
    Extra length           (4)    : 0x30
    Data type              (5)    : 0x00
    Vbucket                (6,7)  : 0x0000
    Total body             (8-11) : 0x00000030
    Opaque                 (12-15): 0x00001000
    CAS                    (16-23): 0x0000000000000000
      flags                (24-27): 0x00000000
      reserved             (28-31): 0x00000000
      start seqno          (32-39): 0x0000000000000000
      end seqno            (40-47): 0xffffffffffffffff
      vb UUID              (48-55): 0x00000000feeddeca
      snapshot start seqno (56-63): 0x0000000000000000
      snapshot end seqno   (64-71): 0x0000000000000000

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x81          | 0x53          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x40          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x10          | 0x00          |
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

    DCP_STREAM_REQ response
    Field        (offset) (value)
    Magic        (0)    : 0x81
    Opcode       (1)    : 0x53
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000040
    Opaque       (12-15): 0x00001000
    CAS          (16-23): 0x0000000000000000
      vb UUID    (24-31): 0x00000000feeddeca
      vb seqno   (32-39): 0x0000000000005432
      vb UUID    (40-47): 0x0000000000decafe
      vb seqno   (48-55): 0x0000000001343214
      vb UUID    (56-63): 0x00000000feedface
      vb seqno   (64-71): 0x0000000000000004
      vb UUID    (72-79): 0x00000000deadbeef
      vb seqno   (80-87): 0x0000000000006524

### Returns

A status code indicating whether or not the operation was successful.

### Error codes

**PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS (0x02)**

If a stream for this VBucket already exists on the same connection.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET (0x07)**

If the VBucket the stream is requested for does not exist.

**PROTOCOL_BINARY_RESPONSE_ERANGE (0x22)**

This error code may be returned for one of the following reason. Check the server logs for more information on which case occurred.

* If the start and end sequence numbers are specified incorrectly. For example the start sequence number cannot be bigger than the current high sequence number in the VBucket. The start sequence number must also be less than the end sequence number.
* If the snapshot start or snapshot end seqno are not specified correctly. For all stream requests the snapshot start seqno must be less than or equal to the start seqno and the start seqno must be less than or equal to the snapshot end seqno.

**PROTOCOL_BINARY_RESPONSE_ROLLBACK (0x23)**

If the consumer needs to rollback its data before reconnecting.

**PROTOCOL_BINARY_RESPONSE_ENOMEM (0x82)**

If the failover log could not be sent to due a failure to allocate memory.

**PROTOCOL_BINARY_RESPONSE_MANIFEST_IS_AHEAD (0x8b)**

The client's manifest `uid` (encoded in the [stream-request value](stream-request-value.md))
is ahead of the vbucket's. Provided that the client observed the manifest uid from
the cluster, the vbucket it is trying to request should catch up with the uid the
client is using.

The client should briefly pause and then retry the stream-request.

**PROTOCOL_BINARY_RESPONSE_DCP_STREAMID_INVALID (0x8d)**

A stream-request was made and either of the following situations was detected.

* The request includes a [value](stream-request-value.md) which includes a sid
and the stream-ID feature is not [enabled](control.md).
* The request does not include a sid and the stream-ID feature is [enabled](control.md).


**(Disconnect)**

A disconnect may happen for one of two reasons:

* If the connection state no longer exists on the server. The most likely reason this will happen is if another connection is made to the server from a different client with the same name as the current connection.
* If this command is sent to a consumer endpoint.
