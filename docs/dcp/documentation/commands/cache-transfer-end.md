### Cache Transfer End (opcode 0x67)

Sent from the producer to the consumer to signal that the
[CacheTransfer](../cache_transfer.md) phase of a stream has finished.

A `DcpCacheTransferEnd` is **only** emitted when the stream's
`end_seqno > start_seqno`, i.e. the stream was requested to continue with a
regular ActiveStream after the cache transfer phase. In that case the
consumer should expect normal DCP messages (snapshot markers, mutations,
deletions, ...) for sequence numbers greater than `start_seqno` to follow.

If the stream was requested as cache-transfer-only (`end_seqno == start_seqno`)
the stream is terminated using a regular [StreamEnd](stream-end.md) message
instead.

The request:
* Must not have extras
* Must not have key
* Must not have value

The consumer should not send a reply to this command. The following example
shows the breakdown of the message:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x67          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x02          | 0x10          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x12          | 0x10          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+

    DCP_CACHE_TRANSFER_END command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x67
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0210
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0x00001210
    CAS          (16-23): 0x0000000000000000

### Stream-ID

If the producer was opened with the stream-ID feature enabled the message uses
the `AltClientRequest` magic (`0x08`) and carries a 3-byte
`DcpStreamIdFrameInfo` framing-extras section (tag byte + 2 byte sid). The
`sid` matches the value the consumer chose in the
[stream-request value JSON](stream-request-value.md#sid).

### Returns

This message will not return a response unless an error occurs.

### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a stream does not exist for the vbucket specified on this connection.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If the message has unexpected extras / key / value content.

**(Disconnect)**

If this message is sent to a connection that is not a consumer.
