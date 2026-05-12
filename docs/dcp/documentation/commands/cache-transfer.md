### Cache Transfer (opcode 0x66)

Sent from the producer to the consumer during the
[CacheTransfer](../cache_transfer.md) phase of a stream. Each message carries
one or more items copied from the producer's hash-table.

See [cache_transfer.md](../cache_transfer.md) for the wider feature overview.

The request:
* Must not have extras
* Must not have key
* Must have a value containing one or more serialized items
* request.extlen must be 0 (and is currently used as version indicator)
* request.cas must be 0

The message body is the value payload only - there is no extras section and no
key field in the header. The value is a concatenation of one or more records:

       +----------------------------------------+
       | DcpCacheTransferPayload (40 bytes)     |
       +----------------------------------------+
       | key bytes (key_len bytes)              |
       +----------------------------------------+
       | value bytes (value_len bytes, may be 0)|
       +----------------------------------------+
                       ... repeats ...

`DcpCacheTransferPayload` is fixed at 40 bytes and is laid out as follows:

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| cas (network byte order)                                      |
       |                                                               |
       +---------------+---------------+---------------+---------------+
      8| by_seqno (network byte order)                                 |
       |                                                               |
       +---------------+---------------+---------------+---------------+
     16| rev_seqno (network byte order)                                |
       |                                                               |
       +---------------+---------------+---------------+---------------+
     24| value_len (network byte order)                                |
       +---------------+---------------+---------------+---------------+
     28| flags (host order, same as DCP mutation)                      |
       +---------------+---------------+---------------+---------------+
     32| expiration (network byte order)                               |
       +---------------+---------------+---------------+---------------+
     36| key_len (network byte order)  | datatype      | cacheHint     |
       +---------------+---------------+---------------+---------------+
       Total 40 bytes

* **cas** - The document's CAS
* **by_seqno** - The document's by-sequence-number. Will be
  `<= start-seqno` of the originating stream request. No ordering is implied
  between items in the same or different `DcpCacheTransfer` messages.
* **rev_seqno** - The document's revision sequence number.
* **value_len** - Length of the value bytes that follow the key. May be `0` if
  the producer decided to ship only the key/metadata (see the discussion of
  `all_keys` in [cache_transfer.md](../cache_transfer.md)).
* **flags** - The document's user-flags. Sent as-is (no byte-order change).
* **expiration** - The document's expiry time (seconds since epoch). `0` means
  no expiry.
* **key_len** - Length of the key bytes. Must be in the range
  `[2, MaxCollectionsKeyLen]`. The key is collection-aware and is prefixed
  with the collection-ID encoded as an unsigned LEB128.
* **datatype** - The datatype of the value bytes that follow. When the
  producer downgrades to "key only" the datatype field is preserved as the
  document's underlying datatype, even though `value_len` is `0`.
* **cacheHint** - An opaque byte carrying the StoredValue's
  frequency-counter / MFU / NRU eviction hint. The consumer should plumb this
  into the StoredValue it creates so eviction behaviour is preserved across
  the transfer.

### Stream-ID

If the producer was opened with the stream-ID feature enabled the message uses
the `AltClientRequest` magic (`0x08`) and carries a 3-byte
`DcpStreamIdFrameInfo` framing-extras section (tag byte + 2 byte sid). The
`sid` matches the value the consumer chose in the
[stream-request value JSON](stream-request-value.md#sid).

### Example

The following message contains a single item with a 5 byte key `"hello"` and
a 5 byte value `"world"`:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x66          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x00          | 0x02          | 0x10          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x32          |
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
      36| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
      40| 0x00          | 0x00          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
      44| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      48| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      52| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      56| 0x00          | 0x05          | 0x00          | 0x0a          |
        +---------------+---------------+---------------+---------------+
      60| 0x68 ('h')    | 0x65 ('e')    | 0x6c ('l')    | 0x6c ('l')    |
        +---------------+---------------+---------------+---------------+
      64| 0x6f ('o')    | 0x77 ('w')    | 0x6f ('o')    | 0x72 ('r')    |
        +---------------+---------------+---------------+---------------+
      68| 0x6c ('l')    | 0x64 ('d')    |
        +---------------+---------------+

    DCP_CACHE_TRANSFER command
    Field             (offset) (value)
    Magic             (0)    : 0x80
    Opcode            (1)    : 0x66
    Key length        (2,3)  : 0x0000
    Extra length      (4)    : 0x00
    Data type         (5)    : 0x00
    Vbucket           (6,7)  : 0x0210
    Total body        (8-11) : 0x00000032 (50 bytes: 40 payload + 5 key + 5 value)
    Opaque            (12-15): 0x00001210
    CAS               (16-23): 0x0000000000000000
    Body (item 0):
      cas             (24-31): 0x0000000000000004
      by_seqno        (32-39): 0x0000000000000001
      rev_seqno       (40-47): 0x0000000000000005
      value_len       (48-51): 0x00000005
      flags           (52-55): 0x00000000
      expiration      (56-59): 0x00000000
      key_len         (60-61): 0x0005
      datatype        (62)   : 0x00
      cacheHint       (63)   : 0x0a
      key             (64-68): hello
      value           (69-73): world

### Validation (consumer side)

The message body is iterated and each record is validated by the
[`DcpCacheTransferBuffer`](../../../include/mcbp/protocol/dcp_cache_transfer_buffer.h)
iterator:

* The body must be at least `sizeof(DcpCacheTransferPayload) + 2` bytes (one
  header plus a minimum-length collection-aware key).
* For each record:
    * `key_len` must be `>= 2` and `<= MaxCollectionsKeyLen`.
    * The buffer must contain at least `40 + key_len + value_len` more bytes.
* If validation fails the consumer will surface a structured error (see the
  `errorContext` returned by `Iterator::getError`) and the connection will
  be disconnected.

### Returns

This message will not return a response unless an error occurs.

### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a stream does not exist for the vbucket specified on this connection.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If the body is shorter than the minimum
`DcpCacheTransferBuffer::minSize()` or the body cannot be successfully
iterated (invalid `key_len`, truncated record, etc.).

**(Disconnect)**

If this message is sent to a connection that is not a consumer, or if a
record fails validation during iteration.
