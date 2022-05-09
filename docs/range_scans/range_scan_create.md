# Create Range Scan (0xDA)

Requests that the server creates a new range scan against a vbucket, the client
configures the scan using a JSON object that is sent as the value of the request.

The request:
* No extras
* No key
* A value (JSON data encoding the required configuration)
* datatype must be JSON and client must enable JSON when issuing HELO

The range scan create gives the client a number of options for scanning a
collection in a vbucket.

* An inclusive or exclusive (TBD) scan using a start and end key.
* A random sample of keys where the user chooses a seed and sample size.
* The scan can require a specific vb-uuid and sequence number to be stored.

All scans must target active vbuckets only.

## JSON definition

The following keys are accepted input. Any keys not shown in the following
sections will be ignored.

### Root-level keys

The following keys will be checked for at the root of the JSON object

* The collection of the scan
  * `"collection"`
  * If this key is omitted, the scan will use the default collection.
  * The value is a string
  * The value is the collection-ID as hexadecimal. This matches other interfaces
    where a collection-ID is included in a JSON object.
  * For example collection with base10 ID 2748 is encoded as "abc"

* Key or Key/Value configuration of the scan
  * `"key_only"`
  * The value is a bool
  * When set to true the scan will only return keys
  * When set to false (or omitted) the scan will return the complete "document",
    that is the key, metadata and value

* Range-scan configuration
  * `"range"`
  * value is a JSON object (described below)
  * When omitted the `"sampling"` key must be included.

* Sampling configuration
  * `"sampling"`
  * value is a JSON object (described below)
  * When omitted the `"range"` key must be included.

* Snapshot Requirements
  * `"snapshot_requirements"`
  * value is a JSON object (described below)
  * This key can be omitted

### For a range-scan

The range-scan configuration is defined in the `"ranges"` object, the object has
the following keys.

Key values are base64 encodings of the desired strings, allowing for much
simpler encoding of all keys.

* The start key of the scan
  * `"start"`
  * value is a string
  * value must be base64 encoded
  * The start key must be defined
  * The scan will be inclusive of the start key
* The end key of the scan
  * `"end"`
  * value is a string
  * value must be base64 encoded
  * The end key must be defined
  * The scan will be inclusive of the end key

Currently the range-scan feature does not support a way to request a scan is
exclusive of the start/end, such a feature is planned for simpler 'failover'
scenario.

### For a random-sample

The random-sample configuration is defined in the `"sampling"` object, the
object has the following keys.

* The prng seed to use
  * `"seed"`
  * value is a number
  * If omitted the seed is 0
* The sample size
  * `"samples"`
  * value is a number
  * This key must be included in the `"sampling"` object.

### Snapshot Requirements

The request can include a set of requirements that the vbucket snapshot must
meet in-order for the request to be successful. The `"snapshot_requirements"`
can be defined for either a range-scan or a random-sample.

* The 64-bit vbucket uuid which the snapshot must match for the create to
  succeed.
  * `"vb_uuid"`
  * value is a string encoding a base10 number
  * This key must be defined in the `"snapshot_requirements"` object.
* A sequence number that must of been persisted for the create to succeed.
  * `"seqno"`
  * value is a number
  * This key must be defined in the `"snapshot_requirements"` object.
* If a mutation is still stored for the sequence number.
  * `"seqno_exists"`
  * value is a bool
  * When true the scan will check the by-seqno index for existince of an item
    with `"seqno"`
  * When omitted this defaults to false
* A millisecond timeout, the create request will wait this long the `"seqno"` to
  be persisted.
  * `"timeout_ms"`
  * value is a number
  * When omitted, no timeout is used and the command will fail immediately if
    the `"seqno"` is not persisted.
  * TBD: upper limit on this timeout

### Examples

A range-scan for all "user" prefixed keys. Here the start is "user" and the end
is "user\xFF" (i.e. the 5th byte is 255 which ensures *all* user prefixed keys
match).

```
{
  "collection": "f2",
  "range": {
    "end": "dXNlcv8=",
    "start": "dXNlcg=="
  }
}
```

The same range-scan, but require a vbucket uuid match and that a seqno has been
persisted.
```
{
  "collection": "f2",
  "range": {
    "end": "dXNlcv8=",
    "start": "dXNlcg=="
  },
  "snapshot_requirements": {
    "vb_uuid": "16627788222",
    "seqno": 1000
  }
}
```

Next key-only and require a vbucket uuid match and that a seqno has been
persisted with a 2 second wait.

```
{
  "collection": "f2",
  "key_only": true,
  "range": {
    "end": "dXNlcv8=",
    "start": "dXNlcg=="
  },
  "snapshot_requirements": {
    "vb_uuid": "16627788222",
    "seqno": 1000,
    "timeout_ms": 2000
  }
}

```

Random sample
```
{
  "collection": "f2",
  "sampling": {
    "seed": 18111,
    "samples": 512
  }
}
```

### Returns

On success the response will include a 128-bit (16-byte) identifier that the
client will use in continue and cancel requests. This will be encoded as
the value of the response packet.

The returned UUID does not define any sub-structure to the 16-bytes and the
entire 16-bytes are in network byte order. For example the following success
response shows UUID 00112233-4455-6677-8899-aabbccddeeff.

         CREATE_RANGE_SCAN success
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0xDA
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000
    Total body   (8-11) : 0x00000010
    Opaque       (12-15): 0x00001210
    CAS          (16-23): 0x0000000000000000
    scan-uuid    (24-39): 0x00112233445566778899aabbccddeeff

### Errors

**Status::KeyEnoent (0x01)**

The requested range is empty.

**Status::NotStored (0x05)**

When snapshot requirements are defined, this status indicates that the sequence
number which must exist, i.e. `"seqno_exists":true`, was not found.

**Status::NotMyVbucket (0x07)**

Additional to the usual cause of this status (e.g. vb is replica or not on this
node), a vb-uuid mismatch will result in this status.

**Status::Erange (0x22)**

When sampling, this error indicates the collection does not have enough keys
to satisfy the requested sample size.

**Status::Etmpfail (0x86)**

When snapshot requirements are defined, this status indicates that the vbucket
has yet to persist the required sequence number.

**Status::Einternal (0x84)**

The create can return this response for various runtime issues, logging should
be generated by such issues.