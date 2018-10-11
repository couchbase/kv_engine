# Stream Request (opcode 0x53) Value Specification

Sent by the consumer side to the producer specifying that the consumer wants to
create a vbucket stream. The command can optionally accept a value, which encodes
a JSON object to configure the stream.

The following keys can be included in the JSON object.

* `uid` - for resuming collection aware DCP streams, manifest-UID
* `sid` - for specifying an ID the client would like to associated with the stream
* `collections` - for specifying the set of collection IDs the stream must include
* `scope` - for specifying the scope-ID the stream must include

## Key Definition

### uid

`uid` should be set by the client when they are resuming a stream, the value must
be the `uid` they last observed from a collection's DCP System event.

For example if the client observed a create-collection event with uid `0xb4` at
seqno 2091, then any subsequent stream-request for a stream starting at seqno 2091 or
higher must include a JSON object that encodes at least the manifest `uid`.

The value of the "uid" parameter matches the JSON manifest data used to manage
each key-value node's collection configuration:

* A JSON string containing the manifest-UID as a string representation of the
 value in base16 with no leading 0x.

For example:

```
{
    "uid" : "b4"
}
```

### sid

The `sid` must be set for any stream-requested from a DCP producer which has the
DCP stream-ID feature enabled. The `sid` is an unsigned 16-bit integer
(1 to 65536) chosen by the client. All DCP mutations, deletions, expirations,
snapshot markers and end stream messages will be transmitted from server to
client with the chosen value encoded in the message (using flexible framing.)

A client can create many DCP streams on the same vbucket using different values
of sid.

```
{
    "sid" : 71
}
```

### collections

The `collections` key can be specified to request the stream only includes items
relating to the specified collection or collections.

The key specifies an array of collection-IDs which folled the JSON collections
manifest format, JSON strings representing the ID as a base-16 string without a 0x
prefix.

```
{
    "collections" : ["0", "8a"]
}
```

### scope

The `scope` key can be specified to request the stream only includes items
relating to the collections of the specified scope.

The value is a JSON string representing the ID as a base-16 string without a 0x
prefix.

```
{
    "scope" : "9"
}
```

## Validation

* The stream-request code does not error for unknown keys.
* The stream-request will fail with `Invalid` if `collections` and `scope` are
 defined.

### uid
* The stream-request will fail if `uid` is not a JSON string

### sid
* The stream-request will fail if `sid` is included and the client has not
[enabled multiple streams](control.md).
* The stream-request will fail if `sid` is not a JSON integer
* The stream-request will fail if `sid` is 0
* The stream-request will fail if `sid` is in use by a stream
* No more streams can be created, there will be a limit on how many streams can
be created per vbucket.

### collections
* The stream-request will fail if `collections` is not a JSON array

### scope
* The stream-request will fail if `scope` is not a JSON string

## Example

Request a DCP stream to include collections with id `0xa` and `0x1e` and resuming
when the last observed system event encoded a manifest-UID of `0xc2`

```
{
    "uid" : "c2",
    "collections" : ["a", "1e"]
}
```
