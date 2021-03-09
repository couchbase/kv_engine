# Couchbase Binary Protocol - collections commands

## All memcached protocol commands that reference a document - encoding collection ID with LEB128

When collections are enabled (by use of HELLO) every command that can reference
a document is assumed to also encode the collection-ID of the document.

Memcached will expect that every document command (e.g. SET/GET) sent on a collections
enabled connection encodes the collection-ID beginning at offset 0 using an
unsigned LEB128 format.

* See https://en.wikipedia.org/wiki/LEB128

### Valid LEB128 encodings

KV will only consider up-to the maximum size of bytes for the LEB128 collection-ID.
* LEB128 collection-IDs are a maximum of 5 bytes
* KV expects to find a stop byte within those 5-bytes, otherwise it is invalid input and the command will fail.

For example encoding 0 as 6-bytes - 0x80.80.80.80.80.00 is invalid.

### Non-canonical encodings

LEB128 permits non-canonical encodings, e.g. 1 validly be represented as:
* 0x01
* 0x81.00
* 0x81.80.00
* 0x81.80.80.00
* 0x81.80.80.80.00

KV-engine will fail the use of non-canonical encodings, all encodings must
use the smallest, canonical form.

### LEB128 Encoded Examples

The following table shows collection-ID values and their leb128 encoding. The
encoding is shown with as `byte0, byte1, byte2`. Clients can use these values to
check their LEB128 encoding/decoding routines.

| Collection-ID | Encoded leb128 bytes |
|---------------|----------------------|
|`0x00`   |  `0x00`|
|`0x01`   |  `0x01`|
|`0x7F`   |  `0x7F`|
|`0x80`   |  `0x80, 0x01`|
|`0x555`   |  `0xD5, 0x0A`|
|`0x7FFF`   |  `0xFF, 0xFF, 0x01`|
|`0xBFFF`   |  `0xFF, 0xFF, 0x02`|
|`0xFFFF`   |  `0XFF, 0xFF, 0x03`|
|`0x8000`   |  `0x80, 0x80, 0x02`|
|`0x5555`   |  `0xD5, 0xAA, 0x01`|
|`0xCAFEF00`   |  `0x80, 0xDE, 0xBF, 0x65`|
|`0xCAFEF00D`   |  `0x8D, 0xE0, 0xFB, 0xD7, 0x0C`|
|`0xFFFFFFFF`   |  `0xFF, 0xFF, 0xFF, 0xFF, 0x0F`|

For example `0x5555` would be encoded into the protocol command field as:

```
  Byte/     0       |       1       |       2       |
     /              |               |               |
    |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
    +---------------+---------------+---------------+
   0| 0xD5          | 0xAA          | 0x01          |
    +---------------+---------------+---------------+
```

### Example command

For example ADD command with collections enabled, adding “Hello” to
collection-ID 555 with value “World” looks as follows. The leb128 CID is shown
in bytes 32,33 which are the 0 and 1 byte of the key.

```
  Byte/     0       |       1       |       2       |       3       |
     /              |               |               |               |
    |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
    +---------------+---------------+---------------+---------------+
   0| 0x80          | 0x02          | 0x00          | 0x07          |
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
  32| 0xab          | 0x04          | 0x48 ('H')    | 0x65 ('e')    |
    +---------------+---------------+---------------+---------------+
  36| 0x6c ('l')    | 0x6c ('l')    | 0x6f ('o')    | 0x57 ('W')    |
    +---------------+---------------+---------------+---------------+
  40| 0x6f ('o')    | 0x72 ('r')    | 0x6c ('l')    | 0x64 ('d')    |
    +---------------+---------------+---------------+---------------+

   Total 44 bytes (24 byte header, 8 byte extras, 7 byte key/cid and
                    5 byte value)


Field        (offset) (value)
Magic        (0)    : 0x80
Opcode       (1)    : 0x02
Key length   (2,3)  : 0x0007 # Includes the leb128 size (2 bytes)
Extra length (4)    : 0x08
Data type    (5)    : 0x00
VBucket      (6,7)  : 0x0000
Total body   (8-11) : 0x00000012
Opaque       (12-15): 0x00000000
CAS          (16-23): 0x0000000000000000
Extras              :
  Flags      (24-27): 0xdeadbeef
  Expiry     (28-31): 0x00000e10
Key (cid)    (32,33): 0xab,0x04
Key          (34-38): The textual string "Hello"
Value        (39-43): The textual string "World"
```


## Error Code 0x88, Unknown collection

The unknown collection error is used when a request is made, referencing a
collection and the node could not match the collection against its manifest.
The same error can be see on "opcode 0xbb Get Collection ID" when an unknown
"scope.name" is used.

The response packet for the unknown collection error includes the UID of the
manifest the server used in determining the collection doesn't exist. The format
of the returned value is a JSON document with the following format. The
manifest_uid field's value is the UID of the manifest in the same format as the
JSON manifest (a hex number in a string with no leading 0x).


```
{
  "manifest_uid":"1f"
}
```

Note that other fields within this JSON response value are allowed.

## 0xb9 - Set Collections Manifest

Request:

* MUST NOT have extras.
* MUST NOT have a key.
* MUST have a value.

Response (if status is success):

* No extras
* No key
* No value.

The set collections command informs the selected bucket of the collection
configuration. The collection configuration is the value of the command in a
JSON format.

The JSON manifest is required to contain the following two keys at the top-level.

* `uid`: A string containing the uid of the manifest, formatted as a base-16 value.
* `scopes`: An array of scope entries, containing at least the default scope.

A scope entry is defined to have the following keys:

* `name`: Required - A string containing the name of the scope, must be unique.
* `uid`: Required - A unique ID for the scope, formatted as a base-16 value.
* `collections`: Optional - An array of collection entries, the array can be empty.

A collection entry is define to have the following keys:

* `name`: Required - A string containing the name of the collection, must be
 unique within the scope.
* `uid`: Required - A unique ID for the collection (unique amongst every collection
 regardless of the scope), formatted as a base-16 value.
* `maxTTL`: Optional - An integer value defining the maximum time-to-live (in seconds)
 to apply to the new items added to the collection. The value has the same properties
 as the bucket TTL.

For example:
```
{
  "uid": "a2",
  "scopes": [
    {
      "name": "_default",
      "uid": "0",
      "collections": [
        {
          "name": "_default",
          "uid": "0"
        },
        {
          "name": "brewery",
          "uid": "1c",
          "maxTTL": 1
        }
      ]
    }
  ]
}
```

In the following sections we refer to user's collections and system collections.
At the moment, the only defining feature of a system collection is the naming
and for validation purposes set collections allows either collection to be created.

### Errors

#### Temporary Failure

Concurrent attempt to update the manifest, only one management connection should
be active so concurrent updates may fail.

#### Invalid Argument

Invalid argument can be returned for many issues with the header or value of the
command.

Header errors:
* extra len, cas, vbucket, datatype are not 0

Value errors:
* Invalid JSON
* Missing a required key
* A key has a value of the wrong type
* A name is not valid
  * Size: 1 byte minimum, 30 byte maximum.
  * A user's collection can only contain characters `A-Z`, `a-z`, `0-9` and the following symbols `_ - %`
  * The prefix character of a user’s collection name however is restricted. It cannot be `_` or `%`
  * A system collection name can contain characters `A-Z, a-z, 0-9` and the following symbols `_  $ - %`
  * A system collection must be `_` prefixed.
  * `$` prefixed collections shall be reserved for future use (and if used will trigger this error)
* A scope or collection ID is invalid, values 0 to 7 are reserved for internal use
  * A value of 0 defines the _default collection/scope
* Collection/Scope IDs are not unique
* Scope names are not unique
* Scope array exceeds the bucket's configuration limit
* The total number of defined collections exceeds the bucket's configuration limit
* Collection names in a scope are not unique
* Default scope is missing

#### Out of range

* The manifest uid is less than uid of the last set manifest

## 0xba - Get Collections Manifest

Request:

* MUST NOT have extras.
* MUST NOT have a key.
* MUST NOT have a value.

Response (if status is success):

* No extras
* No key
* MUST have a value.

Get collections returns the value of the last successful set collection.

### Errors

#### No collections manifest

Get collections was invoked without a prior set collections.

#### Invalid Argument

Invalid argument can be returned for issues with the header.

Header errors:
* extra len, cas, vbucket, datatype are not 0

## 0xbb - Get Collections ID

Request:

* MUST NOT have extras.
* MUST NOT have a key.
* MUST have a value.

Response (if status is success):

* Must have extras
* No key
* No value.

Get collection ID interprets the value as a path to a collection and using the
current manifest attempts to return the collection's unique ID.

* A 'path' to a collection is defined as `scope.collection`
* A 'path' supports _default scope and _default collection by omission as shown
in the example inputs.

Example inputs:

* `_default.c1` and `.c1` are equivalent paths, they both will lookup the `c1`
collection in the `_default` scope.
* `_default._default` and `.` are equivalent paths and they and will lookup the
`_default` collection in the `_default` scope.
* `App1.c1` will lookup the unique ID of the `c1` collection in the `App1`
scope.

### Extras

Successful response includes an extras payload with two values.

* Offset 0: u64 manifest ID
* Offset 8: u32 collection ID

The returned manifest ID is the ID of the manifest that the lookup was performed
against.

### Errors

#### Unknown Scope

If the path is correctly formed and consists of valid collection and scope names
but the scope couldn't be found, this error is returned.

#### Unknown Collection

If the path is correctly formed and consists of valid collection and scope names
but no collection could be found, this error is returned.

#### Invalid Argument

Invalid argument can be returned for issues with the header or if the path is
invalid.

Header errors:
* key len, extra len, cas, vbucket, datatype are not 0

Path errors:
* The path does not contain one `.`
* The scope is not a valid scope name (see "0xb9 - Set Collections Manifest"
errors for validation of the name format)
* The collection is not a valid collection name (see "0xb9 - Set Collections
Manifest" errors for validation of the name format)

## 0xbc - Get Scope ID

Request:

* MUST NOT have extras.
* MUST NOT have a key.
* MUST have a value.

Response (if status is success):

* Must have extras
* No key
* No value.

Get scope ID interprets the value as a path to a scope and using the current
manifest attempts to return the scope's unique ID.

* A 'path' to a scope is defined as `scope`
* A 'path' supports `_default` scope by omission as shown in the example inputs.

Example inputs:

* `_default` and `` are equivalent, both will lookup the unique ID of the
`_default` scope.
* `App1` will lookup the unique ID of the `App1` scope.

Note: Get Scope ID can accept a complete path `scope.collection` it ignores the
the `.collection` part and only examines the scope.

### Extras

Successful response includes an extras payload with two values.

* Offset 0: u64 manifest ID
* Offset 8: u32 scope ID

The returned manifest ID is the ID of the manifest that the lookup was performed
against.

### Errors

#### Unknown Scope

If the path is correctly formed and consists of valid scope names but no
matching scope could be found, this error is returned.

#### Invalid Argument

Invalid argument can be returned for issues with the header or if the path is
invalid.

Header errors:
* key len, extra len, cas, vbucket, datatype are not 0

Path errors:
* The path contains more than one `.`
* The scope is not a valid scope name (see "0xb9 - Set Collections Manifest"
errors for validation of the name format)
