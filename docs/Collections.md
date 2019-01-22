# Couchbase Binary Protocol - collections commands

## All memcached protocol commands that reference a document

When collections are enabled (by use of HELLO) every command that can reference
a document is assumed to also encode the collection-ID of the document.

A connection with collections enabled will assume that the key-bytes encodes the
collection-ID beginning at offset 0 using an unsigned LEB128 format.

* https://en.wikipedia.org/wiki/LEB128

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
of the returned value is a JSON document with the following format. The context
field's value is the UID of the manifest in the same format as the JSON manifest
(a hex number in a string with no leading 0x).

```
{
  "error": {
    "context": "1f"
  }
}
```

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
* `max_ttl`: Optional - An integer value defining the max_ttl to apply to the
 collection.

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
          "max_ttl": 1
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
* MUST have a key.
* MUST NOT have a value.

Response (if status is success):

* Must have extras
* No key
* No value.

Get collection ID takes the key of command and the last set collections manifest
to return the uid of the collection.

The key is defined as a 'path' to the collection, given as "scope.collection"

The command supports _default scope/collection by omission.

Example key:

* `_default.c1` and `.c1` are equivalent and will lookup the `c1` collection in the `_default` scope.
* `_default._default` and `.` are equivalent and will lookup the `_default` collection in the `_default` scope.
* `App1.c1` will lookup the ID of `c1` collection in the `App1` scope.

### Errors

#### No collections manifest

Get collection ID was invoked without a prior set collections

#### Unknown Collection

If the path is correctly formed and consists of valid collection and scope names
but no collection could be found, this error is returned.

#### Invalid Argument

Invalid argument can be returned for issues with the header or if the path is invalid.

Header errors:
* extra len, cas, vbucket, datatype are not 0

Path errors:
* The path does not contain one `.`
* The scope is not a valid scope name (see set collections errors for validation of the name format)
* The collection is not a valid collection name (see set collections errors for validation of the name format)
