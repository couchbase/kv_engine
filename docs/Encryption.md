# Encryption at rest

## Key format

Each key is specified as a JSON entry with the following syntax:

    {
       "id": "the key identifier",
       "cipher": "AES-256-GCM",
       "key": "base64 encoded key"
    }

All keys for a given entity is sepecified:

    {
        "keys": [
           {key}
        ],
        "active": "key identifier of the active key"
    }

Example JSON for bootstrapping memcached:

    {
      "@audit": {
        "active": "audit:1",
        "keys": [
          {
            "cipher": "AES-256-GCM",
            "id": "audit:1",
            "key": "srxK+AAGLy9xk4SUQNZ4tKLMJxM2o5fQRT/JoF7NM6E="
          },
        ]
      },
      "@config": {
        "active": "config:5",
        "keys": [
          {
            "cipher": "AES-256-GCM",
            "id": "config:4",
            "key": "/1v4qEtagYdVTZAN49PGtGWILx66Bpwxb00m5/pfPl4="
          },
          {
            "cipher": "AES-256-GCM",
            "id": "config:5",
            "key": "cBWOote1S4mrGW6/5yYQGzCyL1Idp9nFn6ofLA/trXM="
          }
        ]
      },
      "@logs": {
        "active": "logs:2",
        "keys": [
          {
            "cipher": "AES-256-GCM",
            "id": "logs:2",
            "key": "fBSKnC3PrOjS2par7hd6yH7/0GA0GTQJ4Oj67XokNB8="
          }
        ]
      }
    }

## Bootstrap keys

The bootstrap keys are passed to memcached via standard input. To avoid
any kind of race conditions a new command line option (`--stdin`) is
required which cause startup of memcached to do blocking read of standard
input until the keys is successfully retrieved.

To allow for later reuse of this mechanism (passing information via
stdin) the following format should be used:

    KEY=value\n
    DONE\n

The bootstrap keys should be passed as

    BOOTSTRAP_DEK=<json>\n

## Create bucket

Create bucket needs a new configuration parameter: `dek=<json>`. This allows
for opening the database files.

## SetActiveEncryptionKey

The key field of the packet contains the entity to set the keys for (name of the
bucket, `@logs`, `@config` etc). The value field contains the new key to add (and make
it active).

# Encrypted file format

Each file is built up starting with a file header followed by multiple chunks.

## File header

The file header consists of two parts. A fixed size part and a variable
size part. If the key id's is plain numbers we can make the file header
fixed size containing the id in network byte order.

### Fixed size

    | offset | length | description     |
    +--------+--------+-----------------+
    | 0      | 5      | magic: \0CEF\0  |
    | 5      | 1      | version         |
    | 6      | 1      | id len          |

    Total of 7 bytes

### Variable size

This is the number of bytes used for the key id

### Example

In the current implementation only supporting AES-256-GCM the header for the key
`self:1` should look like:

    Offset 0 | 00 43 45 46 00    | Magic: \0CEF\0
    Offset 5 | 00                | Version: 0
    Offset 6 | 06                | Id length 6
    Offset 7 | 73 65 6c 66 3a 31 | self:1

    Total 13 bytes

## Chunk

Each chunk contains a fixed two byte header containing the
size (in network byte order) of the data which is encoded
as:

    nonce ++ ciphertext ++ tag

In version 0 the size of the nonce is 12 bytes and the tag is 16
bytes.
