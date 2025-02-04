# PrepareSnapshot

The prepare command is used to prepare the server for a snapshot for the
requested vbucket. The server will respond with a snapshot manifest that can
be used to download the snapshot from the server.

If the server owns vbucket 0 and the snapshot was successfully created,
the server replies with a status code success, datatype JSON and the [snapshot
manifest](#snapshot-manifest) in the payload.

# ReleaseSnapshot

Release the snapshot for the requested vbucket (or the snapshot identified
with the provided UUID (set in the key field)). The server will respond with
a status code of success if the snapshot was found and deleted.

# DownloadSnapshot

Download the snapshot for the requested vbucket to this node. The value field
of the packet contains a JSON object with the following fields:

* `bucket` - The name of the bucket to download the snapshot from.
* `host` - The hostname or IP address of the node to download the snapshot from.
* `port` - The port number to connect to.
* `sasl` - The SASL credentials to use when connecting to the server. This
  object contains the following fields:
  * `mechanism` - The SASL mechanism to use (PLAIN).
  * `username` - The username to use.
  * `password` - The password to use.
* `tls` - The TLS configuration to use when connecting to the server. This
  object contains the following fields:
  * `ca_store` - The path to the CA certificate store.
  * `cert` - The path to the client certificate.
  * `key` - The path to the client private key.
  * `passphrase` - The passphrase to use when decrypting the private key.
    (This field is base64 encoded).
* `fsync_interval` - The interval (in bytes) to call fsync() when writing
                     the files to disk (default 50MB). Set to 0 to disable
                     fsync.

Example:

    {
      "bucket": "default",
      "host": "::1",
      "port": 58036,
      "fsync_interval": 52428800,
      "sasl": {
        "mechanism": "PLAIN",
        "password": "password",
        "username": "@admin"
      },
      "tls": {
        "ca_store": "/cert/root/ca_root.cert",
        "cert": "/cert/clients/internal.cert",
        "key": "/cert/clients/internal.key",
        "passphrase": ""
      }
    }

# GetFileFragment

The GetFileFragment command is used to request a fragment of a file from the
server. The value field of the packet contains a JSON object with the following
fields:

* `id` - The ID of the file to request a fragment from.
* `offset` - The offset in the file to start reading from (as string).
* `length` - The number of bytes to read (as string).

Example:

    {
      "id": 1,
      "offset": "0",
      "length": "1024"
    }

Upon success a response packet is sent containing the requested fragment.

# Snapshot manifest

The snapshot manifest is a JSON object that contains the following fields:

* `uuid` - The UUID of the snapshot.
* `vbid` - The vbucket ID of the snapshot.
* `deks` - An array of DEKs used to encrypt the files in the snapshot.
* `files` - An array of files in the snapshot. Each file is represented by a
  JSON object with the following fields:
  * `id` - The ID of the file.
  * `path` - The path to the file.
  * `sha512` - The SHA512 hash of the file.
  * `size` - The size of the file.
 
Example

    {
      "deks": [],
      "files": [
        {
           "id": 1,
           "path": "0.couch.1",
           "sha512": "620b098899be489f74d9bca16efe40671de9710c9243ac7d7d5c34c54429555a41ebbf794f3f41f24d42f9c5aa034d97db89a83f3c15179a17d84614029303df",
           "size": "16489"
        }
      ],
      "uuid": "f7970e20-ca1b-4059-8542-3196e93f6927",
      "vbid": 0
    }
