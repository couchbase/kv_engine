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
        "active": "fdcda290-fecd-4032-b8cd-7a23815fb934",
        "keys": [
          {
            "cipher": "AES-256-GCM",
            "id": "fdcda290-fecd-4032-b8cd-7a23815fb934",
            "key": "hDYX36zHrP0/eApT7Gf3g2sQ9L5gubHSDeLQxg4v4kM="
          }
        ]
      },
      "@config": {
        "active": "c7e26d06-88ed-43bc-9f66-87b60c037211",
        "keys": [
          {
            "cipher": "AES-256-GCM",
            "id": "c7e26d06-88ed-43bc-9f66-87b60c037211",
            "key": "ZdA1gPe3Z4RRfC+r4xjBBCKYtYJ9dNOOLxNEC0zjKVY="
          }
        ]
      },
      "@logs": {
        "active": "489cf03d-07f1-4e4c-be6f-01f227757937",
        "keys": [
          {
            "cipher": "AES-256-GCM",
            "id": "489cf03d-07f1-4e4c-be6f-01f227757937",
            "key": "cXOdH9oGE834Y2rWA+FSdXXi5CN3mLJ+Z+C0VpWbOdA="
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

Create bucket needs a new configuration parameter: `encryption=<json>`.
This allows for opening the database files.

The format for the configuration string passed to memcached looks like:

    key1=value;key2=value

To include the character `;` in a value it must be escaped like `\;`.

Is content would be:

      {
        "active": "489cf03d-07f1-4e4c-be6f-01f227757937",
        "keys": [
          {
            "cipher": "AES-256-GCM",
            "id": "489cf03d-07f1-4e4c-be6f-01f227757937",
            "key": "cXOdH9oGE834Y2rWA+FSdXXi5CN3mLJ+Z+C0VpWbOdA="
          }
        ]
      }

The field `"active"` indicates if encryption is enabled or not. If the bucket
used to be encrypted and should migrate over to an unencrypted data storage
the field would include all the keys it would need to open the existing
database files, but `"active"` would be absent (or empty).

## SetActiveEncryptionKeys

The key field of the packet contains the entity to set the keys for (name of the
bucket, `@logs`, `@config`, `@audit`). The value of the packet contains the list
of keys and the active key:

    {
      "keystore": {
        "active": "489cf03d-07f1-4e4c-be6f-01f227757937",
        "keys": [
          {
            "cipher": "AES-256-GCM",
            "id": "489cf03d-07f1-4e4c-be6f-01f227757937",
            "key": "cXOdH9oGE834Y2rWA+FSdXXi5CN3mLJ+Z+C0VpWbOdA="
          }
        ]
      },
      "unavailable": [
        "c7e26d06-88ed-43bc-9f66-87b60c037211",
        "fdcda290-fecd-4032-b8cd-7a23815fb934"
      ]
    }

The `unavailable` field is a list of keys should be present in the keystore,
but the caller had problems accessing them. This is used to indicate that
these keys should be copied from the old keystore to the new one. An
error (`EncryptionKeyNotAvailable`) is reported if the server is missing
any of the keys in the `unavailable` list.
