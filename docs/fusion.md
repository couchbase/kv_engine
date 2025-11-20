# 0x10 - Fusion Stats

This command is used to retrieve Fusion-related statistics from the server.
It is similar to a regular ```stat``` command but specifically targets
Fusion stats by using a specially formatted key.

The request has:
* A bucket
* No extras
* A key - must begin with the prefix "fusion"
* No value

## Key Format

```
fusion <stat-group> [<vbid>]
```
* ```<stat-group>```: The category of Fusion stats you want.

* ```<vbid>``` (optional): The specific VBucket ID to query.

## Stat Groups

The stat-group can be one of the following:
* ```active_guest_volumes```: Returns a JSON array of active Fusion volumes
for a VBucket.
* ```uploader```: Returns a JSON object of uploader progress related stats.
* ```migration```: Returns a JSON object of migration progress related stats.

## Behaviour

If a ```<vbid>``` is provided → Returns stats for that specific VBucket.

If a ```<vbid>``` is not provided → Returns aggregated stats across all
VBuckets.

If aggregation is not supported for the stat group, the request will fail.

## Returns

If the request is successful, the server will respond with a JSON object
where the keys are the stat names and the values are the corresponding
stat value.

Example response for `fusion active_guest_volumes`:
```
{
  "volume1",
  "volume2",
  "volume3"
}
```

Example response for `fusion uploader`:
```
{
  "vb_0": {
    "uploader_state": "enabled",
    "sync_session_completed_bytes": 100,
    "sync_session_total_bytes": 1000,
    "sync_snapshot_pending_bytes": 100
    "term": 2,
  },
  "vb_1": ...
}
```

Example response for `fusion migration 0`:
```
{
  "completed_bytes": 100,
  "total_bytes": 1000,
}
```


# 0x70 - Get Fusion Storage Snapshot

Requests that the server creates a logical snapshot in the fusion backend.

The request has:
* No extras
* No key
* A value (JSON object encoding the arguments)
* datatype must be JSON and client must enable JSON when issuing HELO

The value is a JSON object with a number of fields, detailed below.

If the request is successful the server now holds a logical snapshot and
preserves the data files that make that snapshot.

## JSON definition

The following keys are accepted input. All keys are mandatory.
Any key not shown in the following sections will be ignored.

* The uuid to assign to the snapshot being created
  * `"snapshot_uuid"`
  * The value is a string

* The uuid to assign to the bucket being created
  * `"bucket_uuid"`
  * The value is a string

* The URI of the Metadatastore
  * `"metadatastore_uri"`
  * The value is a string

* The Auth token for accessing the Metadatastore
  * `"metadatastore_auth_token"`
  * The value is a string

* The temporal validity of the preserved snapshot
  * `"valid_till"`
  * The value is an unsigned integer
  * Timestamp, in seconds

* The list of vbuckets to get their fusion storage snapshots for
  * `"vbucket_list"`
  * The value is a Json array of unsigned integers

### Examples

```
{
    "snapshot_uuid": "some-snapshot-uuid",
    "bucket_uid": "some-bucket-uuid",
    "metadatastore_uri": "uri2",
    "metadatastore_auth_token": "some-token",
    "valid_till": 123456879,
    "vbucket_list": [12,23,25,66]
}
```

### Returns

On success the response's payload returns a JSON object that contains the
following keys:

* Snapshot creation time
  * `"createdAt"`
  * Timestamp, in milliseconds

* Log manifest name
  * `"logManifestName"`
  * String

* The uuid assigned to the created snapshot
  * `"snapshot_uuid"`
  * String

* The validity of the created snapshot
  * `"valid_till"`
  * Timestamp, in milliseconds

* The fusion version
  * `"version"`
  * Unsigned integer

* The volume id
  * `"volumeID"`
  * String

### Errors

**Status::Einval (0x04)**

Input validation failure (e.g. incorrect key length). The returned error context
will contain details.

**Status::NotMyVbucket (0x07)**

Server does not own an active copy of the vbucket.

**Status::Einternal (0x84)**

This status code is used for unexpected internal failure.


# 0x71 - Release Fusion Storage Snapshot

Requests that the server releases a logical snapshot in the fusion backend.

The request has:
* No extras
* No key
* A value (JSON object encoding the arguments)
* datatype must be JSON and client must enable JSON when issuing HELO

The value is a JSON object, detailed below.

If the request is successful the server now holds a logical snapshot and
preserves the data files that make that snapshot.

## JSON definition

The following keys are accepted input. All keys are mandatory.
Any key not shown in the following sections will be ignored.

* The uuid to assign to the snapshot being created
  * `"snapshot_uuid"`
  * The value is a string

* The uuid to assign to the bucket being created
  * `"bucket_uuid"`
  * The value is a string

* The URI of the Metadatastore
  * `"metadatastore_uri"`
  * The value is a string

* The Auth token for accessing the Metadatastore
  * `"metadatastore_auth_token"`
  * The value is a string

* The list of vbuckets to release their fusion storage snapshots for.
  * `"vbucket_list"`
  * The value is a Json array of unsigned integers

### Examples

```
{
    "snapshot_uuid": "some-snapshot-uuid",
    "bucket_uid": "some-bucket-uuid",
    "metadatastore_uri": "uri2",
    "metadatastore_auth_token": "some-token",
    "vbucket_list": [12,23,25,66]
}
```

### Returns

The call returns Status::Success, an error code otherwise.

### Errors

**Status::Einval (0x04)**

Input validation failure (e.g. incorrect arg format). The returned error context
will contain details.

**Status::NotMyVbucket (0x07)**

Server does not own an active copy of the vbucket.

**Status::Einternal (0x84)**

This status code is used for unexpected internal failure.


# 0x72 - Mount Fusion Vbucket

Requests that the server mounts a kvstore for the given vbucket.

The request has:
* Vbucket
* No extras
* No key
* A value (JSON object encoding the arguments)
* datatype must be JSON and client must enable JSON when issuing HELO

The value is a JSON object, detailed below.

If the request is successful the server now holds a logical snapshot and
preserves the data files that make that snapshot.

## JSON definition

The following keys are accepted input. All keys are mandatory.
Any key not shown in the following sections will be ignored.

* The uuid to of the snapshot being released
  * `"mountPaths"`
  * The value is an array of strings

### Examples

```
{
  "mountPaths": ["path1", "path2"]
}
```

### Returns

The call returns Status::Success, an error code otherwise.

### Errors

**Status::Einval (0x04)**

Input validation failure (e.g. incorrect arg format). The returned error context
will contain details.

**Status::Einternal (0x84)**

This status code is used for unexpected internal failure.


# 0x73 - Unmount Fusion Vbucket

TODO


# 0x74 - Sync Fusion Logstore

Requests that the server forces a flush to disk of the magma write cache for the
given  vbucket and syncs the data to fusion.

The request has:
* Vbucket
* No extras
* No key
* No value
* datatype must be RAW

### Returns

The call returns Status::Success, an error code otherwise.

### Errors

**Status::Einval (0x04)**

Input validation failure (e.g. incorrect arg format). The returned error context
will contain details.

**Status::Einternal (0x84)**

This status code is used for unexpected internal failure.


# 0x75 - Start Fusion Uploader

Requests that the server starts the Fusion Uploader for the given vbucket.
That uploads data to FusionLogStore for the latest revision of the given kvstore.
Unreferenced log files are also deleted as part of the upload process, which
requires a valid FusionMetadataStore auth token set via
SetFusionMetadataStoreAuthToken.
The given term must be monotonic. It must be incremented every time the fusion
uploader role is reassigned. The term is used to ensure zombie uploaders are
fenced and are disallowed from deleting log files from FusionLogStore.

The request has:
* Vbucket
* No extras
* No key
* A value (JSON object encoding the arguments)
* datatype must be JSON and client must enable JSON when issuing HELO

The value is a JSON object, detailed below.

## JSON definition

The following keys are accepted input. All keys are mandatory.
Any key not shown in the following sections will be ignored.

* The uuid to of the snapshot being released
  * `"term"`
  * The value is a string representation of unsigned int (64bit)

### Examples

```
{
  "term": "1234"
}
```

### Returns

The call returns Status::Success, an error code otherwise.

### Errors

**Status::Einval (0x04)**

Input validation failure (e.g. incorrect arg format). The returned error context
will contain details.

**Status::Einternal (0x84)**

This status code is used for unexpected internal failure.


# 0x76 - Stop Fusion Uploader

Requests that the server stops the Fusion Uploader for the given vbucket.

The request has:
* Vbucket
* No extras
* No key
* No value
* datatype must be RAW

### Returns

The call returns Status::Success, an error code otherwise.

### Errors

**Status::Einval (0x04)**

Input validation failure (e.g. incorrect arg format). The returned error context
will contain details.

**Status::Einternal (0x84)**

This status code is used for unexpected internal failure.


# 0x77 - Delete Fusion Namespace

The command deletes data from the given FusionLogStore and FusionMetadataStore
for all Fusion volumes under the given namespace.

The request has:
* No extras
* No key
* A value (JSON object encoding the arguments)
* datatype must be JSON and client must enable JSON when issuing HELO

The value is a JSON object with a number of fields, detailed below.

## JSON definition

The following keys are accepted input. All keys are mandatory.
Any key not shown in the following sections will be ignored.

* The URI of the Logstore
  * `"logstore_uri"`
  * The value is a string

* The URI of the Metadatastore
  * `"metadatastore_uri"`
  * The value is a string

* The Auth token for accessing the Metadatastore
  * `"metadatastore_auth_token"`
  * The value is a string

* The Fusion namespace under which data for all volumes is to be deleted
  * `"namespace"`
  * The value is a string

### Examples

```
{
  "logstore_uri": "uri1",
  "metadatastore_uri": "uri2"
  "metadatastore_auth_token": "some-token"
  "namespace": "namespace-to-delete"
}
```

### Returns

The call returns Status::Success, an error code otherwise.

### Errors

**Status::Einval (0x04)**

Input validation failure (e.g. incorrect params format). The returned error context
will contain details.

**Status::Einternal (0x84)**

This status code is used for unexpected internal failure.


# 0x78 - Get Fusion Namespaces

This command retrieves a list of buckets with their unique identifiers
for buckets that still have data on FusionLogStore.

The request has:
* No extras
* No key
* A value (JSON object encoding the arguments)
* datatype must be JSON and client must enable JSON when issuing HELO

The value is a JSON object with a number of fields, detailed below.

## JSON definition

The following keys are accepted input. All keys are mandatory.
Any key not shown in the following sections will be ignored.

* The URI of the Metadatastore
  * `"metadatastore_uri"`
  * The value is a string

* The Auth token for accessing the Metadatastore
  * `"metadatastore_auth_token"`
  * The value is a string

### Examples

```
{
  "metadatastore_uri": "uri"
  "metadatastore_auth_token": "some-token"
}
```

### Returns

On success the response's payload returns a JSON object that contains the
following keys:

* Version
  * `"version"`
  * The value is an unsigned integer

* Namespaces
  * `"namespaces"`
  * The value is a list of buckets, with their unique identifiers, that still have data on FusionLogStore

### Examples

```
{
  "version": 1,
  "namespaces": [
         "kv/travel-sample/uuid1",
         "kv/bucket-sample/uuid2",
      ]
}
```

### Errors

**Status::Einval (0x04)**

Input validation failure (e.g. incorrect params format). The returned error context
will contain details.

**Status::Einternal (0x84)**

This status code is used for unexpected internal failure.

# 0x3d - Set VBucket State

To request loading a vbucket from a snapshot, the command is extended from the
format in [BinaryProtocol.md](BinaryProtocol.md), which is used for non-snapshot
state changes.

The command value (JSON datatype) is extended to include a
`"use_snapshot": "fusion"` field.

Request:

* MUST have vbucket
* MUST have extra
* MUST NOT have key
* MUST have value

The command contains an extra section of one byte containing the vbucket state:

1. Active
2. Replica

Response:

* MUST NOT have extras
* MUST NOT have key
* MAY have value (error message)

## Errors

**Status::KeyEexists (0x02)**

The node already has this vbucket.

**Status::Einval (0x04)**

Input validation failure.

**Status::Einternal (0x84)**

Unexpected internal failure.
