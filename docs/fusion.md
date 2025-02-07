# 0x70 - Get Fusion Storage Snapshot

Requests that the server creates a logical snapshot in the fusion backend.

The request has:
* Vbucket
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
  * `"snapshotUuid"`
  * The value is a string

* The temporal validity of the preserved snapshot
  * `"validity"`
  * The value is an unsigned integer
  * Timestamp, in seconds

### Examples

```
{
  "snapshotUuid": "some-snapshot-uuid",
  "validity": 123456879
}
```

### Returns

On success the response's payload returns a JSON object that contains the
following keys:

* Snapshot creation time
  * `"createdAt"`
  * Timestamp, in milliseconds

* Log files related to the snapshot
  * `"logFiles"`
  * Timestamp, in milliseconds

* Log manifest name
  * `"logManifestName"`
  * String

* The uuid assigned to the created snapshot
  * `"snapshotUUID"`
  * String

* The validity of the created snapshot
  * `"validTill"`
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
  * `"snapshotUuid"`
  * The value is a string

### Examples

```
{
  "snapshotUuid": "some-snapshot-uuid"
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