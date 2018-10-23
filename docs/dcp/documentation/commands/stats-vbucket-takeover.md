## VBucket Takeover

The `dcp-vbtakeover` statistic group is used by ns_server to monitor the progress of a DCP takeover stream.
The statistic has two variants - *Per vBucket* and *Per Stream*.

### Per vBucket

If the statistic is requested with just a vBucket ID (`dcp-vbtakeover <vbid>`, then it returns statistics for the given vBucket.

| Key | Value | Description |
|-----|-------|-------------|
| status  | "does_not_exist" | Always returns "does_not_exist" when no Stream name specified. |
| on_disk_deletes | <size_t> | Number of deleted documents that are persisted to disk. |
| vb_items | <size_t> | Number of alive (non-deleted) documents in the vBucket. |
| chk_items | <size_t> | Number of documents in the current open checkpoint. |
| estimate | <size_t> | An estimate of the number of documents which would need to be sent to replicate this vBucket. |

Example request/response:

```
dcp-vbtakeover 0

chk_items:       1
estimate:        9
on_disk_deletes: 0
status:          does_not_exist
vb_items:        9
```

## Per-Stream

If the statistic is requested with a valid DCP stream name (`dcp-vbtakeover <vbid> <stream>`), then it returns statistics
specific to that stream.

In *addition* to the fields listed in [Per vBucket](#per-vbucket) statistics, the following fields are included:

| Key | Value | Description |
|-----|-------|-------------|
| name | <string> | Stream name. |
| status  | <string> | Stream status. Possible values are: *backfilling* - Stream is reading historic data from disk. *in-memory* - Stream is reading in-memory data from the Checkpoint Manager. *does_not_exist* - Stream is no longer active. |
| backfillRemaining | <size_t> | Number of documents still to be backfilled from disk for this stream. |

Example request/response:

```
dcp-vbtakeover 0 replication:n_0@192.168.4.8->n_1@127.0.0.1:beer-sample

 backfillRemaining: 0
 chk_items:         0
 estimate:          0
 name:              eq_dcpq:replication:n_0@192.168.4.8->n_1@127.0.0.1:beer-sample
 on_disk_deletes:   0
 status:            in-memory
 vb_items:          9
```
