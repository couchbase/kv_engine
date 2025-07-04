# dcpdrain - Test program for draining DCP streams

dcpdrain is a test program that connects to a Couchbase cluster and
drains DCP streams, and its use case is to test/verify/measure the
functionality of DCP streams in the kv_engine.

## Usage

The minimal number of arguments to dcpdrain is:

```
$ dcpdrain --user username --password pass --bucket bucket
```

The program will connect to the Couchbase cluster running on the same
computer, fetch the cluster topology map for the named bucket and
connect to all nodes in the cluster to drain the DCP streams for the
bucket.

By default, dcpdrain reports its output as plain text like:

```
Execution took 1287 ms - 63288 mutations with a total of 44413085 bytes received in 128 snapshots and 0 oso snapshots (overhead 1719845 bytes, 42MB/s)
```

## Options

dcpdrain supports a wide number of command line options, which can be
listed by running: `dcpdrain --help`. Some of the options are common
to all Couchbase command line tools, such as `--user`, `--password`,
`--host` and `--tls` and won't be described here. Others are specific
to `dcpdrain` and are described below.

### --bucket

The name of the bucket to drain DCP streams for. This is a required
option.

### --num-connections <number>

By default dcpdrain use one connection to each server and streams all
active vbuckets from that host over the single connection. By using
`--num-connections` one may specify the number of connections to use
to each server (and the vbuckets will be distributed across the
connection).

### --buffer-size <size>

By default, dcpdrain uses a DCP buffer size of 13421772 bytes (13
MB). This can be changed by using the `--buffer-size` option, to
specify a different buffer size. The value can be specified as a
number of bytes, or with a suffix of `k` or `m` to specify kilo or
megabytes respectively. If set to 0, DCP flow control will be
disabled.

### --acknowledge-ratio <ratio>

This option specifies the percentage of the DCP buffer size that
should be filled before an acknowledgment is sent back to the
server. The default value is 0.5 (50%). This can be specified as a
float value, e.g. `0.5` for 50%.

### --control <key=value>

This option allows you to add a control message to the DCP stream. The
key and value should be specified in the format `key=value`. This can
be used to send custom control messages to the DCP stream, which can
be useful for testing.

By default dcpdrain sends the following control messages:

    set_priority=high
    supports_cursor_dropping_vulcan=true
    supports_hifi_MFU=true
    send_stream_end_on_client_close_stream=true
    enable_expiry_opcode=true
    set_noop_interval=60
    enable_noop=true

### --json

If specified, the output will be formatted as JSON. This is useful for
programmatic consumption of the output. If you try to use this and
`--verbose` at the same time, the progress logging will be printed as
its own JSON objects, and finally the summary will be printed as a
single JSON object.

### --name <name>

By default, dcpdrain use `dcpdrain:<pid>:<connection number>` as the
name for the DCP connection. You may replace the prefix
`dcpdrain:<pid>` with a custom name by using the `--name` option.

### --verbose

If specified, dcpdrain will produce more verbose output, including
progress updates and detailed information about the DCP stream
draining process. This is useful for debugging and understanding the
internal workings of dcpdrain.

### --enable-oso[=with-seqno-advanced]

Enable 'Out-of-Sequence Order' backfills. If the optional value
`with-seqno-advanced` is specified, also enable support for sending a
`SeqnoAdvanced` message when an out of order snapshot is used and the
transmitted item with the greatest seqno is not the greatest seqno of
the disk snapshot.

### --disable-collections

Disable Hello::Collections negotiation (for use with pre-7.0
versions). When this option is specified, dcpdrain will only
stream items from the default collection.

### --stream-request-value <filename>

Path to a file containing stream-request value. This must be a file
storing a JSON object matching the stream-request value
specification. The file should contain a JSON object with the
following structure:

```json
{
  "collections": [
    "a",
    "1e"
  ],
  "purge_seqno": "1000",
  "uid": "b4",
  "sid": 71,
  "scope": "9"
}
```

### --stream-id <filename>

Path to a file containing stream-ID config. This must be a file
storing a JSON object that stores a single array of JSON objects, the
JSON objects are stream-request values (with stream-ID
configured). Use of this parameter enables DCP stream-id. The file
should contain a JSON object with the following structure:

```json
{
  "streams": [
    {
      "collections": [
        "0"
      ],
      "sid": 2
    },
    {
      "collections": [
        "8"
      ],
      "sid": 5
    }
  ]
}
```
### --stream-request-flags <number>

Value to use for the 4-byte stream-request flags field. This is an
integer value that can be specified as a decimal number.  The default
value is `DCP_ADD_STREAM_FLAG_TO_LATEST`, which is defined as
`0x00000004`.

### --vbuckets <list>

This option allows you to specify a subset of vbuckets to stream from.
The vbuckets should be specified as a comma-separated list of
integers.  For example, `--vbuckets 1,2,3` will only stream from
vBuckets 1, 2, and 3.

### --enable-flatbuffer-sysevents

Enable system events with flatbuffer values. This allows the DCP
stream to include system events that use flatbuffer values, which can
be useful for testing and debugging purposes.

### --enable-change-streams

Enable change streams support. This allows the DCP stream to include
change streams, which can be useful for tracking changes to documents
in the bucket.

### --hang

Create streams, but do not drain them. This is useful for testing the
DCP stream creation process without actually draining the streams.

### --start-inside-snapshot

Start the stream inside of a snapshot. The client will query
vbucket-details and begin at 1/2 of each vbucket's high-seqno. This is
useful for testing scenarios where the stream needs to start inside an
existing snapshot.



