
# Protocol Specification

DCP utilizes the Memcached binary protocol as the basis for protocol definitions ([Memcached Binary Protocol Definitions](https://code.google.com/p/memcached/wiki/BinaryProtocolRevamped)) and defines a set opcodes for DCP commands.

A DCP connection differs from the standard Memcached connections because DCP connections are full duplex while the normal Memcached connections are simplex (eg. the client sends a command, the server responds, etc).

The typical scenario begins with the client requesting a stream. Upon success the server starts sending messages back to the client for mutations/deletions/expirations etc. While receiving messages the client can send additional commands to the server to start additional DCP streams, etc.

### Protocol Definitions

* [**Abort**](commands/abort.md)
* [**Add Stream**](commands/add-stream.md)
* [**Buffer Acknowledgement**](commands/buffer-ack.md)
* [**Cache Transfer**](commands/cache-transfer.md) (see also the
  [Cache Transfer overview](cache_transfer.md))
* [**Cache Transfer End**](commands/cache-transfer-end.md)
* [**Close Stream**](commands/close-stream.md)
* [**Commit**](commands/commit.md)
* [**Control**](commands/control.md)
* [**Deletion**](commands/deletion.md)
* [**Expiration**](commands/expiration.md)
* [**Failover Log Request**](commands/failover-log.md)
* [**Flush**](commands/flush.md)
* [**Get All vBucket Sequence Numbers**](commands/get_seqno.md)
* [**Mutation**](commands/mutation.md)
* [**No-Op**](commands/no-op.md)
* [**Open Connection**](commands/open-connection.md)
* [**OSO Snapshot**](commands/oso_snapshot.md)
* [**Persist Sequence Number**](commands/persist_seqno.md)
* [**Prepare**](commands/prepare.md)
* [**Random Key**](commands/random_key.md)
* [**Seqno Acknowledged**](commands/seqno-acknowledged.md)
* [**Seqno Advanced**](commands/seqno-advanced.md)
* [**Set VBucket State**](commands/set-vbucket-state.md)
* [**Snapshot Marker**](commands/snapshot-marker.md)
* [**Stats VBucket-Seqno**](commands/stats-vbucket-seqno.md)
* [**Stream End**](commands/stream-end.md)
* [**Stream Request**](commands/stream-request.md)
* [**System Event**](commands/system_event.md)
* [**VBucket Takeover**](commands/stats-vbucket-takeover.md)