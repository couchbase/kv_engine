
# Protocol Specification

DCP utilizes the Memcached binary protocol as the basis for protocol definitions ([Memcached Binary Protocol Definitions](https://code.google.com/p/memcached/wiki/BinaryProtocolRevamped)) and defines a set opcodes for DCP commands.

A DCP connection differs from the standard Memcached connections because DCP connections are full duplex while the normal Memcached connections are simplex (eg. the client sends a command, the server responds, etc).

The typical scenario begins with the client requesting a stream. Upon success the server starts sending messages back to the client for mutations/deletions/expirations etc. While receiving messages the client can send additional commands to the server to start additional DCP streams, etc.

### Protocol Definitions

* [**Open Connection**](commands/open-connection.md)
* [**Add Stream**](commands/add-stream.md)
* [**Close Stream**](commands/close-stream.md)
* [**Stream Request**](commands/stream-request.md)
* [**Get Failover Log**](commands/failover-log.md)
* [**Stream End**](commands/stream-end.md)
* [**Snapshot Marker**](commands/snapshot-marker.md)
* [**Mutation**](commands/mutation.md)
* [**Deletion**](commands/deletion.md)
* [**Expiration**](commands/expiration.md)
* [**Flush**](commands/flush.md)
* [**Set VBucket State**](commands/set-vbucket-state.md)
* [**No-Op**](commands/no-op.md)
* [**Buffer Acknowledgement**](commands/buffer-ack.md)
* [**Control**](commands/control.md)

