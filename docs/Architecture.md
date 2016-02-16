# KV Engine Architectural Overview

````
                                                        +------------+
                                                        | Babysitter |
                                                        +--+------+--+
                                                           |      |
                                                +----------v--+   |
                                                |  ns_server  |   |
                                                +------+------+   |
                                             11209 / stdin/stderr |
                                                       |          |
                       +-------------------------------v----------v-------------------------------+
                       |                 __ ___   __    ____          _                           |
                       |                / //_/ | / /___/ __/__  ___ _(_)__  ___                   |
                       |               / ,<  | |/ /___/ _// _ \/ _ `/ / _ \/ -_)                  |
                       |              /_/|_| |___/   /___/_//_/\_, /_/_//_/\__/                   |
                       |                                      /___/                               |
                       |   +---------------+           +----------------------------------------+ |
                       |   |   Memcached   |           |       Engine (Back-end)                | |
                       |   |  (Front-end)  |           |                                        | |
 +------------+        |   |   ────────    |           | +------------------------+             | |
 |Smart-client|--11210-+-->|  ╱        ╲   |           | | default.so (MC Bucket) |             | |
 +------------+        |   |   Dispatch    |   Engine  | |                        |             | |
                       |   |  ╲          <-+-Interface-+->  +##################+  |             | |                 +------------+
 +------------+        |   |   ───────▶    |           | |  #    Hash-table    |  |             | |                 |  Replicas  |
 |Smart-client|--11210-+-->|   ────────    |           | |  +------------------+  |             | |                 +------------+
 +------------+        |   |  ╱        ╲   |           | +------------------------+             | |                 +------------+
                       |   |    Worker     |           |                                        | |                 |   Views    |
                       |   |  ╲            |           |                                        | |  DCP Producer   +------------+
         +----+        |   |   ───────▶    |           | +------------------------+   ────────  | +-----11209-----> +------------+
--11211->|Moxi|--11210-+-->|   ────────    |           | | ep.so (CB Bucket)      |  ╱        ╲ | |                 |    XDCR    |
         +----+        |   |  ╱        ╲   |           | |  +------------------+  |    AuxIO    | |                 +------------+
                       |   |    Worker     |           | |  |    vbucket 0     |  |  ╲          | |                 +------------+
         +----+        |   |  ╲            |           | |  |                  |  |   ───────▶  | |                 |    GSI     |
--11211->|Moxi|--11210-+-->|   ───────▶    |           | |  |  +############+  |  |   ────────  | |                 +------------+
         +----+        |   |               |           | |  |  # Hash-table |  |  |  ╱        ╲ | |
                       |   |   ────────    |           | |  |  +------------+  |  |    NonIO    | |
                       |   |  ╱        ╲   |   Engine  | |  +------------------+  |  ╲          | |
+-------+ DCP Consumer |   |    stdin    <-+-Interface-+->                        |   ───────▶  | |
|KV-Node|----11209-----+-->|  ╲            |           | |  +------------------+  |   ────────  | |
+-------+              |   |   ───────▶    |           | |  |    vbucket 1     |  |  ╱        ╲ | |                 +------------+
                       |   |   ────────    |           | |  |                  |  |    Writer   | |-------TAP------>| TAP Client |
                       |   |  ╱        ╲   |           | |  |  +############+  |  |  ╲      xN  | |                 +------------+
                       |   |     TAP       |           | |  |  # Hash-table |  |  |   ───────▶  | |
                       |   |  ╲            |           | |  |  +------------+  |  |   ────────  | |
                       |   |   ───────▶    |           | |  +------------------+  |  ╱        ╲ | |
                       |   |   ────────    |           | +------------------------+    Reader   | |
                       |   |  ╱        ╲   |           |                             ╲      xN  | |
                       |   |   Logging     |           |                              ───────▶  | |
                       |   |  ╲            |           |                               ^        | |
                       |   |   ───────▶    |           |                               |        | |
                       |   +---------------+           +-------------------------------+--------+ |
                       +---------------------------------------------------------------+----------+
                                                                                       |
                                                                                    ╒══v═══╕
                                                                                    │ Disk │
                                                                                    ╘══════╛
````

Main components:

 * [couchbase/memcached](http://github.com/couchbase/memcached): Front-end to
 KV-Engine, responsible for handling connections and buckets. Also includes
 'default engine' used for Memcached buckets.
 * [couchbase/ep-engine](http://github.com/couchbase/ep-engine): Main bucket
 implementation used by end-users. Responsible for storing documents in memory,
 persisting documents to disk and streaming changes to other Couchbase
 components.
 * [couchbase/platform](http://github.com/couchbase/platform): Utility library
 for frequently used abstractions which differ between different operating
 systems (e.g. threading, temp files, clocks, random)

# Memcached (Front-end)
Memcached is responsible for handling connections and buckets. It receives and
decodes inbound requests and interacts with an underlying engine to service
those requests.

## Startup and Initialization
On startup Memcached has a number of tasks:

* Establishing the worker thread pool
* Initialising the buckets/engines
* Initialising logging
* Connecting to the sockets
* Starting the dispatch event loop

## Connection management
The `Connection` class represents a Socket (it is used by both clients and
server objects).

The Connection object is bound to a thread object, and never changes (Except
for clients running TAP, which are moved over to the TAP thread after the
initial packet).

If the connection is idle for a configurable (through
`connection_idle_time`) amount of time (5 minutes by default) it is
automatically disconnected unless:

* The connection authenticated as `_admin`
* The connection is used for TAP or DCP

### Threads

Memcached uses a number of threads engineered to service a large number of
concurrent requests efficiently. More in depth rationale of Memcached's
threading model can be found in the [C10K Document](in-depth/C10k.md).

#### Main (dispatch) thread

The main thread, is responsible for listening to all of the server's sockets.
When a new inbound connection is received it delegates the connection using a
round-robin model to one of the worker threads.

#### Worker threads

The worker threads are responsible for serving the clients and most of the time
run entirely independently of each other. Each of them runs their own libevent
loop for servicing their connections.

The number of worker threads is specified with the `threads` parameter in the
configuration, by default this is approximately 0.75 worker threads for the
total number of cores on the system.

#### Other threads

* The TAP thread is responsible for serving all TAP communication.
* The logging thread is responsible for writing log entries in the log buffer to
file and for managing the log rotation. A separate thread so that the worker
threads do not get blocked handling IO. This is an optional thread created when
loading the file logger extension.
* The stdin thread performs a blocking listen on stdin and will shutdown when
sent a specific command via stdin. It is run in a separate thread in case
another thread is busy in order to keep shutdown fast. It is required for
integration with ns_server. It is currently planned to be changed due to
failures with this communication (Including deadlocks).
* Some commands (bucket creation/deletion, reload of the SASL password database)
are run on a 'new' separate executor pool whilst others are implemented by 
dispatching separate threads to execute the user's command. There are currently 
no checks trying to protect ourselves from clients trying to allocate too many 
threads (but the commands themselves are not available to the regular bucket 
users).

#### Worker thread locking

A client is bound to its worker thread when the client is created. When the
clients' event loop is triggered, it acquires the threads lock and runs the
entire state machine. This means that the thread can safely access any
resource available in the LIBEVENT thread object. Given that it already holds
a lock to a libevent thread object it is *NOT* allowed to try to acquire *ANY*
other thread locks. That would lead to a deadlock.

Given that all of the clients share the same set of worker threads, the
clients should not block while waiting for a resource to become available. In
order to facilitate this we expect the underlying engine to return `EWOULDBLOCK`
and run the blocking task in a *different* thread and call `notify_io_complete`
when the resource is available.

The reason it has to be a *different* thread is firstly `notify_io_complete`
will try to lock the thread object where the connection is bound to (which would
cause a deadlock if called while holding a thread lock) and so that other
connections on the same thread do not have to wait.

### Connection Lifecycle

When a new TCP connection is made it will be connected to the dispatch thread
which is listening for new connections on all the server ports. The dispatch
thread will assign it to a worker thread. Note: Greenstack Protocol (mentioned
below) will use libevent's bufferevent framework

Each worker thread is running a [libevent](http://libevent.org/) loop. libevent
is an abstraction which allows for scheduling tasks to be done in response to
certain events. These events include:

- File / Socket is ready to be read from
- File / Socket is ready to be written to
- Signal occurred
- Timer expired

#### Packet Structure

##### Binary Protocol
Couchbase smart clients connect using a protocol derived from the
Memcached binary protocol. While the encoding is the same as the Memcached
binary protocol there are additional opcodes for Couchbase specific features
such as DCP and Subdoc. More in-depth explanation of the binary protocol used by
Couchbase can be found in the [Binary Protocol document](./BinaryProtocol.md).

##### Greenstack Protocol
[Greenstack](../protocol/Greenstack) is a 'next-generation' protocol for all
components of Couchbase Server using flatbuffers allowing for more flexible
commands and out of order operations. Greenstack is still in development so a
lot of this connection lifecycle overview will not apply.

#### Reading the request
The first step to reading a packet from a socket is to read the header. The
general process for reading from a socket is to attempt to read everything
needed non-blocking. If everything needed was read then Memcached will continue
with the packet and process it. If there was not enough read from the buffer
then the thread will switch to servicing another connection until libevent
notifies that there is more to be read. Memcached will repeat this process until
it has read everything it requires to continue. A similar process is also used
when writing a response packet to the socket.

The header contains information about the length of the packet from which we can
read the rest of the packet as described above.

#### Execution
Once the packet has been read the appropriate executor is run. There are two
classes of executors request executors and response executors.

Response executors are used primarily by TAP and DCP, both of which are a duplex
protocol where either end may send a packet at any time. By comparison the
'normal' client operations are entirely client driven where a client sends a
packet and the server responds. It does not support out of order responses so
the connection is blocked until the server responds.

For this example we will look at a GET command. In the case of a GET memcached
almost immediately delegates responsibility to the engine. It passes the key
requested and an item reference for the engine to fill in.

When the engine returns to memcached one of several things could have happened:

* Success: Memcached uses the item reference to prepare a response for the
connection and writes it to the socket
* Key Doesn't Exist: Memcached sends an error response back to the client
* `EWOULDBLOCK`: A common response code telling Memcached that the operation
'would block' if not handled specially.

The `EWOULDBLOCK` behaviour is extremely important for Couchbase as it occurs
frequently for DGM scenarios (Disk greater than memory) where an item might not
be in memory. The latency for getting an item from the hashtable compared to
getting an item from disk is much lower. This means that that the worker thread
could serve orders of magnitude more resident requests in the time it takes to
return from disk.

For this reason, when Memcached receives an `EWOULDBLOCK` response from the
engine it pauses working on the current connection and switches to servicing
another. By returning an `EWOULDBLOCK` the engine effectively promises to notify
Memcached when it is in a state where the operation will not block (e.g. the
requested key is now in memory). This is done using the `notify_io_complete`
call, at which point Memcached will effectively 'retry' the operation.

## Multi-tenancy (buckets)
The original Memcached has no concept of buckets. There is in effect a single
store which everything goes into. Couchbase Server adds buckets which allow for
grouping sets of data.

Memcached implements this by having multiple instances of engines (discussed
below), one for each bucket. Memcached maintains a map of bucket names to
engine handles and forwards requests to a bucket onto the corresponding engine
instance.

# Engines (Back-end)

## ep-engine (Couchbase Bucket)
[ep-engine](http://github.com/couchbase/ep-engine) (Eventually Persistent
engine) is the usual bucket engine used by Couchbase clusters.

## default engine (Memcached Bucket)
Default engine is roughly in spirit with the original operation of memcached.
It is not in active development for Couchbase Server and does not support most
of the additional KV engine features (replication, xdcr, rebalance, persistence,
views, backups). Default engine is broadly speaking just a single hash-table per
KV node and has no cluster awareness.

## Experimental and Test Engines

### crash
The crash engine is designed to simply crash on start-up. It is used for testing
things like [breakpad integration]
(https://chromium.googlesource.com/breakpad/breakpad/).

### ewouldblock
[ewouldblock engine](../engines/ewouldblock_engine/ewouldblock_engine.cc) wraps
another engine and proxies requests to it. The primary difference in operation
is that the ewouldblock engine will, as the name suggests, return an
`EWOULDBLOCK` response on any call that can potentially block (e.g. a GET) —
even if it doesn't actually block. The purpose of this is for consistently
testing the behaviour of memcached in response to blocking operations as it is
otherwise awkward to force the engine into a state where it will block for a
specific operation.
